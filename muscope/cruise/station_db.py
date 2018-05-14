import os.path
import urllib.parse

import pandas as pd

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String

from irods.keywords import FORCE_FLAG_KW
from irods.session import iRODSSession

import muscope

from orminator import session_manager_from_db_uri


Base = declarative_base()


class Station(Base):
    __tablename__ = 'station'

    id = Column(Integer, primary_key=True, autoincrement=True)

    cruise_name = Column(String)
    station_number = Column(Integer)
    latitude = Column(Float)
    longitude = Column(Float)

    sa.UniqueConstraint('cruise_name', 'station_number')


def get_engine(db_uri):
    return sa.create_engine(db_uri, echo=False)


def create_db(db_uri):
    engine = get_engine(db_uri)
    Base.metadata.create_all(engine)


def insert_station(cruise_name, station_number, latitude, longitude, session):
    x = Station(
        cruise_name=cruise_name,
        station_number=station_number,
        latitude=latitude,
        longitude=longitude)
    session.add(x)


def find_station(cruise_name, station_number, session):
    return session.query(Station).filter(
        Station.cruise_name == cruise_name,
        Station.station_number == station_number).one()


def build(db_uri):
    o = urllib.parse.urlparse(db_uri)
    # remove the leading /
    db_file_path = o.path[1:]
    print('station db: "{}"'.format(db_file_path))
    if os.path.exists(db_file_path):
        os.remove(db_file_path)

    create_db(db_uri)

    with session_manager_from_db_uri(db_uri) as db_session:
        # insert station 0
        insert_station(cruise_name='No name', station_number=0, latitude=0, longitude=0, session=db_session)

        # temporarily insert some HOT cruise stations for Church HOT 201-222 (???)
        insert_station(cruise_name='HOT233', station_number=2, latitude=22.45, longitude=-158.0, session=db_session)
        insert_station(cruise_name='HOT234', station_number=2, latitude=22.45, longitude=-158.0, session=db_session)
        insert_station(cruise_name='HOT267', station_number=2, latitude=22.45, longitude=-158.0, session=db_session)

        with iRODSSession(irods_env_file=os.path.expanduser('~/.irods/irods_environment.json')) as irods_session:
            scope_data_core_collection = irods_session.collections.get('/iplant/home/scope/data/core')
            print('loading station and cast data into "{}"'.format(db_uri))
            for data_object in scope_data_core_collection.data_objects:
                print('\t{}'.format(data_object.path))

                downloads_dp = os.path.join(
                    os.path.dirname(muscope.__file__),
                    'downloads')

                if not os.path.exists(downloads_dp):
                    os.mkdir(downloads_dp)

                # get the file
                local_file_fp = os.path.join(
                    downloads_dp,
                    data_object.name)

                irods_session.data_objects.get(
                    data_object.path,
                    local_file_fp,
                    **{FORCE_FLAG_KW: True})

                # MS_watercolumn.xlsx is a little different from the others
                if os.path.basename(local_file_fp).startswith('MS_'):
                    ##print('why it is "{}"'.format(local_file_fp))
                    skiprows = (1, )
                else:
                    skiprows = (0, 2)

                watercolumn_df = pd.read_excel(
                    local_file_fp,
                    skiprows=skiprows)

                for r, row in watercolumn_df.iterrows():

                    # the MESO-SCOPE cruise is called 'MS' in the spreadsheets
                    # but we want to call it 'MESO-SCOPE' in the database
                    cruise_name = row[0]
                    if cruise_name == 'MS':
                        cruise_name = 'MESO-SCOPE'

                    station_query_result = db_session.query(Station).filter(
                        Station.cruise_name == cruise_name,
                        Station.station_number == row.station).one_or_none()
                    if station_query_result is None:
                        s = Station(
                            cruise_name=cruise_name,
                            station_number=int(row.station),
                            latitude=row.latitude,
                            longitude=(-1.0 * row.longitude))
                        db_session.add(s)

    with session_manager_from_db_uri(db_uri) as db_session:
        print('inserted {} stations'.format(db_session.query(Station).count()))
        #for station in db_session.query(Station).all():
        #    print('cruise "{0.cruise_name}" station {0.station_number} lat/long {0.latitude}/{0.longitude}'.format(station))
