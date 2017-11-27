import os.path

import pandas as pd

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, Float, String

from irods.keywords import FORCE_FLAG_KW
from irods.session import iRODSSession

import muscope_loader
import muscope_loader.util as util

from orminator import session_manager


Base = declarative_base()


class Station(Base):
    __tablename__ = 'station'

    id = Column(Integer, primary_key=True, autoincrement=True)

    cruise_name = Column(String)
    station_number = Column(Integer)
    latitude = Column(Float)
    longitude = Column(Float)

    sa.UniqueConstraint('cruise_name', 'station_number')


def get_engine():
    return sa.create_engine('sqlite:///stations.sqlite3', echo=False)


def create_db():
    engine = get_engine()
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


def build():
    if os.path.exists('stations.sqlite3'):
        os.remove('stations.sqlite3')
    create_db()
    session_class = sessionmaker(bind=get_engine())
    with session_manager(session_class) as db_session:
        with iRODSSession(irods_env_file=os.path.expanduser('~/.irods/irods_environment.json')) as irods_session:
            scope_data_core_collection = irods_session.collections.get('/iplant/home/scope/data/core')
            print('loading station and cast data')
            for data_object in scope_data_core_collection.data_objects:
                print('\t{}'.format(data_object.path))
                # get the file
                local_file_fp = os.path.join(
                    os.path.dirname(muscope_loader.__file__),
                    'downloads',
                    data_object.name)

                irods_session.data_objects.get(
                    data_object.path,
                    local_file_fp,
                    **{FORCE_FLAG_KW: True})

                watercolumn_df = pd.read_excel(
                    local_file_fp,
                    skiprows=(0,2))

                #print(watercolumn_df.iloc[:5, :5])
                #print(watercolumn_df.iloc[:, 0].unique())

                for r, row in watercolumn_df.iterrows():
                    station_query_result = db_session.query(Station).filter(
                        Station.cruise_name == row[0],
                        Station.station_number == row.station).one_or_none()
                    if station_query_result is None:
                        #print(row[0], row.station, row.rosette_position)
                        s = Station(
                            cruise_name=row[0],
                            station_number=int(row.station),
                            latitude=row.latitude,
                            longitude=(-1.0 * row.longitude))
                        db_session.add(s)

    with session_manager(session_class) as db_session:
        print('inserted {} stations'.format(db_session.query(Station).count()))
        #for station in db_session.query(Station).all():
        #    print('cruise "{0.cruise_name}" station {0.station_number} lat/long {0.latitude}/{0.longitude}'.format(station))
