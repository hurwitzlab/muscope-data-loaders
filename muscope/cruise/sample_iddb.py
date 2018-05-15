import os
import urllib.parse

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Boolean, Integer, String

Base = declarative_base()


class SampleNameToSampleFileIdentifier(Base):
    __tablename__ = 'sample_name_to_sample_file_identifier'
    id = Column(Integer, primary_key=True, autoincrement=True)
    sample_name = Column(String)
    sample_file_identifier = Column(String, unique=True)

    sa.UniqueConstraint('sample_name', 'sample_file_identifier')


# version 2
#
# map file name -> sample name
# for example:
#   SM001_072615_10pm_rep2_selected_R1.fastq -> SM001_072615_10pm_rep2_mRNA
#   SM001_072615_10pm_rep2_selected_R2.fastq -> SM001_072615_10pm_rep2_mRNA
#
class SampleFileNameToSampleName(Base):
    __tablename__ = 'sample_file_name_to_sample_name'
    id = Column(Integer, primary_key=True, autoincrement=True)
    sample_file_name = Column(String, unique=True)
    sample_name = Column(String)

    data_type = Column(String)
    processed = Column(Boolean)

    sa.UniqueConstraint('sample_name', 'sample_file_identifier')


def get_engine(db_uri):
    return sa.create_engine(db_uri, echo=False)


def create_tables(db_uri):
    engine = get_engine(db_uri)
    Base.metadata.create_all(engine)


def build(db_uri):
    o = urllib.parse.urlparse(db_uri)
    # remove the leading /
    db_file_path = o.path[1:]
    print('sample id db: "{}"'.format(db_file_path))
    if os.path.exists(db_file_path):
        os.remove(db_file_path)
    create_tables(db_uri)


def insert_sample_info(sample_name, sample_file_identifier, session):
    try:
        find_sample_name_for_sample_file_identifier(
            sample_file_identifier=sample_file_identifier,
            session=session)
    except sa.orm.exc.NoResultFound:
        session.add(
            SampleNameToSampleFileIdentifier(
                sample_name=sample_name,
                sample_file_identifier=sample_file_identifier))


def find_sample_name_for_sample_file_identifier(sample_file_identifier, session):
    return session.query(
        SampleNameToSampleFileIdentifier).filter(
            SampleNameToSampleFileIdentifier.sample_file_identifier == sample_file_identifier).one().sample_name


# version 2
def insert_sample_file_name_and_sample_name(sample_file_name, data_type, sample_name, session):
    try:
        session.query(SampleFileNameToSampleName).filter(
            SampleFileNameToSampleName.sample_file_name == sample_file_name).one()
    except sa.orm.exc.NoResultFound:
        session.add(
            SampleFileNameToSampleName(
                sample_file_name=sample_file_name,
                sample_name=sample_name,
                data_type=data_type,
                processed=False))


def mark_sample_file_processed(sample_file_name, session):
    print('mark "{}" processed'.format(sample_file_name))
    s = session.query(SampleFileNameToSampleName).filter(
        SampleFileNameToSampleName.sample_file_name == sample_file_name).one()

    s.processed = True


def find_sample_name_for_sample_file_name(sample_file_name, session):
    return session.query(
        SampleFileNameToSampleName).filter(
            SampleFileNameToSampleName.sample_file_name == sample_file_name).one_or_none()


def get_unprocessed_sample_files(session):
    return session.query(SampleFileNameToSampleName).filter(
        SampleFileNameToSampleName.processed == False).all()
