import os

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()

class SampleNameToSampleFileIdentifier(Base):
    __tablename__ = 'sample_name_to_sample_file_identifier'
    id = Column(Integer, primary_key=True, autoincrement=True)
    sample_name = Column(String)
    sample_file_identifier = Column(String, unique=True)

    sa.UniqueConstraint('sample_name', 'sample_file_identifier')


def get_engine():
    return sa.create_engine('sqlite:///sample_iddb.sqlite3', echo=False)


def create_tables():
    engine = get_engine()
    Base.metadata.create_all(engine)


def build():
    if os.path.exists('sample_iddb.sqlite3'):
        os.remove('sample_iddb.sqlite3')
    create_tables()


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
