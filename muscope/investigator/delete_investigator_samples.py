"""
Delete all samples associated with the specified investigator.
"""
import os
import sys

from orminator import session_manager_from_db_uri
import muscope.models as models


investigator_last_name = sys.argv[1]
print('removing all samples associated with investigator "{}"'.format(investigator_last_name))

with session_manager_from_db_uri(os.environ['MUSCOPE_DB_URI']) as db_session:
    investigator_query = db_session.query(
        models.Investigator).filter(
            models.Investigator.last_name == investigator_last_name)

    investigator = investigator_query.one()
    investigator_sample_count = len(investigator.sample_list)
    print('found {} samples associated with investigator "{}"'.format(
        investigator_sample_count,
        investigator_last_name))
    for s in investigator.sample_list:
        print('deleting {}'.format(s.sample_name))
        db_session.delete(s)

    print('deleted {} samples associated with investigator "{}"'.format(
        investigator_sample_count,
        investigator_last_name))
