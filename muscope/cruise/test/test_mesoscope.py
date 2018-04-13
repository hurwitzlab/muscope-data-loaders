import os

from muscope.models import Cruise, Sample
from orminator import session_manager_from_db_uri


def test_four_data_files_per_sample():
    """
    This is a quick check after importing MESO-SCOPE data for the first time.
    We expect 4 data files for each sample.
    """
    with session_manager_from_db_uri(os.environ['MUSCOPE_DB_URI']) as session:
        meso_scope_cruise_id = session.query(Cruise.cruise_id).filter(Cruise.cruise_name == 'MESO-SCOPE')
        for meso_scope_sample in session.query(Sample).filter(Sample.cruise_id == meso_scope_cruise_id).all():
            assert len(meso_scope_sample.sample_file_list) == 4
