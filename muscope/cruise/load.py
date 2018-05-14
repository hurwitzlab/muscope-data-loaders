"""
Given an IRODS collection that contains one or more attribute spreadsheets and subcollections with the associated
data files, insert the data from the attribute spreadsheets and the data file paths into the muSCOPE database.

For example, given collection

  /iplant/home/scope/data/dyhrman

find the attribute spreadsheets

  /iplant/home/scope/data/dyhrman/HL2A/Dyhrman_HL2A_RNAdiel_seq_assoc_data_v4.xls
  /iplant/home/scope/data/dyhrman/HL2A/Dyhrman_HL2A_Tricho_seq_attrib_v2.xls
  /iplant/home/scope/data/dyhrman/Dyhrman_HL4_incubation_seq_assoc_data_v4.xls
  /iplant/home/scope/data/dyhrman/MS/Dyhrman_MS_incubation_assoc_data_v4.xls

and find the data file directories

  /iplant/home/scope/data/dyhrman/HL2A/RNAdiel
  /iplant/home/scope/data/dyhrman/HL2A/TrichoRNAdiel_reads
  /iplant/home/scope/data/dyhrman/HL2A/incubation
  /iplant/home/scope/data/dyhrman/HL4/incubation
  /iplant/home/scope/data/dyhrman/HL4/insitu
  /iplant/home/scope/data/dyhrman/MS/incubation
  /iplant/home/scope/data/dyhrman/MS/insitu

under the assumption that data files will be found in a subcollection under the collection that includes the
corresponding attribute spreadsheet.

First all recognized attribute spreadsheets are downloaded and parsed, then samples and attributes are inserted into
the muSCOPE database.

Second the sample file paths are inserted into the muSCOPE database.

usage:
  python load.py --collections /iplant/home/scope/data/dyhrman --db-uri $MUSCOPE_DB_URI --file-limit 100
  python load.py --collections /iplant/home/scope/data/dyhrman,/iplant/home/scope/data/delong --db-uri $MUSCOPE_DB_URI --load-data

current usage:

Parse but do not load data for 10 files from recognized attribute spreadsheet(s) in the
/iplant/home/scope/data/dyhrman collection (this is a test):

(msdl) vagrant@vagrant:/muscope-data-loaders$ python muscope/cruise/load.py \
       --collections /iplant/home/scope/data/dyhrman \
       --db-uri $MUSCOPE_DB_URI \
       --file-limit 10

Load all data from attribute spreadsheet /iplant/home/scope/data/caron/HL2A/Caron_HL2A_VertProf_seq_attrib_v3.xls:

(flum) vagrant@vagrant:/muscope-data-loaders$ time python muscope/cruise/load.py \
       --collections /iplant/home/scope/data/caron/HL2A \
       --attribute-file-pattern Caron_HL2A_VertProf_seq_attrib_v3\\.xls \
       --db-uri $MUSCOPE_DB_URI \
       --load-data

(msdl) [jklynch@myo muscope-data-loaders]$ time python muscope/cruise/load.py \
       --collections /iplant/home/scope/data/caron/HL2A \
       --attribute-file-pattern Caron_HL2A_VertProf_seq_attrib_v3\\.xls \
       --db-uri $MUSCOPE_DB_URI \
       --load-data > load_Caron_HL2A_VertProf_seq_attrib_v3.log

Load all data from all recognized attribute spreadsheets in /iplant/home/scope/data/caron/HL3:

(msdl) [jklynch@myo muscope-data-loaders]$ time python muscope/cruise/load.py \
       --collections /iplant/home/scope/data/caron/HL3 \
       --db-uri $MUSCOPE_DB_URI \
       --load-data > load_Caron_HL3_VertProf_seq_attrib_v3.log

(msdl) [jklynch@myo muscope-data-loaders]$ time python muscope/cruise/load.py \
       --collections /iplant/home/scope/data/church/HOT/201-222 \
       --db-uri $MUSCOPE_DB_URI \
       --file-limit 10


       --load-data > load_Church_HOT_seq_attrib_v3.log


"""
import argparse
import datetime
import os
import re
import sys

from irods.keywords import FORCE_FLAG_KW
from irods.session import iRODSSession

import pandas as pd
import sqlalchemy as sa

import muscope
import muscope.models as models
import muscope.util as util

import muscope.cruise.sample_iddb as sample_iddb
import muscope.cruise.station_db as station_db

from orminator import session_manager_from_db_uri


def get_args(argv):
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument('--db-uri', required=True,
                            help='muSCOPE database URI e.g. mysql+pymysql://imicrobe:<password>@localhost/muscope2')
    arg_parser.add_argument('--collections', required=True,
                            help='comma-separated IRODS collections')
    arg_parser.add_argument('--attribute-file-pattern', required=False, default='\\.xlsx?',
                            help='optionally specify a regular expression to match attribute file names')
    arg_parser.add_argument('--load-data', required=False, action='store_true', default=False,
                            help='load database')
    arg_parser.add_argument('--file-limit', required=False, type=int, default=None,
                            help='maximum number of files to process')

    args = arg_parser.parse_args(argv)
    print('command line args: {}'.format(args))

    return args


def main(argv):
    args = get_args(argv)

    # these are temporary SQLite databases
    sample_id_db_uri = 'sqlite:///sample_iddb.sqlite3'
    station_db_uri = 'sqlite:///stations.sqlite3'

    sample_iddb.build(sample_id_db_uri)
    station_db.build(station_db_uri)

    muscope_collection_paths = args.collections.split(',')
    print('collection paths:\n\t{}'.format('\n\t'.join(muscope_collection_paths)))
    # muscope_collection_paths = (
    #     #'/iplant/home/scope/data/armbrust/HL2A',
    #     # check '/iplant/home/scope/data/caron/HL2A',
    #     #'/iplant/home/scope/data/caron/HL3',
    #     # check '/iplant/home/scope/data/caron/HOT/268_271_275_279',
    #     # check '/iplant/home/scope/data/caron/HOT/273',
    #     #'/iplant/home/scope/data/chisholm/HOT',
    #     #'/iplant/home/scope/data/delong/HL2A',
    #     '/iplant/home/scope/data/dyhrman/HL4',
    #     #'/iplant/home/scope/data/dyhrman/MS',
    # )

    attribute_file_pattern = args.attribute_file_pattern

    for c in muscope_collection_paths:
        process_muscope_collection(
            muscope_collection_path=c,
            attribute_file_pattern=attribute_file_pattern,
            db_uri=args.db_uri,
            sample_id_db_uri=sample_id_db_uri,
            station_db_uri=station_db_uri,
            load_data=args.load_data,
            file_limit=args.file_limit)

    return 0


def process_muscope_collection(muscope_collection_path, attribute_file_pattern, db_uri, sample_id_db_uri, station_db_uri, load_data, file_limit):
    """
    List the contents of the argument (a collection) and recursively list the contents of subcollections.
    When a data object is found look for a function that can parse it based on its name.

    :param muscope_collection_path: (str) start the search for attribute spreadsheets here
    :param attribute_file_pattern:  (str) process attribute spreadsheets matching this pattern
    :param db_uri:                  (str) SQLAlchemy database URI for muSCOPE database
                                          e.g. mysql+pymysql://imicrobe:<password>@localhost/muscope2
    :param sample_id_db_uri:        (str) temporary SQLite database URI
    :param station_db_uri:          (str) temporary SQLite database URI
    :param load_data:               (bool) insert database rows if True
    :param file_limit:              (int or None) stop after file_limit files have been processed
    :return:
    """

    ##data_file_endings = re.compile(r'\.(fastq|fasta|fna|gff|faa)(\.(gz|bz2))?$')
    attribute_file_re = re.compile(attribute_file_pattern)

    processed_file_paths = []
    loaded_file_paths = []
    unrecognized_file_paths = []

    #
    # parse attribute spreadsheets
    # insert attributes
    #
    unprocessed_collection_paths = [muscope_collection_path]
    while len(unprocessed_collection_paths) > 0:
        c = unprocessed_collection_paths.pop(0)
        print('processing collection "{}"\n'.format(c))

        with iRODSSession(irods_env_file=os.path.expanduser('~/.irods/irods_environment.json')) as irods_session, \
                session_manager_from_db_uri(sample_id_db_uri) as sample_db_session:

            muscope_collection = irods_session.collections.get(c)

            for muscope_data_object in muscope_collection.data_objects:
                #print('found data object {} in {}'.format(muscope_data_object.name, muscope_collection.path))

                attribute_file_match = attribute_file_re.search(muscope_data_object.name)

                if attribute_file_match is None:
                    print('{} does not match attribute file pattern "{}"'.format(
                        muscope_data_object.name,
                        attribute_file_pattern))
                else:
                    print('found an attribute file {}'.format(muscope_data_object.path))
                    conjectured_parse_function_name = \
                        'parse_' + \
                        muscope_data_object.name.replace('.', '__').replace('-', '_')
                    print('looking for parse function with name "{}"'.format(conjectured_parse_function_name))
                    if conjectured_parse_function_name in sys.modules[__name__].__dict__:
                        parse_function = sys.modules[__name__].__dict__[conjectured_parse_function_name]

                        local_attribute_file_fp = os.path.join(
                            os.path.dirname(muscope.__file__),
                            'downloads',
                            muscope_data_object.name)

                        print('copying attribute file "{}" to "{}"'.format(
                            muscope_data_object.path,
                            local_attribute_file_fp))

                        irods_session.data_objects.get(
                            muscope_data_object.path,
                            local_attribute_file_fp,
                            **{FORCE_FLAG_KW: True})

                        #
                        # parse an attribute spreadsheet into a pandas.DataFrame
                        #
                        attr_df = parse_function(local_attribute_file_fp)
                        print('attributes:\n{}'.format(attr_df.head()))

                        load_attributes(
                            attr_df,
                            db_uri=db_uri,
                            sample_id_db_uri=sample_id_db_uri,
                            station_db_uri=station_db_uri,
                            load_data=load_data)

                    else:
                        print('    no parse function by that name')
                        unrecognized_file_paths.append(muscope_data_object.path)

            # add sub collections to the list of collections to continue the
            # recursive search for attribute spreadsheets
            for subcollection in muscope_collection.subcollections:
                print('adding subcollection path "{}"'.format(subcollection.path))
                unprocessed_collection_paths.append(subcollection.path)

    print('done with attribute files')

    #
    # handle sample data files
    #
    unprocessed_collection_paths = [muscope_collection_path]
    while len(unprocessed_collection_paths) > 0:
        c = unprocessed_collection_paths.pop(0)
        print('processing collection "{}"\n'.format(c))

        with iRODSSession(irods_env_file=os.path.expanduser('~/.irods/irods_environment.json')) as irods_session, \
                session_manager_from_db_uri(sample_id_db_uri) as sample_db_session:
            muscope_collection = irods_session.collections.get(c)

            for muscope_data_object in muscope_collection.data_objects:

                ##data_file_match = data_file_endings.search(muscope_data_object.name)
                sample_for_data_file = sample_iddb.find_sample_name_for_sample_file_name(
                    muscope_data_object.name,
                    session=sample_db_session)

                if sample_for_data_file is None:
                    print('nothing to do with file "{}"'.format(muscope_data_object.path))
                else:
                    try:
                        if (file_limit is not None) and len(processed_file_paths) >= file_limit:
                            print('reached file limit {}'.format(file_limit))
                            unprocessed_collection_paths = []
                            break
                        else:
                            processed_file_paths.append(muscope_data_object.path)

                            load_data_file(
                                muscope_data_object,
                                db_uri=db_uri,
                                sample_db_uri=sample_id_db_uri,
                                load_data=load_data)
                            loaded_file_paths.append(muscope_data_object.path)

                            # I need some space
                            print()

                    except util.FileNameException as fne:
                        print(fne)
                        unrecognized_file_paths.append(muscope_data_object.path)

            # add sub collections to the list of collections to continue the
            # recursive search for sample data files
            for subcollection in muscope_collection.subcollections:
                print('adding subcollection path "{}"'.format(subcollection.path))
                unprocessed_collection_paths.append(subcollection.path)

    print('loaded {} file path(s):\n\t{}'.format(
        len(loaded_file_paths),
        '\n\t'.join(loaded_file_paths)))
    print('failed to recognize {} file path(s):\n\t{}'.format(
        len(unrecognized_file_paths),
        '\n\t'.join(unrecognized_file_paths)))


"""
sample_name_pattern_table = {
#
# KM1513.S06C1_A_600.6tr.orfs40.fasta.gz
#
    re.compile(
        r'KM\d+\.'
        r'(?P<sample_name>S\d+C\d+_[A-Z])'
        r'_\d+\.[a-zA-Z0-9]+\.orfs\d+\.fasta.gz$')
    : 'Armbrust HL2A EukTxnDiel sample',

    #
# KM1513.S06C1_A_600.H5C5H_1.1.fastq.gz
#
    re.compile(
        r'KM\d+\.'
        r'(?P<sample_name>S\d+C\d+_[A-Z])'
        r'_\d+\.[A-Z0-9]+_\d+\.\d+\.fastq.gz$')
    : 'Armbrust HL2A EukTxnDiel sample',

#
# Diel-RNA-1_S1_L001_R1_001.fastq.gz
#
    re.compile(
        r'(?P<sample_name>Diel-[DR]NA-\d+_S\d+)'
        r'_L00\d_R[12]_001\.fastq\.gz$')
    : 'HL2A Caron diel sample',

#
# July_1000m_TAGCTT_L001_R1_001.fastq.gz
# July_5m_Rep1_ATCACG_L001_R1_001.fastq.gz
# July_DCM_Rep1_TTAGGC_L001_R1_001.fastq.gz
#
    re.compile(
        r'(?P<sample_name>(July|March)_(\d+m|DCM))'
        r'(_Rep\d+)?'
        r'_[ACGT]+'
        r'_L00\d_R[12]_001\.fastq\.gz$')
    : 'HL2A Caron vert profile sample',

#
# 10a-268-400-DNA_S10_L001_R1_001.fastq.gz
#
    re.compile(
        r'(?P<sample_name>\d+[abc]-\d+-\d+-(DeDNA|DNA|RNA))'
        r'_S\d+'
        r'_L00\d_R[12]_001\.fastq\.gz$')
    : 'HOT Caron quarterly sample',

#
# 1_200um_S1_L001_R1_001.fastq.gz
#
    re.compile(
        r'(?P<sample_name>\d+_\d+um_S\d+)'
        r'_L00\d_R[12]_001\.fastq\.gz$')
    : 'Caron HOT273 18S size fraction',

#
# S0501_1_sequence.fastq.bz2
#
    re.compile(
        r'(?P<sample_name>S\d+)'
        r'_\d+_sequence\.fastq\.bz2$')
    : 'Chisholm HOT BATS',

#
# 161013Chi_D16-10856_1_sequence.fastq.gz
#
    re.compile(
        r'(?P<sample_name>\d+Chi_D\d+-\d+)'
        r'_\d+_sequence\.fastq\.gz$')
    : 'Chisholm Vesicle',

#
# This is a second re intended to work on path or file name.
#
# /iplant/home/scope/data/delong/HL2A/HLIID00-20/CSHLIID00-20a-S06C001-0015:
#   CSHLIID00-20a-S06C001-0015_S1_R1_001.fastq
#   CSHLIID00-20a-S06C001-0015_S1_R2_001.fastq
#   S06C001.metagenome.readpool.fastq.gz
#   contigs.fastq
#   genes.fna
#   prodigal.gff
#   proteins.faa
#   ribosomal_rRNA.fna
#   ribosomal_rRNA.gff
#
    re.compile(
        r'(?P<sample_name>(CSHLII[DR]\d\d)-(\d+[a-z]+)-(S\d+C\d+)-(\d+))')
    : 'DeLong HL2A',

#
#  SM125_S42_L008_R1_001.fastq.gz
#
    re.compile(
        r'(?P<sample_name>SM\d+_S\d+)'
        r'_L\d+_R[12]_\d+\.fastq(\.gz)?$')
    : 'Dyhrman HL4 or MESO-SCOPE',

}


def parse_data_file_path_or_name(path_or_name):
    print('parsing data file path or name "{}"'.format(path_or_name))

    sample_path_match_set = {
        sample_name_pattern.search(path_or_name)
        for sample_name_pattern, sample_name_pattern_label
        in sample_name_pattern_table.items()}

    if None in sample_path_match_set:
        sample_path_match_set.remove(None)

    print('matched sample path patterns:\n\t{}'.format(
        '\n\t'.join([
            '"{}" sample "{}"'.format(sample_name_pattern_table[match.re], match.group('sample_name'))
            for match
            in sample_path_match_set])))

    if len(sample_path_match_set) == 0:
        raise util.FileNameException('failed to parse file path "{}"'.format(path_or_name))
    elif len(sample_path_match_set) == 1:
        return sample_path_match_set.pop()
    else:
        raise util.FileNameException('too many matches:\n\t{}'.format('\n\t'.join([
            sample_name_pattern_table[match.re] for match in sample_path_match_set])))
"""


def load_attributes(core_attr_df, db_uri, sample_id_db_uri, station_db_uri, load_data):
    with session_manager_from_db_uri(db_uri) as session, \
            session_manager_from_db_uri(sample_id_db_uri) as sample_iddb_session, \
            session_manager_from_db_uri(station_db_uri) as station_session:

        sample_attributes_present = dict()
        for column_header in core_attr_df.columns:
            ##print(column_header)
            sample_attr_type = session.query(models.Sample_attr_type).filter(
                models.Sample_attr_type.type_ == column_header).one_or_none()
            if sample_attr_type is None:
                print('** no sample_attr_type for column header "{}"'.format(column_header))
            else:
                print('@@ found sample_attr_type "{}" for column header "{}"'.format(sample_attr_type.type_,
                                                                                     column_header))
                sample_attributes_present[column_header] = sample_attr_type

        print('found the following sample attribute types:\n\t{}'.format(
            '\n\t'.join([v.type_ for k, v in sorted(sample_attributes_present.items())])))

        # many but NOT ALL attribute spreadsheets have an even number of rows for each sample
        for r1, sample_file_r1_row in core_attr_df.iterrows():
            print('sample with sample_name "{}"'.format(sample_file_r1_row.sample_name))

            if str(sample_file_r1_row.sample_name) == 'nan':
                # do nothing with rows without sample name
                pass
            else:
                # this row has a sample name

                investigator = session.query(models.Investigator).filter(
                    models.Investigator.last_name == sample_file_r1_row.pi).one()

                cruise_name = sample_file_r1_row.cruise_name
                cruise_query_result = session.query(models.Cruise).filter(
                    models.Cruise.cruise_name == cruise_name).one_or_none()
                if cruise_query_result is None:
                    print('cruise "{}" is not in the database'.format(cruise_name))
                    # start date and end date will have to be entered manually
                    cruise = models.Cruise(cruise_name=cruise_name)
                    if load_data:
                        session.add(cruise)
                    else:
                        print('  cruise will not be loaded')
                else:
                    cruise = cruise_query_result

                station_number = int(sample_file_r1_row.station)
                cast_number = int(sample_file_r1_row.cast_num)
                print('  on row {} station_number is "{}" and cast number is "{}"'.format(r1, station_number, cast_number))

                station = station_db.find_station(
                    cruise_name=cruise.cruise_name,
                    station_number=station_number,
                    session=station_session)

                if station_number == 0:
                    # this is a net tow
                    # take latitude and longitude from the spreadsheet
                    sample_latitude = sample_file_r1_row.latitude
                    sample_longitude = sample_file_r1_row.longitude
                else:
                    sample_latitude = station.latitude
                    sample_longitude = station.longitude

                print('associating file "{}" with sample name "{}"'.format(
                    sample_file_r1_row.seq_name,
                    sample_file_r1_row.sample_name))

                if 'data_type' in core_attr_df.columns:
                    data_type = sample_file_r1_row.data_type
                else:
                    data_type = 'Reads'

                sample_iddb.insert_sample_file_name_and_sample_name(
                    sample_file_name=sample_file_r1_row.seq_name,
                    data_type=data_type,
                    sample_name=sample_file_r1_row.sample_name,
                    session=sample_iddb_session)

                # if sample_name is empty on the next row then assume that row is also part of the current sample
                # this happens, for example, when there are forward and reverse read files
                print('next row sample_name: "{}"'.format(core_attr_df.loc[r1 + 1].sample_name))
                if str(core_attr_df.loc[r1 + 1].sample_name) == 'nan':
                    print('associating file "{}" with sample name "{}"'.format(
                        core_attr_df.loc[r1 + 1].seq_name,
                        sample_file_r1_row.sample_name))
                    sample_iddb.insert_sample_file_name_and_sample_name(
                        sample_file_name=core_attr_df.loc[r1 + 1].seq_name,
                        data_type=data_type,
                        sample_name=sample_file_r1_row.sample_name,
                        session=sample_iddb_session)

                sample_query_result = session.query(models.Sample).filter(
                    models.Sample.station_number == station_number,
                    models.Sample.cast_number == cast_number,
                    models.Sample.sample_name == sample_file_r1_row.sample_name).one_or_none()

                if sample_query_result is None:
                    print('** sample "{}":"{}" does not exist in the database'.format(
                        sample_file_r1_row.sample_name,
                        sample_file_r1_row.seq_name))
                    if load_data:
                        sample = models.Sample(
                            cruise=cruise,
                            collection_time_zone='HST',
                            collection_start=datetime.datetime.combine(
                                date=sample_file_r1_row.collection_date,
                                time=sample_file_r1_row.collection_time),
                            #collection_stop=datetime.datetime.combine(
                            #    date=sample_file_r1_row.collection_date,
                            #    time=sample_file_r1_row.collection_time),
                            depth=sample_file_r1_row.depth,
                            latitude_start=sample_latitude,
                            longitude_start=sample_longitude,
                            #latitude_stop=station.latitude,
                            #longitude_stop=station.longitude,
                            station_number=station.station_number,
                            cast_number=cast_number,
                            sample_name=sample_file_r1_row.sample_name)
                        sample.investigator_list.append(investigator)
                        session.add(sample)
                    else:
                        print('  sample will not be loaded')
                        sample = None
                else:
                    print('@@ found sample "{}"'.format(sample_query_result.sample_name))
                    sample = sample_query_result

                if load_data is False and sample is None:
                    # we are not loading data right now
                    pass
                else:
                    # load 'em up
                    if len(sample.sample_file_list) == 0:
                        print('  !! no sample files')
                    else:
                        print('  sample files are:\n\t{}'.format(
                            '\n\t'.join(
                                [f.file_ for f in sample.sample_file_list])))

                    # check the sample attributes
                    print('  {} sample attributes are present'.format(len(sample.sample_attr_list)))
                    print('\t{}'.format('\n\t'.join(sorted(['{}: "{}"'.format(a.sample_attr_type.type_, a.value) for a in sample.sample_attr_list]))))

                    for column_header, column_sample_attr_type in sorted(sample_attributes_present.items()):
                        sample_attrs_with_column_attr_type = [
                            a
                            for a
                            in sample.sample_attr_list
                            if a.sample_attr_type == column_sample_attr_type]
                        if len(sample_attrs_with_column_attr_type) == 0:
                            print('  sample has {} attribute(s) with sample attribute type "{}"'.format(
                                len(sample_attrs_with_column_attr_type), column_sample_attr_type.type_))
                        else:
                            # already printed this attribute above
                            pass

                        attr_value = sample_file_r1_row[column_header]
                        if str(attr_value) == 'nan':
                            # there was no value in the spreadsheet for this row and column
                            print('    no "{}" value for this sample in attribute file'.format(column_sample_attr_type.type_))
                        else:
                            if not load_data:
                                print('  attribute will not be loaded')
                            elif len(sample_attrs_with_column_attr_type) == 0:
                                # add a new sample attribute
                                print('    need to add value "{}" from column "{}"'.format(attr_value, column_header))
                                sample_attr = models.Sample_attr(value=attr_value)
                                sample_attr.sample = sample
                                sample_attr.sample_attr_type = column_sample_attr_type
                            elif len(sample_attrs_with_column_attr_type) == 1:
                                # everything is ok
                                pass
                            else:
                                # is something wrong?
                                print(sample_attrs_with_column_attr_type)
                                raise Exception('too many attributes with the same type?')
        print('all rows have been parsed')


file_type_table = {
    re.compile(r'contigs\.fastq'): 'Assembly',
    re.compile(r'genes\.fna'): 'Annotation Genes',
    re.compile(r'prodigal\.gff'): 'Annotation Prodigal',
    re.compile(r'proteins\.faa'): 'Peptides',
    re.compile(r'ribosomal_rRNA\.fna'): 'Ribosomal rRNA FASTA',
    re.compile(r'ribosomal_rRNA\.gff'): 'Ribosomal rRNA GFF'
}

reads_re = re.compile(r'\.(fasta|fastq)(\.(gz|bz2))?$')


def get_sample_file_type(data_object_name):
    matched_file_types = []
    for file_type_re, file_type in file_type_table.items():
        match = file_type_re.search(data_object_name)
        if match is None:
            pass
        else:
            matched_file_types.append(file_type)

    if len(matched_file_types) == 0:
        reads_match = reads_re.search(data_object_name)
        if reads_match is None:
            raise Exception('failed to find file type for "{}"'.format(data_object_name))
        else:
            return 'Reads'
    elif len(matched_file_types) == 1:
        return matched_file_types[0]
    else:
        raise Exception('found too many file types for "{}":\n\t{}'.format(
            data_object_name,
            '\n\t'.join([t for t in matched_file_types])))


def load_data_file(muscope_data_object, db_uri, sample_db_uri, load_data):
    print('loading data file "{}"'.format(muscope_data_object.path))

    with session_manager_from_db_uri(db_uri=db_uri) as db_session, \
            session_manager_from_db_uri(sample_db_uri) as sample_db_session:
        print('searching for sample file "{}"'.format(muscope_data_object.name))
        try:
            s = sample_iddb.find_sample_name_for_sample_file_name(
                muscope_data_object.name,
                sample_db_session)

            sample = db_session.query(
                models.Sample).filter(
                    models.Sample.sample_name == s.sample_name).one_or_none()

            if sample is None:
                # the BATS files should be ignored, for example
                error_msg = 'ERROR: failed to find sample with name "{}" for file "{} in {}"'.format(
                    s.sample_name,
                    muscope_data_object.name,
                    db_uri)
                print(error_msg)
            else:
                sample_file_query_result = db_session.query(
                    models.Sample_file).filter(
                        models.Sample_file.sample == sample,
                        models.Sample_file.file_ == muscope_data_object.path).one_or_none()

                if s.data_type is not None:
                    sample_file_type = s.data_type
                else:
                    sample_file_type = get_sample_file_type(muscope_data_object.name)
                print('sample file type is "{}"'.format(sample_file_type))

                if sample_file_query_result is None:
                    print('inserting sample_file "{}"'.format(muscope_data_object.path))
                    sample_file = models.Sample_file(file_=muscope_data_object.path)

                    sample_file.sample_file_type = db_session.query(
                        models.Sample_file_type).filter(models.Sample_file_type.type_ == sample_file_type).one()

                    sample_file.sample = sample
                else:
                    sample_file = sample_file_query_result
                    print('sample_file "{}" is already in the database'.format(muscope_data_object.path))
                    print('  file type is "{}"'.format(sample_file.sample_file_type.type_))
                    print('  setting file type to "{}"'.format(sample_file_type))
                    sample_file.sample_file_type = db_session.query(
                        models.Sample_file_type).filter(models.Sample_file_type.type_ == sample_file_type).one()

        except sa.orm.exc.MultipleResultsFound as mrf:
            # this probably means we have found one or more rows with mismatched cruise and sample
            # this is an error I caused earlier
            print('repairing sample "{}"'.format(s.sample_name))
            bad_samples = []
            sample_query_result = db_session.query(models.Sample).filter(
                models.Sample.sample_name == s.sample_name).all()
            print('found multiple samples with sample name "{}"'.format(s.sample_name))
            for sample in sample_query_result:
                if sample.cast.station.cruise.cruise_name == 'HOT268':
                    print('this is probably a bad sample and it will be deleted:')
                    bad_samples.append(sample)
                else:
                    print('this is probably a good sample:')

                print('    cruise: "{}" sample id: "{}" sample name: "{}"'.format(
                      sample.cast.station.cruise.cruise_name,
                      sample.sample_id,
                      sample.sample_name))

            for bad_sample in bad_samples:
                for bad_sample_attr in bad_sample.sample_attr_list:
                    db_session.delete(bad_sample_attr)
                db_session.delete(bad_sample)


def parse_attributes(spreadsheet_fp):
    core_attr_plus_data_df = pd.read_excel(
        spreadsheet_fp,
        sheet_name='core attributes + data',
        skiprows=(0, 2)
    )
    core_attr_plus_data_df.rename(columns={'depth_sample': 'depth'}, inplace=True)

    return core_attr_plus_data_df


def parse_Armbrust_HL2A_EukTxnDiel_seq_attrib__xls(spreadsheet_fp):
    """Nothing peculiar about this file.
    :param spreadsheet_fp:
    :return: core_attr_plus_data_df pandas.DataFrame
    """
    return parse_attributes(spreadsheet_fp)


def parse_Caron_HL2A_18Sdiel_seq_attrib_v2__xls(spreadsheet_fp):
    """Some parts of this file need adjustment.
    :param spreadsheet_fp:
    :return:
    """

    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    # column 10 header is on the wrong line
    column_10_header = core_attr_plus_data_df.columns[9]
    core_attr_plus_data_df.rename(columns={'depth_sample': 'depth', column_10_header: 'seq_name'}, inplace=True)

    # entries in the 'station' column look like 'S47' but we want just the number
    core_attr_plus_data_df.station = [
        (s if isinstance(s, float) else float(s[1:]))
        for s
        in core_attr_plus_data_df.station]

    # entries in the 'cast' column look like 'C1' but we want just the number
    core_attr_plus_data_df.cast_num = [
        (c if isinstance(c, float) else float(c[1:]))
        for c
        in core_attr_plus_data_df.cast_num]

    return core_attr_plus_data_df


def parse_Caron_HL2A_VertProf_seq_attrib_v3__xls(spreadsheet_fp):
    """Copy sample_name to rows with R1 file name.
    :param spreadsheet_fp:
    :return:
    """

    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    for (r1, row1), (r2, row2) in util.grouper(core_attr_plus_data_df.iterrows(), n=2):
        if r1 == 0:
            # skip row 0
            pass
        else:
            # copy attributes from previous sample ONLY IF THE ATTRIBUTE IS EMPTY
            column_names = list(core_attr_plus_data_df.columns)
            for attr_name in column_names:
                print('row {} attr "{}" is "{}"'.format(r1, attr_name, core_attr_plus_data_df.loc[r1, attr_name]))
                if str(core_attr_plus_data_df.loc[r1, attr_name]) in ('nan', 'NaT'):
                    print('  copy "{}" from previous sample'.format(core_attr_plus_data_df.loc[r1-2, attr_name]))
                    core_attr_plus_data_df.loc[r1, attr_name] = core_attr_plus_data_df.loc[r1-2, attr_name]
                else:
                    pass

    return core_attr_plus_data_df


def parse_Caron_HL3_VertProf_seq_attrib_v3__xls(spreadsheet_fp):
    """Nothing to adjust here.
    :param spreadsheet_fp:
    :return:
    """
    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    for (r1, row1), (r2, row2) in util.grouper(core_attr_plus_data_df.iterrows(), n=2):
        if r1 == 0:
            # skip row 0
            pass
        else:
            # copy attributes from previous sample ONLY IF THE ATTRIBUTE IS EMPTY
            column_names = list(core_attr_plus_data_df.columns)
            for attr_name in column_names:
                print('row {} attr "{}" is "{}"'.format(r1, attr_name, core_attr_plus_data_df.loc[r1, attr_name]))
                if str(core_attr_plus_data_df.loc[r1, attr_name]) in ('nan', 'NaT'):
                    print('  copy "{}" from previous sample'.format(core_attr_plus_data_df.loc[r1-2, attr_name]))
                    core_attr_plus_data_df.loc[r1, attr_name] = core_attr_plus_data_df.loc[r1-2, attr_name]
                else:
                    pass

    return core_attr_plus_data_df


def parse_Caron_HOT273_18Ssizefrac_seq_assoc_data_v2__xls(spreadsheet_fp):
    """Need to adjust the cruise name column and add collection time.
    :param spreadsheet_fp:
    :return:
    """
    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    # the cruise name column is just '273'
    ##core_attr_plus_data_df.cruise_name = 'HOT273'
    core_attr_plus_data_df.cruise_name = ['HOT{}'.format(n) for n in core_attr_plus_data_df.cruise_name]

    # the spreadsheet does not have collection_time
    core_attr_plus_data_df['collection_time'] = datetime.time(hour=0, minute=0, second=0)

    # return a dummy dataframe to fit in with the parse/load function scheme
    return core_attr_plus_data_df


def parse_Caron_HOTquarterly_18Sv4_seq_assoc_data_v2__xls(spreadsheet_fp):
    """Need to add 'HOT' to the cruise name.
    :param spreadsheet_fp:
    :return:
    """
    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    # the cruise name column is just '273'
    core_attr_plus_data_df.cruise_name = ['HOT{}'.format(n) for n in core_attr_plus_data_df.cruise_name]

    ## the spreadsheet does not have collection_time
    #core_attr_plus_data_df['collection_time'] = datetime.time(hour=0, minute=0, second=0)

    return core_attr_plus_data_df


def parse_Chisholm_HOT__BATS_seq_attrib__xls(spreadsheet_fp):
    """Remove BATS cruises.
    :param spreadsheet_fp:
    :return:
    """
    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    # this spreadsheet is missing the 'seq_name' column header
    core_attr_plus_data_df.rename(columns={'Unnamed: 9': 'seq_name'}, inplace=True)

    # cut off the BATS
    print('removing BATS cruises!')
    core_attr_plus_data_df = core_attr_plus_data_df.iloc[:132, :]

    return core_attr_plus_data_df


def parse_Chisholm_HOT263__283_Vesicle_seq_attrib_v2__xls(spreadsheet_fp):
    return parse_attributes(spreadsheet_fp)


def parse_Church_HOT201_222_Tricho16S_seq_assoc_v2__xls(spreadsheet_fp):
    """
    This spreadsheet has 'net tow' in the cast_num column. These will be changed to '0'.
    The cruise_name column has only numbers. 'HOT' will be prepended to them.
    The depth column is missing some values. For now use 175.
    The collection_time column is empty. '00:00:00' will be used.

    :param spreadsheet_fp:
    :return:
    """
    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    for (r1, row1), (r2, row2) in util.grouper(core_attr_plus_data_df.iterrows(), n=2):
        # replace 'net tow' with '0' in the cast_num column
        if core_attr_plus_data_df.loc[r1, 'cast_num'] == 'net tow':
            core_attr_plus_data_df.loc[r1, 'cast_num'] = 0

        # add 'HOT' to cruise_name
        core_attr_plus_data_df.loc[r1, 'cruise_name'] = \
            'HOT' + str(int(core_attr_plus_data_df.loc[r1, 'cruise_name']))

        # use 999 for missing sample depth and change it to null with the admin console
        if str(core_attr_plus_data_df.loc[r1, 'depth']) == 'nan':
            core_attr_plus_data_df.loc[r1, 'depth'] = 999

        if r1 == 0:
            # copy time from first date column to first time column
            core_attr_plus_data_df.loc[0, 'collection_time'] = core_attr_plus_data_df.collection_date[0].time()
        else:
            # copy attributes from previous sample ONLY IF THE ATTRIBUTE IS EMPTY
            column_names = list(core_attr_plus_data_df.columns)
            # do not copy over missing values in the depth column
            column_names.remove('depth')
            for attr_name in column_names:
                print('row {} attr "{}" is "{}"'.format(r1, attr_name, core_attr_plus_data_df.loc[r1, attr_name]))
                if str(core_attr_plus_data_df.loc[r1, attr_name]) in ('nan', 'NaT'):
                    print('  copy "{}" from previous sample'.format(core_attr_plus_data_df.loc[r1 - 2, attr_name]))
                    core_attr_plus_data_df.loc[r1, attr_name] = core_attr_plus_data_df.loc[r1 - 2, attr_name]
                else:
                    pass

    return core_attr_plus_data_df


def parse_DeLong_HL2A_DNAdiel_seq_assoc_data_v3__xls(spreadsheet_fp):
    return parse_attributes(spreadsheet_fp)


def parse_DeLong_HL2A_0__2frac_diel_seq_assoc_data_v3__xls(spreadsheet_fp):
    return parse_attributes(spreadsheet_fp)


def parse_DeLong_HL2A_RNAdiel_seq_assoc_data_v3__xls(spreadsheet_fp):
    return parse_attributes(spreadsheet_fp)


def parse_Dyhrman_HL4_incubation_seq_assoc_data_v5__xls(spreadsheet_fp):
    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    for (r1, row1), (r2, row2), (r3, row3), (r4, row4) in util.grouper(core_attr_plus_data_df.iterrows(), n=4):
        print(row1.seq_name)
        if row1.data_type == 'mRNA reads':
            core_attr_plus_data_df.loc[r1, 'data_type'] = 'mRNA Reads'
        else:
            raise Exception()
        if row3.data_type == 'total RNA reads':
            core_attr_plus_data_df.loc[r3, 'data_type'] = 'Total RNA Reads'
        else:
            raise Exception()

        # append .gz to file names
        if str(row1.seq_name).endswith('.fastq'):
            core_attr_plus_data_df.loc[r1, 'seq_name'] = row1.seq_name + '.gz'
        else:
            pass
        if str(row2.seq_name).endswith('.fastq'):
            core_attr_plus_data_df.loc[r2, 'seq_name'] = row2.seq_name + '.gz'
        else:
            pass
        if str(row3.seq_name).endswith('.fastq'):
            core_attr_plus_data_df.loc[r3, 'seq_name'] = row3.seq_name + '.gz'
        else:
            pass
        if str(row4.seq_name).endswith('.fastq'):
            core_attr_plus_data_df.loc[r4, 'seq_name'] = row4.seq_name + '.gz'
        else:
            pass

        # copy attributes from previous sample ONLY IF THE ATTRIBUTE IS EMPTY
        column_names = list(core_attr_plus_data_df.columns)
        for attr_name in column_names:
            #print('row {} attr "{}" is "{}"'.format(r3, attr_name, core_attr_plus_data_df.loc[r3, attr_name]))
            if str(core_attr_plus_data_df.loc[r3, attr_name]) in ('nan', 'NaT'):
                #print('  copy "{}" from previous sample'.format(core_attr_plus_data_df.loc[r1, attr_name]))
                core_attr_plus_data_df.loc[r3, attr_name] = core_attr_plus_data_df.loc[r1, attr_name]
            else:
                pass

    return core_attr_plus_data_df


def parse_Dyhrman_HL2A_incubation_seq_assoc_data_v5__xls(spreadsheet_fp):
    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    # 2 related samples appear in groups of 4 rows
    # the first sample is usually "mRNA"
    # the second sample is usually "totalRNA"

    # the "totalRNA" sample row is missing information that is duplicated
    # by its corresponding "mRNA" sample so this function will copy that
    # information to the "totalRNA" row

    # but there are a few cases of isolated "totalRNA" samples
    # that do have all the information already
    for (r1, row1), (r2, row2) in util.grouper(core_attr_plus_data_df.iterrows(), n=2):
        # remove .fastq.tar from sample names
        if str(row1.sample_name).endswith('.fastq.tar'):
            core_attr_plus_data_df.loc[r1, 'sample_name'] = row1.sample_name[:-10]
        else:
            pass

        # append .gz to file names
        if str(row1.seq_name).endswith('.fastq'):
            core_attr_plus_data_df.loc[r1, 'seq_name'] = row1.seq_name + '.gz'
        else:
            pass
        if str(row2.seq_name).endswith('.fastq'):
            core_attr_plus_data_df.loc[r2, 'seq_name'] = row2.seq_name + '.gz'
        else:
            pass

        # copy attributes from previous sample ONLY IF THE ATTRIBUTE IS EMPTY
        for attr_name in core_attr_plus_data_df.columns:
            #print('row {} attr "{}" is "{}"'.format(r1, attr_name, core_attr_plus_data_df.loc[r1, attr_name]))
            if str(core_attr_plus_data_df.loc[r1, attr_name]) in ('nan', 'NaT') and r1 > 0:
                #print('  copy "{}" from previous sample'.format(core_attr_plus_data_df.loc[r1 - 2, attr_name]))
                core_attr_plus_data_df.loc[r1, attr_name] = core_attr_plus_data_df.loc[r1 - 2, attr_name]
            else:
                pass

    return core_attr_plus_data_df


def parse_Dyhrman_HL2A_RNAdiel_seq_assoc_data_v5__xls(spreadsheet_fp):
    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    # change column name 'gene_name' to 'data_type' for now
    ##column_headers = list(core_attr_plus_data_df.columns)
    ##n = column_headers.index('gene_name')
    ##column_headers[n] = 'data_type'
    ##core_attr_plus_data_df.columns = column_headers

    for (r1, row1), (r2, row2), (r3, row3), (r4, row4) in util.grouper(core_attr_plus_data_df.iterrows(), n=4):
        # remove .fastq.tar from sample names
        if str(row1.sample_name).endswith('.fastq.tar'):
            core_attr_plus_data_df.loc[r1, 'sample_name'] = row1.sample_name[:-10]
        else:
            pass

        if row1.data_type == 'mRNA reads':
            core_attr_plus_data_df.loc[r1, 'data_type'] = 'mRNA Reads'
        else:
            raise Exception()
        if row3.data_type == 'total RNA reads':
            core_attr_plus_data_df.loc[r3, 'data_type'] = 'Total RNA Reads'
        else:
            raise Exception()

        # append .gz to file names
        if str(row1.seq_name).endswith('.fastq'):
            core_attr_plus_data_df.loc[r1, 'seq_name'] = row1.seq_name + '.gz'
        else:
            pass
        if str(row2.seq_name).endswith('.fastq'):
            core_attr_plus_data_df.loc[r2, 'seq_name'] = row2.seq_name + '.gz'
        else:
            pass
        if str(row3.seq_name).endswith('.fastq'):
            core_attr_plus_data_df.loc[r3, 'seq_name'] = row3.seq_name + '.gz'
        else:
            pass
        if str(row4.seq_name).endswith('.fastq'):
            core_attr_plus_data_df.loc[r4, 'seq_name'] = row4.seq_name + '.gz'
        else:
            pass

        # copy attributes from previous sample ONLY IF THE ATTRIBUTE IS EMPTY
        column_names = list(core_attr_plus_data_df.columns)
        for attr_name in column_names:
            #print('row {} attr "{}" is "{}"'.format(r3, attr_name, core_attr_plus_data_df.loc[r3, attr_name]))
            if str(core_attr_plus_data_df.loc[r3, attr_name]) in ('nan', 'NaT'):
                #print('  copy "{}" from previous sample'.format(core_attr_plus_data_df.loc[r1, attr_name]))
                core_attr_plus_data_df.loc[r3, attr_name] = core_attr_plus_data_df.loc[r1, attr_name]
            else:
                pass

    return core_attr_plus_data_df


def parse_Dyhrman_HL2A_Tricho_seq_attrib_v2__xls(spreadsheet_fp):
    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    for (r1, row1), (r2, row2) in util.grouper(core_attr_plus_data_df.iterrows(), n=2):
        # set station and cast to 0
        core_attr_plus_data_df.loc[r1, 'station'] = 0
        core_attr_plus_data_df.loc[r1, 'cast_num'] = 0
        core_attr_plus_data_df.loc[r2, 'station'] = 0
        core_attr_plus_data_df.loc[r2, 'cast_num'] = 0

        # append .gz to file names
        if str(row1.seq_name).endswith('.fastq'):
            core_attr_plus_data_df.loc[r1, 'seq_name'] = row1.seq_name + '.gz'
        else:
            pass
        if str(row2.seq_name).endswith('.fastq'):
            core_attr_plus_data_df.loc[r2, 'seq_name'] = row2.seq_name + '.gz'
        else:
            pass

        if core_attr_plus_data_df.loc[r1, 'data_type'] == 'reads':
            core_attr_plus_data_df.loc[r1, 'data_type'] = 'Reads'
        else:
            raise Exception()
        if core_attr_plus_data_df.loc[r2, 'data_type'] == 'reads':
            core_attr_plus_data_df.loc[r2, 'data_type'] = 'Reads'
        else:
            raise Exception()

        # copy attributes from previous sample ONLY IF THE ATTRIBUTE IS EMPTY
        column_names = list(core_attr_plus_data_df.columns)
        for attr_name in column_names:
            # print('row {} attr "{}" is "{}"'.format(r3, attr_name, core_attr_plus_data_df.loc[r3, attr_name]))
            if str(core_attr_plus_data_df.loc[r1, attr_name]) in ('nan', 'NaT') and r1 > 0:
                # print('  copy "{}" from previous sample'.format(core_attr_plus_data_df.loc[r1, attr_name]))
                core_attr_plus_data_df.loc[r1, attr_name] = core_attr_plus_data_df.loc[r1 - 2, attr_name]
            else:
                pass

    # convert to decimal degrees
    for (r1, row1), (r2, row2) in util.grouper(core_attr_plus_data_df.iterrows(), n=2):
        # print(core_attr_plus_data_df.loc[r1, 'latitude'])
        lat1, lat2 = core_attr_plus_data_df.loc[r1, 'latitude'].split()
        core_attr_plus_data_df.loc[r1, 'latitude'] = float(lat1) + (float(lat2) / 60)

        lon1, lon2 = core_attr_plus_data_df.loc[r1, 'longitude'].split()
        core_attr_plus_data_df.loc[r1, 'longitude'] = -1.0 * float(lon1) + (float(lon2) / 60)

    # these start out empty but get filled in by the attribute copy loop above
    core_attr_plus_data_df.loc[12, 'latitude'] = None
    core_attr_plus_data_df.loc[12, 'longitude'] = None
    core_attr_plus_data_df.loc[14, 'latitude'] = None
    core_attr_plus_data_df.loc[14, 'longitude'] = None

    return core_attr_plus_data_df


def parse_Dyhrman_MS_incubation_assoc_data_v5__xls(spreadsheet_fp):
    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    # 2 related samples appear in groups of 4 rows
    # the first sample is usually "mRNA"
    # the second sample is usually "totalRNA"

    # parse time strings such as '12:45:00' and '1245'
    # the first 3 collection times are datetime objects, the remaining collection times are just strings like "1205"
    time_re = re.compile(r'^(?P<hour>\d{1,2}):?(?P<minute>\d{1,2})(:(?P<second>)\d{1,2})?$')

    for (r1, row1), (r2, row2), (r3, row3), (r4, row4) in util.grouper(core_attr_plus_data_df.iterrows(), n=4):
        # change the cruise name to MESO-SCOPE
        core_attr_plus_data_df.loc[r1, 'cruise_name'] = 'MESO-SCOPE'

        if row1.data_type == 'mRNA reads':
            core_attr_plus_data_df.loc[r1, 'data_type'] = 'mRNA Reads'
        else:
            raise Exception()
        if row3.data_type == 'total RNA reads':
            core_attr_plus_data_df.loc[r3, 'data_type'] = 'Total RNA Reads'
        else:
            raise Exception()

        # convert the strings in collection_time to datetime.time objects
        ##print('"{}"'.format(row1.collection_time))
        collection_time_match = time_re.search(str(row1.collection_time))
        core_attr_plus_data_df.loc[row1.name, 'collection_time'] = datetime.time(
            hour=int(collection_time_match.group('hour')),
            minute=int(collection_time_match.group('minute')))

        # copy attributes from previous sample ONLY IF THE ATTRIBUTE IS EMPTY
        column_names = list(core_attr_plus_data_df.columns)
        for attr_name in column_names:
            #print('row {} attr "{}" is "{}"'.format(r3, attr_name, core_attr_plus_data_df.loc[r3, attr_name]))
            if str(core_attr_plus_data_df.loc[r3, attr_name]) in ('nan', 'NaT'):
                #print('  copy "{}" from previous sample'.format(core_attr_plus_data_df.loc[r1, attr_name]))
                core_attr_plus_data_df.loc[r3, attr_name] = core_attr_plus_data_df.loc[r1, attr_name]
            else:
                pass

    return core_attr_plus_data_df


def cli():
    return main(sys.argv[1:])


if __name__ == '__main__':
    cli()
