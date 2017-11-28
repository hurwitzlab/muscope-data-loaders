"""
Import data from

/iplant/home/scope/data/caron/HL2A/Caron_HL2A_18Sdiel_seq_attrib_v2.xls
/iplant/home/scope/data/caron/HL2A/Caron_HL2A_VertProf_seq_attrib_v2.xls

install python irods client
use python irods client to list data files

"""
import datetime
import os
import re
import sys

from irods.keywords import FORCE_FLAG_KW
from irods.session import iRODSSession

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

import muscope_loader
import muscope_loader.models as models
import muscope_loader.util as util

import muscope_loader.cruise.sample_iddb as sample_iddb
import muscope_loader.cruise.station_db as station_db

from orminator import session_manager


def main(argv):

    sample_iddb.build()
    station_db.build()

    muscope_collection_paths = (
        #'/iplant/home/scope/data/armbrust/HL2A',
        # check '/iplant/home/scope/data/caron/HL2A',
        '/iplant/home/scope/data/caron/HL3',
        # check '/iplant/home/scope/data/caron/HOT/268_271_275_279',
        # check '/iplant/home/scope/data/caron/HOT/273',
        '/iplant/home/scope/data/chisholm/HOT',
        # check '/iplant/home/scope/data/delong/HL2A',
        '/iplant/home/scope/data/dyhrman/HL4',
    )

    for c in muscope_collection_paths:
        print('processing collection "{}"'.format(c))
        process_muscope_collection(c)

    return 0


def process_muscope_collection(muscope_collection_path):
    """
    List the contents of the argument (a collection) and recursively list the contents of subcollections.
    When a data object is found look for a function that can parse it based on its name.
    :param muscope_collection_path:
    :return:
    """

    # connect to database on server
    # e.g. mysql+pymysql://imicrobe:<password>@localhost/muscope2
    Session_class = sessionmaker(
        bind=sa.create_engine(
            os.environ.get('MUSCOPE_DB_URI'), echo=False))

    data_file_endings = re.compile(r'\.(fastq|fasta|fna|gff|faa)(\.(gz|bz2))?$')
    attribute_file_endings = re.compile(r'\.xlsx?')

    loaded_file_paths = []
    unrecognized_file_paths = []

    load_data = True

    unprocessed_collection_paths = [muscope_collection_path]
    while len(unprocessed_collection_paths) > 0:
        c = unprocessed_collection_paths.pop(0)
        print('processing collection "{}"\n'.format(c))

        with iRODSSession(irods_env_file=os.path.expanduser('~/.irods/irods_environment.json')) as irods_session:
            muscope_collection = irods_session.collections.get(c)

            for muscope_data_object in muscope_collection.data_objects:
                #print('found data object {} in {}'.format(muscope_data_object.name, muscope_collection.path))

                attribute_file_match = attribute_file_endings.search(muscope_data_object.name)
                data_file_match = data_file_endings.search(muscope_data_object.name)

                if attribute_file_match is not None:
                    print('found an attribute file {}'.format(muscope_data_object.path))
                    conjectured_parse_function_name = 'parse_' + muscope_data_object.name.replace('.', '__')
                    print('looking for parse function with name "{}"'.format(conjectured_parse_function_name))
                    if conjectured_parse_function_name in sys.modules[__name__].__dict__:
                        parse_function = sys.modules[__name__].__dict__[conjectured_parse_function_name]

                        local_attribute_file_fp = os.path.join(
                            os.path.dirname(muscope_loader.__file__),
                            'downloads',
                            muscope_data_object.name)

                        print('copying attribute file "{}" to "{}"'.format(
                            muscope_data_object.path,
                            local_attribute_file_fp))

                        irods_session.data_objects.get(
                            muscope_data_object.path,
                            local_attribute_file_fp,
                            **{FORCE_FLAG_KW: True})

                        attr_df = parse_function(local_attribute_file_fp)
                        if load_data:
                            load_attributes(attr_df, session_class=Session_class)
                        else:
                            pass

                    else:
                        print('    no parse function by that name')
                        unrecognized_file_paths.append(muscope_data_object.path)
                elif data_file_match is not None:
                    try:
                        match = parse_data_file_path_or_name(muscope_data_object.path)
                        if match is None:
                            print('failed to parse "{}"'.format(muscope_data_object.path))
                        elif load_data:
                            print('parsed "{}"'.format(muscope_data_object.path))
                            load_data_file(muscope_data_object, match, session_class=Session_class)
                            loaded_file_paths.append(muscope_data_object.path)

                            # just load a few files while testing
                            #if len(loaded_file_paths) > 20:
                            #    collection_paths = []
                            #    break

                        else:
                            pass

                        # I need some space
                        print()

                    except util.FileNameException as fne:
                        print(fne)
                        unrecognized_file_paths.append(muscope_data_object.path)

                else:
                    print('nothing to do with file "{}"'.format(muscope_data_object.path))

            # add collections to the list of collections
            for subcollection in muscope_collection.subcollections:
                print('adding subcollection path "{}"'.format(subcollection.path))
                unprocessed_collection_paths.append(subcollection.path)

    print('loaded {} file path(s):\n\t{}'.format(
        len(loaded_file_paths),
        '\n\t'.join(loaded_file_paths)))
    print('failed to recognize {} file path(s):\n\t{}'.format(
        len(unrecognized_file_paths),
        '\n\t'.join(unrecognized_file_paths)))


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
        r'(?P<sample_name>([A-Z0-9]+)-(\d+[a-z]+)-(S\d+C\d+)-(\d+))')
    : 'DeLong HL2A',

#
#  SM125_S42_L008_R1_001.fastq.gz
#
    re.compile(
        r'(?P<sample_name>[A-Z]+\d+_[A-Z]+\d+)'
        r'_L\d+_R[12]_\d+\.fastq(\.gz)?$')
    : 'Dyhrman HL4'
}


def parse_data_file_path_or_name(path_or_name):
    print('parsing data file path or name "{}"'.format(path_or_name))

    sample_path_match_set = {
        sample_name_pattern.search(path_or_name)
        for sample_name_pattern, sample_name_pattern_label
        in sample_name_pattern_table.items() }

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


def load_attributes(core_attr_df, session_class):
    sample_iddb_session_class = sessionmaker(bind=sample_iddb.get_engine())
    station_session_class = sessionmaker(bind=station_db.get_engine())

    ##sample_name_file_fragment = sample_name_match.group('sample_name')
    with session_manager(session_class) as session, \
            session_manager(sample_iddb_session_class) as sample_iddb_session, \
            session_manager(station_session_class) as station_session:
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
        # the following did not work for Armbrust
        ##for (r1, sample_file_r1_row), (r2, sample_file_r2_row) in util.grouper(core_attr_df.iterrows(), n=2):
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
                    session.add(cruise)
                else:
                    cruise = cruise_query_result

                station_number = int(sample_file_r1_row.station)
                cast_number = int(sample_file_r1_row.cast_num)
                print('  on row {} station_number is "{}" and cast number is "{}"'.format(r1, station_number, cast_number))

                station = station_db.find_station(
                    cruise_name=cruise.cruise_name,
                    station_number=station_number,
                    session=station_session)

                # create an association between the sample_name and the identifying file name fragment
                identifying_file_name_fragment = parse_data_file_path_or_name(
                    sample_file_r1_row.seq_name).group('sample_name')

                print('found file name fragment "{}" for sample with name "{}"'.format(
                    identifying_file_name_fragment,
                    sample_file_r1_row.seq_name))

                sample_iddb.insert_sample_info(
                    sample_name=sample_file_r1_row.sample_name,
                    sample_file_identifier=identifying_file_name_fragment,
                    session=sample_iddb_session)

                print('associated sample name "{}" with file name fragment "{}"'.format(
                    sample_file_r1_row.sample_name,
                    identifying_file_name_fragment))

                # most rows do not have sample name
                # it can be derived from seq_name
                ##seq_name_pattern = re.compile(r'(?P<sample_name>July_(\d+m|DCM))')
                ##seq_name_pattern_match = seq_name_pattern.search(sample_file_r1_row.seq_name)
                ##derived_sample_name = seq_name_pattern_match.group('sample_name')
                sample_query_result = session.query(models.Sample).filter(
                    models.Sample.station_number == station_number,
                    models.Sample.cast_number == cast_number,
                    models.Sample.sample_name == sample_file_r1_row.sample_name).one_or_none()
                    # some samples are missing seq_name so if we add it to the search we overlook the existing record
                    ##models.Sample.seq_name == sample_file_r1_row.seq_name).one_or_none()

                if sample_query_result is None:
                    print('** sample "{}":"{}" does not exist in the database'.format(
                        sample_file_r1_row.sample_name,
                        sample_file_r1_row.seq_name))
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
                        latitude_start=station.latitude,
                        longitude_start=station.longitude,
                        #latitude_stop=station.latitude,
                        #longitude_stop=station.longitude,
                        station_number=station.station_number,
                        cast_number=cast_number,
                        sample_name=sample_file_r1_row.sample_name)
                    sample.investigator_list.append(investigator)
                    session.add(sample)
                else:
                    print('@@ found sample "{}"'.format(sample_query_result.sample_name))
                    sample = sample_query_result

                # are the sample files in the database?
                if len(sample.sample_file_list) == 0:
                    print('  !! no sample files')
                else:
                    print('  sample files are:\n\t'.format(
                        '\n\t'.join(
                            [s.file_ for s in sample.sample_file_list])))

                # check the sample attributes
                print('  {} sample attributes are present'.format(len(sample.sample_attr_list)))

                for column_header, column_sample_attr_type in sample_attributes_present.items():
                    sample_attrs_with_column_attr_type = [
                        a
                        for a
                        in sample.sample_attr_list
                        if a.sample_attr_type == column_sample_attr_type]
                    print('  sample has {} attribute(s) with sample attribute type "{}"'.format(
                        len(sample_attrs_with_column_attr_type), column_sample_attr_type.type_))

                    attr_value = sample_file_r1_row[column_header]
                    if str(attr_value) == 'nan':
                        # there was no value in the spreadsheet for this row and column
                        print('    no "{}" value for this sample'.format(column_sample_attr_type.type_))
                    else:
                        if len(sample_attrs_with_column_attr_type) == 0:
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
        print('is this a file of reads?')
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


def load_data_file(muscope_data_object, sample_name_match, session_class):
    sample_iddb_session_class = sessionmaker(bind=sample_iddb.get_engine())

    print('loading data file "{}"'.format(muscope_data_object.path))

    sample_name_file_fragment = sample_name_match.group('sample_name')
    with session_manager(session_class) as session, session_manager(sample_iddb_session_class) as sample_iddb_session:
        print('searching for sample.sample_name "{}"'.format(sample_name_file_fragment))
        try:
            sample_name = sample_iddb.find_sample_name_for_sample_file_identifier(
                sample_name_file_fragment, sample_iddb_session)

            sample = session.query(models.Sample).filter(
                models.Sample.sample_name == sample_name).one_or_none()

            if sample is None:
                # the BATS files should be ignored, for example
                error_msg = 'ERROR: failed to find sample with name "{}" for file "{}"'.format(sample_name, muscope_data_object.name)
                print(error_msg)
                #raise Exception(error_msg)
            else:
                sample_file_query_result = session.query(models.Sample_file).filter(
                    models.Sample_file.sample == sample,
                    models.Sample_file.file_ == muscope_data_object.path).one_or_none()

                sample_file_type = get_sample_file_type(muscope_data_object.name)
                print('sample file type is "{}"'.format(sample_file_type))

                if sample_file_query_result is None:
                    print('inserting sample_file "{}"'.format(muscope_data_object.path))
                    sample_file = models.Sample_file(file_=muscope_data_object.path)

                    sample_file.sample_file_type = session.query(
                        models.Sample_file_type).filter(models.Sample_file_type.type_ == sample_file_type).one()

                    sample_file.sample = sample
                else:
                    sample_file = sample_file_query_result
                    print('sample_file "{}" is already in the database'.format(muscope_data_object.path))
                    print('  file type is "{}"'.format(sample_file.sample_file_type.type_))
                    print('  setting file type to "{}"'.format(sample_file_type))
                    sample_file.sample_file_type = session.query(
                        models.Sample_file_type).filter(models.Sample_file_type.type_ == sample_file_type).one()


        except sa.orm.exc.MultipleResultsFound as mrf:
            # this probably means we have found one or more rows with mismatched cruise and sample
            # this is an error I caused earlier
            print('repairing sample "{}"'.format(sample_name))
            bad_samples = []
            sample_query_result = session.query(models.Sample).filter(
                models.Sample.sample_name == sample_name).all()
            print('found multiple samples with sample name "{}"'.format(sample_name))
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
                    session.delete(bad_sample_attr)
                session.delete(bad_sample)


def parse_attributes(spreadsheet_fp):
    core_attr_plus_data_df = pd.read_excel(
        spreadsheet_fp,
        sheet_name='core attributes + data',
        skiprows=(0,2)
    )
    core_attr_plus_data_df.rename(columns={'depth_sample': 'depth'}, inplace=True)

    # combine collection date and collection time

    print('core attributes + data')
    print(core_attr_plus_data_df.head())

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

    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)\

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


def parse_Caron_HL2A_VertProf_seq_attrib_v2__xls(spreadsheet_fp):
    """Nothing to adjust here.
    :param spreadsheet_fp:
    :return:
    """
    return parse_attributes(spreadsheet_fp)


def parse_Caron_HL3_VertProf_seq_attrib_v2__xls(spreadsheet_fp):
    """Nothing to adjust here.
    :param spreadsheet_fp:
    :return:
    """
    return parse_attributes(spreadsheet_fp)


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


def parse_DeLong_HL2A_DNAdiel_seq_assoc_data_v2__xlsx(spreadsheet_fp):
    return parse_attributes(spreadsheet_fp)


def parse_Dyhrman_HL4_incubation_seq_assoc_data_v3__xls(spreadsheet_fp):
    core_attr_plus_data_df = parse_attributes(spreadsheet_fp)

    # there are 4 seq_names for each sample name
    # the first 2 seq_names are paired-end reads e.g.
    #   SM125_S42_L008_R1_001.fastq and SM125_S42_L008_R2_001.fastq
    # the second 2 seq_names are paired-end reads e.g.
    #   SM143_S33_L002_R1_001.fastq and SM143_S33_L002_R2_001.fastq

    # this re will extract a new sample name from each R1 seq_name
    new_sample_name_re = re.compile(r'^(?P<new_sample_name>[A-Z]+\d+_[A-Z]+\d+)_L\d+_R[12]_\d+\.fastq$')

    # the old sample_names will go into sample_description_column
    # a sample_description will be entered for the 3rd row in addition to the 1st row
    ##sample_description_column = []
    for (r1, row1), (r2, row2), (r3, row3), (r4, row4) in util.grouper(core_attr_plus_data_df.iterrows(), n=4):
        print(row1.seq_name)
        ##sample_description_column.append(row1.sample_name)  # e.g. insitu_25m_051116_rep2
        ##sample_description_column.append(row2.sample_name)  # empty
        ##sample_description_column.append(row1.sample_name)  # e.g insitu_25m_051116_rep2
        ##sample_description_column.append(row2.sample_name)  # empty

        # extract a new sample name from each R1 seq_name
        ##new_sample_name_1 = new_sample_name_re.search(row1.seq_name).group('new_sample_name')
        ##new_sample_name_3 = new_sample_name_re.search(row3.seq_name).group('new_sample_name')

        # convert the strings in collection_time to datetime.time objects
        core_attr_plus_data_df.loc[row1.name, 'collection_time'] = datetime.time(
            hour=int(str(row1.collection_time)[0:2]),
            minute=int(str(row1.collection_time)[2:4]))

        ##core_attr_plus_data_df.loc[row1.name, 'sample_name'] = new_sample_name_1
        core_attr_plus_data_df.loc[row3.name, 'sample_name'] = row1.sample_name

        # copy attributes from 1st row to 3rd row
        core_attr_plus_data_df.loc[row3.name, 0:8] = core_attr_plus_data_df.iloc[row1.name, 0:8]
        core_attr_plus_data_df.loc[row3.name, 10:] = core_attr_plus_data_df.iloc[row1.name, 10:]

    # don't do this
    ##core_attr_plus_data_df['sample_description'] = sample_description_column

    return core_attr_plus_data_df


def cli():
    return main(sys.argv[1:])


if __name__ == '__main__':
    cli()
