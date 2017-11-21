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


def main(argv):

    # look for the usual irods file
    #with iRODSSession(irods_env_file=os.path.expanduser('~/.irods/irods_environment.json')) as irods_session:
    #    print(irods_session.host)
    #    print(irods_session.port)
    #    print(irods_session.username)
    #    print(irods_session.zone)

    muscope_collection_paths = (
        '/iplant/home/scope/data/armbrust/HL2A',
        #'/iplant/home/scope/data/caron/HL2A',
        #'/iplant/home/scope/data/caron/HL3',
        #'/iplant/home/scope/data/caron/HOT/268_271_275_279',
        #'/iplant/home/scope/data/caron/HOT/273',
        #'/iplant/home/scope/data/delong/HL2A',
    )

    for c in muscope_collection_paths:
        print('processing collection "{}"'.format(c))
        process_muscope_collection(c)
        # do we already have this file locally?
        #local_fp = os.path.join('downloads', file_name)
        #write_data_object_to_file(data_object_src=d, local_fp_dest=local_fp)

    return 0


def process_muscope_collection(muscope_collection_path):
    """
    List the contents of the argument (a collection) and recursively list the contents of subcollections.
    When a data object is found look for a function that can parse it based on its name.
    :param muscope_collection_path:
    :return:
    """

    # connect to database on server
    # e.g. mysql+pymysql://imicrobe:<password>@localhost/muscope
    db_uri = os.environ.get('MUSCOPE_DB_URI')
    imicrobe_engine = sa.create_engine(db_uri, echo=False)
    Session_class = sessionmaker(bind=imicrobe_engine)

    data_file_endings = re.compile(r'\.(fastq|fasta|fna|gff|faa)(\.gz)?$')

    loaded_file_paths = []
    unrecognized_file_paths = []

    load_data = True

    collection_paths = [muscope_collection_path]
    while len(collection_paths) > 0:
        c = collection_paths.pop(0)
        print('processing collection "{}"\n'.format(c))

        with iRODSSession(irods_env_file=os.path.expanduser('~/.irods/irods_environment.json')) as irods_session:
            muscope_collection = irods_session.collections.get(c)

            for muscope_data_object in muscope_collection.data_objects:
                #print('found data object {} in {}'.format(muscope_data_object.name, muscope_collection.path))

                data_file_match = data_file_endings.search(muscope_data_object.name)
                if data_file_match is not None:
                    try:
                        match = parse_data_file_path(muscope_data_object)
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

                elif muscope_data_object.name.endswith('.xls') or muscope_data_object.name.endswith('.xlsx'):
                    print('found an attribute file? {}'.format(muscope_data_object.path))
                    conjectured_parse_function_name = 'parse_' + muscope_data_object.name.replace('.', '__')
                    print('looking for parse function with name "{}"'.format(conjectured_parse_function_name))
                    if conjectured_parse_function_name in sys.modules[__name__].__dict__:
                        print('    found it')
                        parse_function = sys.modules[__name__].__dict__[conjectured_parse_function_name]

                        # get the file
                        local_file_fp = os.path.join(
                            os.path.dirname(muscope_loader.__file__),
                            'downloads',
                            muscope_data_object.name)

                        irods_session.data_objects.get(
                            muscope_data_object.path, local_file_fp, options={FORCE_FLAG_KW: True})

                        # call it
                        x = parse_function(local_file_fp, muscope_data_object.path)
                        if load_data:
                            load_attributes(*x, session_class=Session_class)
                        else:
                            pass

                    else:
                        print('    no parse function by that name')
                        unrecognized_file_paths.append(muscope_data_object.path)
                else:
                    print('nothing to do with file "{}"'.format(muscope_data_object.path))

            # add collections to the list of collections
            for subcollection in muscope_collection.subcollections:
                print('adding subcollection path "{}"'.format(subcollection.path))
                collection_paths.append(subcollection.path)

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
# This re recognizes the last directory name.
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
        r'delong/HL2A/[A-Z0-9]+-\d+/'
        r'(?P<sample_name>([A-Z0-9]+)-(\d+[a-z]+)-(S\d+C\d+)-(\d+))/')
    : 'DeLong HL2A',
}


def parse_data_file_path(muscope_data_object):
    print('parsing data file path "{}"'.format(muscope_data_object.path))

    sample_path_match_set = {
        sample_name_pattern.search(muscope_data_object.path)
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
        raise util.FileNameException('failed to parse file path "{}"'.format(muscope_data_object.path))
    elif len(sample_path_match_set) == 1:
        return sample_path_match_set.pop()
    else:
        raise util.FileNameException('too many matches:\n\t{}'.format('\n\t'.join([
            sample_name_pattern_table[match.re] for match in sample_path_match_set])))


file_type_table = {
    re.compile(r'contigs\.fastq'): 'Assembly',
    re.compile(r'genes\.fna'): 'Annotation Genes',
    re.compile(r'prodigal\.gff'): 'Annotation Prodigal',
    re.compile(r'proteins\.faa'): 'Peptides',
    re.compile(r'ribosomal_rRNA\.fna'): 'Ribosomal rRNA FASTA',
    re.compile(r'ribosomal_rRNA\.gff'): 'Ribosomal rRNA GFF'
}

reads_re = re.compile(r'\.(fasta|fastq)(\.gz)?$')

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
    print('loading data file "{}"'.format(muscope_data_object.path))

    sample_name = sample_name_match.group('sample_name')
    with util.session_(session_class) as session:
        print('searching for sample.sample_name "{}"'.format(sample_name))
        try:
            sample = session.query(models.Sample).filter(
                models.Sample.sample_name == sample_name).one_or_none()

            if sample is None:
                error_msg = 'failed to find sample with name "{}" for file "{}"'.format(sample_name, muscope_data_object.name)
                print(error_msg)
                raise Exception(error_msg)
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


def parse_Armbrust_HL2A_EukTxnDiel_seq_attrib__xls(spreadsheet_fp, iplant_path):
    core_attr_plus_data_df = pd.read_excel(
        spreadsheet_fp,
        sheet_name='core attributes + data',
        skiprows=(0,2)
    )
    core_attr_plus_data_df.rename(columns={'depth_sample': 'depth'}, inplace=True)

    print('core attributes + data')
    print(core_attr_plus_data_df.head())

    # return a dummy dataframe to fit in with the parse/load function scheme
    return pd.DataFrame(data={}), core_attr_plus_data_df


def parse_Caron_HL2A_18Sdiel_seq_attrib_v2__xls(spreadsheet_fp, iplant_path):
    """
    File Caron_HL2A_18Sdiel_seq_attrib_v2_xls has three sheets:
        'README'
        'core attributes + data'
        'additional attributes'

    Sheet 'README' has two separate blocks of data.

    :param spreadsheet_fp:
    :return:
    """

    print('START parsing "{}"'.format(spreadsheet_fp))

    readme_block_1_df = pd.read_excel(
        spreadsheet_fp,
        sheet_name='README',
        index_col='Cast #',
        skip_footer=27,
        usecols=range(9, 22)
    )
    print('README block 1')
    print(readme_block_1_df.head())

    readme_block_2_df = pd.read_excel(
        spreadsheet_fp,
        sheet_name='README',
        header=22,
        index_col='Cast #',
        skip_rows=range(23),
        skip_footer=5,
        usecols=range(9, 17)
    )
    print('README block 2')
    print(readme_block_2_df.head())

    readme_df = pd.merge(left=readme_block_1_df, left_index=True, right=readme_block_2_df, right_index=True)
    print('all README data')
    print(readme_df.head())

    core_attr_plus_data_df = pd.read_excel(
        spreadsheet_fp,
        sheet_name='core attributes + data',
        skiprows=(0,2)
    )

    print('UNMODIFIED core attributes + data:\n{}'.format(core_attr_plus_data_df.head()))

    # column 10 header is on the wrong line
    column_10_header = core_attr_plus_data_df.columns[9]
    core_attr_plus_data_df.rename(columns={'depth_sample': 'depth', column_10_header: 'seq_name'}, inplace=True)

    # create a new column 'file' with iplant path and file name
    core_attr_plus_data_df = core_attr_plus_data_df.assign(
        file_=[os.path.join(iplant_path, seq_name) for seq_name in core_attr_plus_data_df.seq_name])

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

    print('MODIFIED core attributes + data:\n{}'.format(core_attr_plus_data_df.head()))

    print('DONE parsing "{}"'.format(spreadsheet_fp))

    return readme_df, core_attr_plus_data_df


#def load_Caron_HL2A_18Sdiel_seq_attrib_v2__xls(readme_df, core_attr_df, session_class):
#    load_Caron_HL2A_VertProf_seq_attrib_v2__xls(readme_df, core_attr_df, session_class)


def parse_Caron_HL2A_VertProf_seq_attrib_v2__xls(spreadsheet_fp, iplant_path):
    """
    File Caron_HL2A_VertProf_seq_attrib_v2_xls has three sheets:
        'README'
        'core attributes + data'
        'additional attributes'

    Sheet 'README' has no data.

    :param spreadsheet_fp:
    :return:
    """
    core_attr_plus_data_df = pd.read_excel(
        spreadsheet_fp,
        sheet_name='core attributes + data',
        skiprows=(0,2)
    )
    core_attr_plus_data_df.rename(columns={'depth_sample': 'depth'}, inplace=True)

    # create a new column 'file' with iplant path and file name
    core_attr_plus_data_df = core_attr_plus_data_df.assign(
        file_=[os.path.join(iplant_path, seq_name) for seq_name in core_attr_plus_data_df.seq_name])

    print('core attributes + data')
    print(core_attr_plus_data_df.head())

    # return a dummy dataframe to fit in with the parse/load function scheme
    return pd.DataFrame(data={}), core_attr_plus_data_df


def parse_Caron_HL3_VertProf_seq_attrib_v2__xls(spreadsheet_fp, iplant_path):
    return parse_Caron_HL2A_VertProf_seq_attrib_v2__xls(spreadsheet_fp, iplant_path)


#def load_Caron_HL3_VertProf_seq_attrib_v2__xls(readme_df, core_attr_df, session_class):
#    load_Caron_HL2A_VertProf_seq_attrib_v2__xls(readme_df, core_attr_df, session_class)


def parse_Caron_HOT273_18Ssizefrac_seq_assoc_data_v2__xls(spreadsheet_fp, iplant_path):
    """
    File Caron_HOT273_18Ssizefrac_seq_attrib_v2_xls has three sheets:
        'README'
        'core attributes + data'
        'additional attributes'

    Sheet 'README' has no data.

    :param spreadsheet_fp:
    :return:
    """
    core_attr_plus_data_df = pd.read_excel(
        spreadsheet_fp,
        sheet_name='core attributes + data',
        skiprows=(0,2)
    )
    core_attr_plus_data_df.rename(columns={'depth_sample': 'depth'}, inplace=True)

    # create a new column 'file' with iplant path and file name
    core_attr_plus_data_df = core_attr_plus_data_df.assign(
        file_=[os.path.join(iplant_path, seq_name) for seq_name in core_attr_plus_data_df.seq_name])

    # the cruise name column is just '273'
    core_attr_plus_data_df.cruise_name = 'HOT273'

    # the spreadsheet does not have collection_time
    core_attr_plus_data_df['collection_time'] = datetime.time(hour=0, minute=0, second=0)

    print('core attributes + data')
    print(core_attr_plus_data_df.head())

    # return a dummy dataframe to fit in with the parse/load function scheme
    return pd.DataFrame(data={}), core_attr_plus_data_df


#def load_Caron_HOT273_18Ssizefrac_seq_assoc_data_v2__xls(readme_df, core_attr_df, session_class):
#    return load_Caron_HL2A_VertProf_seq_attrib_v2__xls(readme_df, core_attr_df, session_class)


def parse_Caron_HOTquarterly_18Sv4_seq_assoc_data_v2__xls(spreadsheet_fp, iplant_path):
    """
    File Caron_HOT273_18Ssizefrac_seq_attrib_v2_xls has three sheets:
        'README'
        'core attributes + data'
        'additional attributes'

    Sheet 'README' has no data.

    :param spreadsheet_fp:
    :return:
    """
    core_attr_plus_data_df = pd.read_excel(
        spreadsheet_fp,
        sheet_name='Sample_information',
        skiprows=(0,2)
    )
    core_attr_plus_data_df.rename(columns={'depth_sample': 'depth'}, inplace=True)

    # create a new column 'file' with iplant path and file name
    core_attr_plus_data_df = core_attr_plus_data_df.assign(
        file_=[os.path.join(iplant_path, seq_name) for seq_name in core_attr_plus_data_df.seq_name])

    # the cruise name column is just '273'
    core_attr_plus_data_df.cruise_name = ['HOT{}'.format(n) for n in core_attr_plus_data_df.cruise_name]

    ## the spreadsheet does not have collection_time
    #core_attr_plus_data_df['collection_time'] = datetime.time(hour=0, minute=0, second=0)

    print('Sample_information')
    print(core_attr_plus_data_df.head())

    # return a dummy dataframe to fit in with the parse/load function scheme
    return pd.DataFrame(data={}), core_attr_plus_data_df


def parse_DeLong_HL2A_DNAdiel_seq_assoc_data_v2__xlsx(spreadsheet_fp, iplant_path):
    core_attr_plus_data_df = pd.read_excel(
        spreadsheet_fp,
        sheet_name='core attributes + data',
        skiprows=(0,2)
    )
    core_attr_plus_data_df.rename(columns={'depth_sample': 'depth'}, inplace=True)

    print('core attributes + data')
    print(core_attr_plus_data_df.head())

    # return a dummy dataframe to fit in with the parse/load function scheme
    return pd.DataFrame(data={}), core_attr_plus_data_df


def load_attributes(readme_df, core_attr_df, session_class):

    ##investigator_first_name = 'David A.'
    ##investigator_last_name = 'Caron'

    # do the sample records exist?
    with util.session_(session_class) as session:

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

        for (r1, sample_file_r1_row), (r2, sample_file_r2_row) in util.grouper(core_attr_df.iterrows(), n=2):
            print('sample "{}"'.format(sample_file_r1_row.sample_name))

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

                station_query_result = session.query(models.Station).filter(
                    models.Station.cruise == cruise,
                    models.Station.station_number == station_number).one_or_none()

                if station_query_result is None:
                    print('failed to find station {}'.format(station_number))
                    station = models.Station(
                        station_number=station_number,
                        latitude=0.0,
                        longitude=0.0)
                    station.cruise = cruise
                    session.add(station)
                else:
                    print('found station {}'.format(station_number))
                    station = station_query_result

                cast_query_result = session.query(models.Cast).filter(
                    models.Cast.station_id == station.station_id,
                    models.Cast.cast_number == cast_number).one_or_none()

                if cast_query_result is None:
                    print('failed to find cast {} for station {}'.format(cast_number, station_number))
                    cast = models.Cast(
                        cast_number=cast_number,
                        collection_date=sample_file_r1_row.collection_date.isoformat(),
                        collection_time=sample_file_r1_row.collection_time.isoformat(timespec='seconds'),
                        collection_time_zone='HST'
                    )
                    cast.station = station
                    session.add(cast)
                    print(cast.collection_time)
                else:
                    print('found cast {}'.format(cast_number))
                    cast = cast_query_result

                # most rows do not have sample name
                # it can be derived from seq_name
                ##seq_name_pattern = re.compile(r'(?P<sample_name>July_(\d+m|DCM))')
                ##seq_name_pattern_match = seq_name_pattern.search(sample_file_r1_row.seq_name)
                ##derived_sample_name = seq_name_pattern_match.group('sample_name')
                sample_query_result = session.query(models.Sample).filter(
                    models.Sample.cast_id == cast.cast_id,
                    models.Sample.sample_name == sample_file_r1_row.sample_name).one_or_none()
                    # some samples are missing seq_name so if we add it to the search we overlook the existing record
                    ##models.Sample.seq_name == sample_file_r1_row.seq_name).one_or_none()

                if sample_query_result is None:
                    print('** sample "{}":"{}" does not exist in the database'.format(
                        sample_file_r1_row.sample_name,
                        sample_file_r1_row.seq_name))
                    sample = models.Sample(
                        #investigator=investigator,
                        #cast=cast,
                        sample_name=sample_file_r1_row.sample_name,
                        seq_name=sample_file_r1_row.seq_name)
                    sample.cast = cast
                    sample.investigator = investigator
                    session.add(sample)
                else:
                    print('@@ found sample "{}"'.format(sample_query_result.sample_name))

                    # some samples are missing seq_name
                    # now is a good time to fix them
                    if sample_query_result.seq_name is None:
                        sample_query_result.seq_name = sample_file_r1_row.seq_name
                    else:
                        pass

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
                    sample_attrs_with_column_attr_type = [a for a in sample.sample_attr_list if a.sample_attr_type == column_sample_attr_type]
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
                            raise Exception('too many attributes with the same type?')


def cli():
    return main(sys.argv[1:])


if __name__ == '__main__':
    cli()