"""
This script reads water column spreadsheets such as

  HL3_watercolumn_current.xls
  HL4_watercolumn_current.xls

for data recorded at each station from the rosette.

Then the data are applied as attributes to all samples in the muscope database.

"""
import os
import sys

from irods.keywords import FORCE_FLAG_KW
from irods.session import iRODSSession

import numpy as np

import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from orminator import session_manager
import muscope_loader
import muscope_loader.models as models


def main(argv):
    for water_column_df in get_all_water_column_spreadsheets():
        cruise_name = water_column_df.cruise_name[0]
        print(water_column_df.head())
        # calculate expected depth based on pressure for all rosette positions
        # this calculation is found at http://www.seabird.com/document/an69-conversion-pressure-depth
        x = np.power(np.sin(water_column_df.latitude / 57.29578), 2.0)
        p = water_column_df.pressure
        g = 9.780318 * (1.0 + (5.2788e-3 + 2.36e-5 * x) * x) + 1.092e-6 * p
        water_column_df['predicted_bottle_depth'] = ((((-1.82e-15 * p + 2.279e-10) * p - 2.2512e-5) * p + 9.72659) * p) / g

        # rearrange columns so predicted_bottle_depth is to the left of pressure
        # pressure is the first attribute column and all columns to the right of it will be treated as attributes
        columns = list(water_column_df.columns.values)
        columns.remove('predicted_bottle_depth')
        columns.insert(columns.index('pressure'), 'predicted_bottle_depth')
        print(columns[:10])
        water_column_df = water_column_df[columns]
        pressure_column_index = water_column_df.columns.get_loc('pressure')

        # get the stations
        for station in water_column_df.station.unique():
            print('processing cruise "{}" station "{}"'.format(water_column_df.cruise_name[0], station))
            # get all rows for this station
            station_water_column_df = water_column_df[water_column_df.station == station]

            # look up all samples with this cruise and station
            # connect to database on server
            # e.g. mysql+pymysql://imicrobe:<password>@localhost/muscope2
            Session_class = sessionmaker(
                bind=create_engine(
                    os.environ.get('MUSCOPE_DB_URI'), echo=False))
            with session_manager(Session_class) as session:
                samples_for_cruise_and_station_query = session.query(models.Sample).join(models.Cruise).filter(
                    models.Cruise.cruise_name == station_water_column_df.cruise_name.iloc[0]).filter(
                    models.Sample.station_number == int(station_water_column_df.station.iloc[0]))

                samples_for_cruise_and_station_count = samples_for_cruise_and_station_query.count()

                print('found {} samples for cruise "{}" and station {}'.format(
                    samples_for_cruise_and_station_count,
                    station_water_column_df.cruise_name.iloc[0],
                    station_water_column_df.station.iloc[0]))

                for sample in samples_for_cruise_and_station_query.all():

                    print('processing sample "{}" with\n\tlat: {:8.5f}\t{:8.5f} (station)\n\tlong: {:8.5f}\t{:8.5f} (station)'.format(
                        sample.sample_name,
                        sample.latitude_start if sample.latitude_start is not None else float('nan'),
                        station_water_column_df.latitude.iloc[0],
                        sample.longitude_start if sample.longitude_start is not None else float('nan'),
                        station_water_column_df.longitude.iloc[0]))

                    if sample.latitude_start is None:
                        print('\tupdating latitude to "{:8.5f}"'.format(station_water_column_df.latitude.iloc[0]))
                        sample.latitude_start = str(station_water_column_df.latitude.iloc[0])
                    else:
                        pass

                    if sample.longitude_start is None:
                        station_longitude = -1.0 * station_water_column_df.longitude.iloc[0]
                        print('\tupdating longitude to "{:8.5f}"'.format(station_longitude))
                        sample.longitude_start = str(station_longitude)
                    else:
                        pass

                    # which rosette position has predicted_depth closest to the sample depth?
                    depth_difference = np.abs(sample.depth - station_water_column_df.predicted_bottle_depth)
                    best_bottle_index = depth_difference.values.argmin()
                    print('closest bottle to sample "{}" at depth {:5.2f}m is bottle {} with predicted depth {:5.2f}m ({:5.2f} dbar)\n\tdifference is {:5.2f}m'.format(
                        sample.sample_name,
                        sample.depth,
                        station_water_column_df.rosette_position.iloc[best_bottle_index],
                        station_water_column_df.predicted_bottle_depth.iloc[best_bottle_index],
                        station_water_column_df.pressure.iloc[best_bottle_index],
                        depth_difference.iloc[best_bottle_index]))

                    if depth_difference.iloc[best_bottle_index] >= 1.0:
                        print('  {} {} station {} sample {} large depth difference: {:5.2f}'.format(
                            ','.join([i.last_name for i in sample.investigator_list]),
                            cruise_name,
                            station,
                            sample.sample_name,
                            depth_difference.iloc[best_bottle_index]))

                    best_ctd_row = station_water_column_df.iloc[best_bottle_index, :]

                    # remove columns to the left of pressure - they are not attributes
                    # remove columns with value -9.0 - they are 'missing'
                    best_ctd_attributes = best_ctd_row[pressure_column_index:][best_ctd_row[pressure_column_index:] != -9.0]

                    # build a table of sample attribute type to sample attribute value
                    #  'pressure': 50.0
                    #  'temperature_CTD': 3.0
                    #  ...
                    sample_attribute_table = {
                        a.sample_attr_type.type_: a
                        for a
                        in sample.sample_attr_list}
                    print(sample_attribute_table)
                    # columns to the right of pressure inclusive are attributes
                    # check all attributes for this sample
                    for column_name, column_value in best_ctd_attributes.iteritems():

                        sample_attr = sample_attribute_table.get(column_name.strip().lower(), None)
                        if sample_attr is None:
                            print('  ## sample "{}" does not have attribute "{}"'.format(sample.sample_name, column_name))
                            print('     adding new attribute with type "{}" and value "{}"'.format(column_name, column_value))
                            sample_attr_type = session.query(models.Sample_attr_type).filter(
                                models.Sample_attr_type.type_ == column_name).one()
                            print('     found attribute type "{}"'.format(sample_attr_type.type_))
                            new_sample_attribute = models.Sample_attr(value=str(column_value))
                            new_sample_attribute.sample_attr_type = sample_attr_type
                            sample.sample_attr_list.append(new_sample_attribute)
                        else:
                            # update it
                            print('  @@ sample "{}" has attribute "{}" with value "{}"'.format(
                                sample.sample_name,
                                sample_attr.sample_attr_type.type_,
                                sample_attr.value))
                            print('     updating value to "{}"'.format(column_value))
                            sample_attr.value = str(column_value)


def get_all_water_column_spreadsheets():
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
                skiprows=(0, 2))

            # normalize column names that vary across spreadsheets
            watercolumn_df.rename(
                columns={
                    'Pressure': 'pressure',
                    'Cruise': 'cruise_name'},
                inplace=True)

            yield watercolumn_df


def cli():
    return main(sys.argv[1:])


if __name__ == '__main__':
    cli()
