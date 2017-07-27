import pandas
import os
from sqlalchemy import create_engine # database connection
from sqlalchemy.engine import reflection
from sqlalchemy import Table, Column, Integer, String, MetaData
import progressbar
import argparse
import re
import xlrd

def main():
  parser = argparse.ArgumentParser(description="Create a dataset from an existing database of tablebuilder data.")
  parser.add_argument('data_directory')
  parser.add_argument('database')
  args = parser.parse_args()
  # '../data/2011_BCP_ALL_for_AUST_long-header/2011 Census BCP All Geographies for AUST/'
  directory = args.data_directory 
  # 'sqlite:///../data/2011_BCP_ALL_for_AUST_long-header.db'
  connection_string = 'sqlite:///'+args.database
  disk_engine = create_engine(connection_string) # Initializes database
  geo_levels_to_read = ['sa3', 'sa4']
  read_data_for_geo_level_into_database(directory, geo_levels_to_read, disk_engine)
  update_metadata(directory, disk_engine)

def get_table_names_from_database(disk_engine):
  insp = reflection.Inspector.from_engine(disk_engine)
  return insp.get_table_names()

def get_column_names_from_table(disk_engine, table_name):
  insp = reflection.Inspector.from_engine(disk_engine)
  columns = insp.get_columns(table_name)
  return map(lambda column: column['name'], columns)

def get_column_list_table_name_array(table_name, column_list, long_header_lookup):
  # return a list of [column_name, table_name] lists
  return map(lambda column_name: [long_header_lookup.get_value(column_name, 'Long'), column_name, table_name], column_list)

def read_data_for_geo_level_into_database(directory, geo_levels, disk_engine):
  table_names = get_table_names_from_database(disk_engine)

  table_columns_df = pandas.DataFrame()

  for geo_level in geo_levels:
    # print 'Building database for ' + geo_level
    data_directory = directory+'/'+geo_level+'/AUST/'
    directory_file_list = os.listdir(data_directory)

    bar = progressbar.ProgressBar(maxval=len(directory_file_list), \
      widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])

    long_header_lookup = pandas.read_excel(directory+'/../Metadata/Metadata_2016_GCP_DataPack.xls', sheetname='Cell descriptors information', header=10, index_col=1)
    long_header_lookup = long_header_lookup[~long_header_lookup.index.duplicated(keep='first')]

    bar.start()
    for i, filename in enumerate(directory_file_list):
      if filename == '.DS_Store':
       continue
      bar.update(i)
      reg = r'([A-Z]+)(\d+)([A-Z]*)'
      table_name = re.search(reg, filename).group(0)
      table_name_with_geo_level = (geo_level + '_' + table_name)
      if table_name_with_geo_level not in table_names:
        df = pandas.DataFrame.from_csv(data_directory + filename)
        df.to_sql(table_name_with_geo_level, disk_engine, if_exists='fail')
        column_list = list(df)
      else:
        column_list = get_column_names_from_table(disk_engine, table_name_with_geo_level)

      # table_columns_df = table_columns_df.append(pandas.DataFrame(get_column_list_table_name_array(table_name, column_list[1:], long_header_lookup), columns = ['long', 'short', 'table_name']))
    bar.finish()

def update_metadata(directory, disk_engine):
  table_names = get_table_names_from_database(disk_engine)
  
  bar = progressbar.ProgressBar(maxval=len(table_names), \
    widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])

  long_header_lookup = pandas.read_excel(directory+'/../Metadata/Metadata_2016_GCP_DataPack.xls', sheetname='Cell descriptors information', header=10, index_col=1)
  long_header_lookup = long_header_lookup[~long_header_lookup.index.duplicated(keep='first')]

  connection = disk_engine.connect()
  table_columns_df = pandas.read_sql("SELECT * FROM metadata", connection)
  
  metadata_table_names = table_columns_df['table_name']
  print metadata_table_names
  print 'Updating metadata from exisiting tables'
  bar.start()
  for i, table_name_with_geo_level in enumerate(table_names):
    bar.update(i)
    if (table_name_with_geo_level != "metadata"):
      table_name = table_name_with_geo_level.split('_')[1]
      if (table_name not in metadata_table_names):
        column_list = get_column_names_from_table(disk_engine, table_name)[1:-1]
        print column_list
        table_columns_df = table_columns_df.append(pandas.DataFrame(get_column_list_table_name_array(table_name, column_list, long_header_lookup), columns = ['long', 'short', 'table_name']))
  bar.finish()

  table_columns_df.set_index('long').to_sql('metadata', disk_engine, if_exists='replace')

if __name__ == '__main__':
  main()
