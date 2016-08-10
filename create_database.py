import pandas
import os
from sqlalchemy import create_engine # database connection
from sqlalchemy.engine import reflection
from sqlalchemy import Table, Column, Integer, String, MetaData
import progressbar
import argparse
import re

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
  geo_levels_to_read = ['sa1']
  read_data_for_geo_level_into_database(directory, geo_levels_to_read, disk_engine)

def get_table_names_from_database(disk_engine):
  insp = reflection.Inspector.from_engine(disk_engine)
  return insp.get_table_names()

def get_column_names_from_table(disk_engine, table_name):
  insp = reflection.Inspector.from_engine(disk_engine)
  columns = insp.get_columns(table_name)
  return map(lambda column: column['name'], columns)

def get_column_list_table_name_array(table_name, column_list):
  # return a list of [column_name, table_name] lists
  return map(lambda column_name: [column_name, table_name], column_list)

def read_data_for_geo_level_into_database(directory, geo_levels, disk_engine):
  table_names = get_table_names_from_database(disk_engine)

  table_columns_df = pandas.DataFrame()

  for geo_level in geo_levels:
    # print 'Building database for ' + geo_level
    data_directory = directory+'/'+geo_level+'/AUST/'
    directory_file_list = os.listdir(data_directory)

    bar = progressbar.ProgressBar(maxval=len(directory_file_list), \
      widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])

    bar.start()
    for i, filename in enumerate(directory_file_list):
      if filename == '.DS_Store':
       continue
      bar.update(i)
      reg = r'([A-Z]+)(\d+)([A-Z]*)'
      # print filename
      match = re.search(reg, filename).group(0)
      table_name = (geo_level + '_' + match)
      if table_name not in table_names:
        df = pandas.DataFrame.from_csv(data_directory + filename, index_col='region_id')
        df.to_sql(table_name, disk_engine, if_exists='fail')
        column_list = list(df)
      else:
        column_list = get_column_names_from_table(disk_engine, table_name)

      table_columns_df = table_columns_df.append(pandas.DataFrame(get_column_list_table_name_array(table_name, column_list), columns = ['column', 'table_name']))
    bar.finish()

    table_columns_df.set_index('column').to_sql('metadata', disk_engine, if_exists='replace')

def update_metadata(disk_engine):
  table_names = get_table_names_from_database(disk_engine)
  bar = progressbar.ProgressBar(maxval=len(table_names), \
    widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])

  table_columns_df = pandas.DataFrame(columns=['table_name', 'column'])
  print 'Updating metadata from exisiting tables'
  bar.start()
  for i, table_name in enumerate(table_names):
    bar.update(i)
    column_list = get_column_names_from_table(disk_engine, table_name)
    table_columns_df = table_columns_df.append(pandas.DataFrame(get_column_list_table_name_array(table_name, column_list), columns = ['column', 'table_name']))
  bar.finish()

  table_columns_df.set_index('column').to_sql('metadata', disk_engine, if_exists='replace')

if __name__ == '__main__':
  main()