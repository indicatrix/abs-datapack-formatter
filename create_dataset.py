import pandas
from sqlalchemy import create_engine # database connection
from sqlalchemy.engine import reflection
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import progressbar
from sqlalchemy import select

variables = [] # list of abs variables for constraintss
geo_level = 'SA3' # string of geo level to fit at
geo_ids = 'all' # list of geoids to fit at or 'all'


Base = declarative_base()

class ABSMetaData(Base):
  __tablename__ = 'metadata'
  column = Column(String(250), primary_key=True)
  table_name = Column(String(250), nullable=False)

# return hash of {table_name: variables_in_table}
def get_variables_to_read_per_table(variables, column_to_table_dict):
  variable_to_table_dict = {variable: column_to_table_dict[variable] for variable in variables}
  return flip_dict(variable_to_table_dict)

# get a lookup dict for variables to tables
def get_column_to_table_lookup_dict(disk_engine):
  Base.metadata.bind = disk_engine
  DBSession = sessionmaker()
  DBSession.bind = disk_engine
  session = DBSession()

  column_to_table_dict = {}

  metadata_table_rows = session.query(ABSMetaData)
  for row in metadata_table_rows:
    column_to_table_dict[row.column] = row.table_name

  return column_to_table_dict

# flip keys and values in a dictionary, keys for duplicate values in list
def flip_dict(dict):
  inv_dict = {}
  for k, v in dict.iteritems():
    inv_dict[v] = inv_dict.get(v, [])
    inv_dict[v].append(k)  
  return inv_dict

def get_sql_query_for_table(table, variables):
  for i, variable in enumerate(variables):
    if i == 0:
      select_string = "{0}.{1}".format(table, variable)
    else:
      select_string += ", {0}.{1}".format(table, variable)
  sql_string = "SELECT {0} FROM {1}".format(select_string, table)    
  return sql_string

def read_from_database(tables_to_variables_dict, disk_engine):
  connection = disk_engine.connect()
  result_df = pandas.read_sql("SELECT {0}.region_id FROM {0}".format(tables_to_variables_dict.keys()[0]), connection)
  for table, variables in tables_to_variables_dict.iteritems():
    sql = get_sql_query_for_table(table, variables)
    result_df = pandas.concat([pandas.read_sql(sql, connection), result_df], axis=1)  
  return result_df.set_index('region_id')

disk_engine = create_engine('sqlite:///../data/2011_BCP_ALL_for_AUST_long-header.db') # Initializes database
variables = ['Total_Persons', 'Total_Persons_Males', 'Total_Persons_Females', 'Persons_55_64_years_Widowed', 'Persons_Total_Total']

column_to_table_dict = get_column_to_table_lookup_dict(disk_engine)
tables_to_variables_dict = get_variables_to_read_per_table(variables, column_to_table_dict)

# print "SELECT {0}.region_id FROM {0}".format(tables_to_variables_dict.keys()[0])

print read_from_database(tables_to_variables_dict, disk_engine)