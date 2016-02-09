import pandas
from sqlalchemy import create_engine # database connection
from sqlalchemy.engine import reflection
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import progressbar

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

def get_table(variable, disk_engine):
  print stuff

def flip_dict(dict):
  inv_dict = {}
  for k, v in dict.iteritems():
    inv_dict[v] = inv_dict.get(v, [])
    inv_dict[v].append(k)  
  return inv_dict

disk_engine = create_engine('sqlite:///../data/2011_BCP_ALL_for_AUST_long-header.db') # Initializes database

column_to_table_dict = get_column_to_table_lookup_dict(disk_engine)

print get_variables_to_read_per_table(['Total_Persons'], column_to_table_dict)