import pandas
from sqlalchemy import create_engine # database connection
from sqlalchemy.engine import reflection
from sqlalchemy import Table, Column, Integer, String, MetaData
import progressbar

variables = [] # list of abs variables for constraintss
geo_level = 'SA3' # string of geo level to fit at
geo_ids = 'all' # list of geoids to fit at or 'all'

# look up which table each variable is in
def lookup_tables_for_variables(variables):
  for variable in variables:
    table = get_table(variable)

def get_table(variable):
  print 'stuff'



# get column for 