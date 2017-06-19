import pandas
from sqlalchemy import create_engine  # database connection
from sqlalchemy.engine import reflection
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
import re
from sqlalchemy import select
from collections import defaultdict
import yaml

import sys
import getopt
import argparse

Base = declarative_base()


def main():
    parser = argparse.ArgumentParser(
        description="Create a dataset from an existing database of tablebuilder data.")
    parser.add_argument('database')
    parser.add_argument('variables')
    parser.add_argument('geo_level')
    parser.add_argument(
        'output', help='file path or database connection string')
    parser.add_argument('--output-type', type=str,
                        default='csv', choices=['csv', 'postgres'])
    parser.add_argument('--table-name', type=str)
    args = parser.parse_args()

    if args.output_type == 'postgres' and args.table_name is None:
        raise RuntimeError(
            'Must provide --table-name when --output-type is Postgres')

    writers = {
        'csv': lambda dt: dt.to_csv(args.output),
        'postgres': lambda dt: dt.to_sql(name=args.table_name, con=create_engine(args.output))
    }
    if args.output_type not in writers:
        raise RuntimeError(args.output_type +
                           ' output type does not have a writer')

    # 'sqlite:///../data/2011_BCP_ALL_for_AUST_long-header.db'
    connection_string = 'sqlite:///' + args.database

    variables = get_variables(args.variables)
    geo_level = args.geo_level

    disk_engine = create_engine(connection_string)  # Initializes database

    column_to_table_dict = get_column_to_table_lookup_dict(disk_engine)
    tables_to_variables_dict = get_variables_to_read_per_table(
        variables, geo_level, column_to_table_dict)

    dataset_df = read_from_database(tables_to_variables_dict, disk_engine).rename(
        columns={'region_id': 'GeographyId'}).set_index('GeographyId')
    dataset_df['GeographyType'] = geo_level

    writers[args.output_type](dataset_df)


class ABSMetaData(Base):
    __tablename__ = 'metadata'
    column = Column(String(250), primary_key=True)
    table_name = Column(String(250), primary_key=True)

# return hash of {table_name: variables_in_table}


def get_variables_to_read_per_table(variables, geometry_level,  column_to_table_dict):
    dict_file = open('./dict.thing', 'w+')
    print >> dict_file, column_to_table_dict
    if geometry_level not in column_to_table_dict:
        raise KeyError(geometry_level + ' does not exist in data pack')
    geo_hash = column_to_table_dict[geometry_level]

    variable_to_table_dict = {}
    for variable in variables:
        if variable not in geo_hash:
            raise KeyError(variable + ' not found in data pack')
        variable_to_table_dict[variable] = geometry_level + \
            '_' + geo_hash[variable]

    return flip_dict(variable_to_table_dict)

# get a lookup dict for variables to tables


def get_column_to_table_lookup_dict(disk_engine):
    Base.metadata.bind = disk_engine
    DBSession = sessionmaker()
    DBSession.bind = disk_engine
    session = DBSession()

    column_to_table_dict = defaultdict(lambda: defaultdict(str))

    metadata_table_rows = session.query(ABSMetaData)
    for row in metadata_table_rows:
        match = re.search(r'(\w{3})_(\w+)', row.table_name)
        column_to_table_dict[match.group(1)][row.column] = match.group(2)
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
    result_df = pandas.read_sql("SELECT {0}.region_id FROM {0}".format(
        tables_to_variables_dict.keys()[0]), connection)
    for table, variables in tables_to_variables_dict.iteritems():
        sql = get_sql_query_for_table(table, variables)
        result_df = pandas.concat(
            [pandas.read_sql(sql, connection), result_df], axis=1)
    return result_df


def import_table_builder_outputs(data_directory):
    directory_file_list = os.listdir(data_directory)
    if len(directory_file_list) > 0:
        for i, filename in enumerate(directory_file_list):
            if i == 0:
                df = pandas.DataFrame.from_csv(
                    data_directory + filename, index_col='region_id')
            else:
                new_df = pandas.DataFrame.from_csv(
                    data_directory + filename, index_col='region_id')
                df = pandas.concat([df, new_df], axis=1)
        return df
    else:
        return None


def get_variables(filename):
    variables = [line.rstrip('\n') for line in open(filename)]
    return variables


def combine_variables(combined_variable_name, variables_to_combine, dataframe):
    # Take a list of variables and a dataframe and return a new dataframe
    # those variables combined
    dataframe[combined_variable_name] = dataframe[variables_to_combine].sum(
        axis=1)
    return dataframe


if __name__ == "__main__":
    main()
