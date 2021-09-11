from sqlite3.dbapi2 import Cursor
from sqlalchemy import create_engine
import pandas as pd
import os
import configparser

config = configparser.RawConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config.ini'))



def aws_database_connection():
    """Returns the AWS engine"""
    try:
        print('Connecting to AWS')
        conn = "postgresql+psycopg2://%s:%s@%s:5432/%s" % ('postgres',config['DEFAULT']['password'],config['DEFAULT']['aws_database_url'],'postgres')
        engine = create_engine(conn)
        print('Connected to AWS')
        return engine
    except Exception as e:
        print (e)



def aws_create_and_insert(dataframe, schema, table_name, if_exists):
    """If it does not exist creates a schema and table on the aws server... if exists is either 'append' or 'replace' """
    connection = aws_database_connection()
    cursor = connection.connect()

    try:
        cursor.execute(('CREATE SCHEMA IF NOT EXISTS {schema_name}').format(schema_name = schema.lower()))
        print(('Schema {schema_name} created or updated').format(schema_name = schema.lower()))
    except Exception as e:
        print(e)

    try:
        pd.io.sql.get_schema(dataframe.reset_index(), schema.lower()+'.'+table_name.lower())
        print('Table '+table_name+' created')
    except Exception as e:
            print(e)
            pass

    try:
        print('Inserting data into '+schema+'.'+table_name)
        dataframe.to_sql(schema=schema.lower(),name=table_name.lower(),con=connection, if_exists=if_exists.lower())
        print('Inserted data into '+schema+'.'+table_name)
    except Exception as e:
            print(e)
    cursor.close()


def aws_insert(dataframe, schema, table_name, if_exists):
    """Simply inserts into existing aws schema/table... if exists is either 'append' or 'replace' """
    try:
        dataframe.to_sql(schema=schema.lower() ,name=table_name.lower(),con=aws_database_connection(), if_exists=if_exists.lower())
        print('Inserted data into '+schema+'.'+table_name)
    except Exception as e:
            print(e)


def aws_to_df(sql):
    """Takes a SQL string, excutes it and returns a pandas dataframe"""
    try:
        engine = aws_database_connection()
        df = pd.read_sql_query(sql, engine)
        return df
    except Exception as e:
        print (e)

def aws_execute_sql(sql):
    """Takes a SQL string, excutes it with not return of data (good for drops,creates, or other simple statements)"""
    cursor = aws_database_connection().connect()
    try:
        print('Executing '+sql )
        cursor.execute(sql)
        print(sql + 'excuted')
    except Exception as e:
        print(e)
