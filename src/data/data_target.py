import logging

import sqlalchemy
from data.data_source import Schema
from pipeline.pipeline import ETLComponent
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine


class DataTarget(ETLComponent):
    staging_area = None

    def run(self, staging_area: dict[str, pd.DataFrame]):
        self.staging_area = staging_area
        self.write()

    def write(self):
        raise NotImplementedError(
            '`DataTarget` subclasses should implement the write() method')


class MySQLDataTarget(DataTarget):

    connection = None
    tables = None
    database_name = None

    def __init__(self, connConfig: dict):

        self.connection = mysql.connector.connect(
            user=connConfig['user'], 
            password=connConfig['password'], 
            host=connConfig['host'], 
            database=connConfig['database'])
        user = connConfig["user"]
        password = connConfig["password"]
        host = connConfig["host"]
        database = connConfig["database"]

        self.database_name = database
        self.connection = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
    
    def write(self):
        tables = self.staging_area

        for table_name in tables:
            table: pd.DataFrame = tables[table_name]
            table.reset_index()
            logging.info(f'Writing { table_name } to the database { self.database_name }')
            
            table.to_sql(table_name, self.connection, if_exists='replace', dtype=sqlcol(table), index=False)


def sqlcol(dfparam):    
    
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})
                                 
        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})

    return dtypedict

