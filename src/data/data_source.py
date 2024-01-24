import logging
from pymongo import MongoClient
from pipeline.pipeline import ETLComponent
import mysql.connector
import pandas as pd

class DataSource(ETLComponent):
    """Base class for data sources."""
    def run(self, staging_area: dict[str, pd.DataFrame]):
        """Run the data source.
        
        Args:
            staging_area (dict): The staging area containing DataFrames.
        """
        result = self.read()
        if type(result) == dict:
            for table_name in result.keys():
                staging_area[table_name] = result[table_name]
        else:
            staging_area[result[0]] = result[1]


    def read(self) -> tuple[str, pd.DataFrame] | dict[str, pd.DataFrame]:
        """Abstract method to be implemented by subclasses.
        
        Returns:
            tuple[str, pd.DataFrame] | dict[str, pd.DataFrame]: Data read from the source.
        """
        raise NotImplementedError(
            '`DataSource` subclasses should implement the read() method')

class CSVDataSource(DataSource):

    filename = None
    id_col = None
    table_name = None

    def __init__(self, table_name, filename, id_col):
        self.id_col = id_col
        self.filename = filename
        self.table_name = table_name

    def read(self) -> pd.DataFrame:
        csv = pd.read_csv(self.filename, index_col=self.id_col)
        logging.info(f'Read { csv.shape[0] } rows from { self.filename }')
        return (self.table_name, csv)
    
class Schema:
    id_col = None
    cols: None | tuple[str] = None

    def __init__(self, id_col: str | int, cols: tuple[str]):
        if type(id_col) == 'int':
            self.id_col = cols[id_col]
        else:
            self.id_col = id_col
        
        self.cols = cols

class MySQLDataSource(DataSource):

    database = None
    connection = None
    tables = None

    def __init__(self, connConfig: dict, tables: dict[str, Schema]):

        self.connection = mysql.connector.connect(
            user=connConfig['user'], 
            password=connConfig['password'], 
            host=connConfig['host'], 
            database=connConfig['database'])
        self.tables = tables
        self.database = connConfig['database']
    
    def read(self) -> dict[str, pd.DataFrame]:
        dfs: dict[str, pd.DataFrame] = {}
        schema: Schema
        for table_name in self.tables:
            cursor = self.connection.cursor(dictionary=True)

            schema = self.tables[table_name]
            cols = schema.cols
            strCols = ','.join(cols)

            query = (f'SELECT { strCols } FROM { table_name }')
            cursor.execute(query)

            results = cursor.fetchall()
            logging.info(f'Reading {len(results)} rows from the { table_name } table of { self.database }')
            
            df = pd.DataFrame(results)
            df.set_index(schema.id_col, inplace=True)
            dfs[table_name] = df
        
        return dfs

class MongoDBDataSource(DataSource):

    connection = None
    collections = None
    database = None

    def __init__(
            self, 
            source_id: str, 
            connConfig: dict,
            collections: dict[str, Schema]
            ):
        self.source_id = source_id

        self.connection = MongoClient(connConfig['uri'])
        self.database = connConfig['database']
        self.collections = collections
    
    def read(self) -> dict[str, pd.DataFrame]:
        db = self.connection[self.database]

        dfs: dict[str, pd.DataFrame] = {}
        schema: Schema
        for collection in self.collections:
            schema = self.collections[collection]
            results = db[collection].find()
            
            df = pd.DataFrame(list(results))

            df[schema.id_col] = df[schema.id_col].apply(lambda x: str(x))
            
            df.set_index(schema.id_col, inplace=True)
            dfs[collection] = df
        
        return dfs
