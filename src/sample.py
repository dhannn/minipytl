import os
from data.data_source import *
from data.data_target import MySQLDataTarget
from data.data_transform import DataTransform
from pipeline.pipeline import ETLPipeline
from sample_transforms import flattenMongoDB
from dotenv import load_dotenv

def main():
    load_dotenv()
    
    # initialize etl pipeline
    pipeline = ETLPipeline()

    # extract data from MySQL database
    config = {
        'user': os.getenv('MYSQL_USER_SRC'), 
        'password': os.getenv('MYSQL_PASS_SRC'), 
        'host': os.getenv('MYSQL_HOST_SRC'), 
        'database': os.getenv('MYSQL_DB_SRC')
    }

    mysqlDataSource = MySQLDataSource(config, {
        'go_methods': Schema('order_method_code', ['order_method_code', 'order_method_type']),
        'go_daily_sales': Schema('id', ['id', 'retailer_code', 'product_number', 'order_method_code', 'quantity', 'unit_price', 'unit_sale_price'])
    })

    pipeline.enqueue(mysqlDataSource)

    # extract data from CSV file
    filename = './data/26k-consumer-complaints.csv'
    csvDataSource = CSVDataSource('complaints', filename, 'Complaint ID')
    pipeline.enqueue(csvDataSource)

    # extract data from MongoDB database
    config = {
        'uri': os.getenv('MONGO_URI_SRC'), 
        'database': os.getenv('MONGO_DB_SRC')
    }

    mongoDBDataSource = MongoDBDataSource('s', config, {
        'sales': Schema('_id', ['order_method_code', 'order_method_type'])
    })

    pipeline.enqueue(mongoDBDataSource)

    # transform MongoDB database
    pipeline.enqueue(DataTransform(flattenMongoDB))

    # load staging area to the data warehouse 
    config = {
        'user': os.getenv('MYSQL_USER_DST'), 
        'password': os.getenv('MYSQL_PASS_DST'), 
        'host': os.getenv('MYSQL_HOST_DST'), 
        'database': os.getenv('MYSQL_DB_DST')
    }
    
    mysqlDataTarget = MySQLDataTarget(config)
    pipeline.enqueue(mysqlDataTarget)

    # run pipeline
    pipeline.start()


main()
