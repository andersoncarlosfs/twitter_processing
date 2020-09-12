import argparse

from pyspark.sql import functions
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import Window

DEFAULT_VOLUME='/var/lib/consumer/output'
DEFAULT_APPLICATION_NAME='consumer'
DEFAULT_BROKERS='0.0.0.0:9092, kafka:29092'
DEFAULT_TOPIC='drivers'
DEFAULT_SCHEMA=types.StructType() \
    .add('driver_id', types.IntegerType()) \
    .add('on_duty', types.IntegerType()) \
    .add('timestamp', types.TimestampType())

# Getting a session
def get_session(application_name=DEFAULT_APPLICATION_NAME):
    return SparkSession \
        .builder \
        .appName(application_name) \
        .getOrCreate()
    
# Getting a data from a broker 
def get_data(session=get_session(), brokers=DEFAULT_BROKERS, topic=DEFAULT_TOPIC):
    return session \
        .read \
        .format('kafka') \
        .option('kafka.bootstrap.servers', brokers) \
        .option('subscribe', topic) \
        .load()

# Getting a data frame
def get_data_frame(data, schema=DEFAULT_SCHEMA):
    return data \
        .select(functions.from_json(functions.col('value').cast(types.StringType()), schema).alias('record')) \
        .select('record.*') 

# Getting the default query
def get_default_query(data_frame):
    # Tumbling window to compute aggregations and reshaping the data frame      
    return data_frame \
        .groupBy(functions.window(functions.col('timestamp'), '1 minute')) \
        .pivot('on_duty') \
        .agg(
            functions.countDistinct('driver_id')
        ) \
        .withColumn('Time', functions.to_timestamp('window.start', 'yyyy-MM-dd hh:mm:ss').cast(types.StringType())) \
        .withColumn('Online Drivers', functions.col('0') + functions.col('1')) \
        .withColumnRenamed('0', 'Available Drivers') \
        .select(
            'Time',
            'Online Drivers',
            'Available Drivers'
        ) \
        .orderBy('Time')
    
if __name__ == '__main__':
    # Getting a session
    spark=get_session()

    # Getting a data from a broker
    df=get_data(session=spark)
        
    # Parsing value to JSON
    df=get_data_frame(df)

    # Persisting the data_frame
    query=get_default_query(df) \
        .coalesce(1) \
        .write \
        .format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .option('header', 'true') \
        .option('sep', '\t') \
        .option('encoding', 'UTF-8') \
        .save(DEFAULT_VOLUME)