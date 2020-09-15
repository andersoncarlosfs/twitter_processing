from pyspark.sql import functions
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import Window

DEFAULT_APPLICATION_NAME='consumer'
DEFAULT_HDFS_PATH='hdfs://localhost:9000/'
DEFAULT_KAFKA_BROKERS='localhost:9092, kafka:29092'
DEFAULT_KAFKA_TOPIC='tweets'  
DEFAULT_SCHEMA='consumer'
    
if __name__ == '__main__':
    application_name=DEFAULT_APPLICATION_NAME
    hdfs_path=DEFAULT_HDFS_PATH    
    kafka_brokers=DEFAULT_KAFKA_BROKERS
    kafka_topic=DEFAULT_KAFKA_TOPIC

    # Getting a session
    session=SparkSession \
        .builder \
        .appName(application_name) \
        .getOrCreate()
    
    # Setting log level
    session.sparkContext.setLogLevel('WARN')

    # Getting hashtags from a broker
    df=session \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', kafka_brokers) \
        .option('subscribe', kafka_topic) \
        .load()

    # Persisting the hashtags
    df=df.selectExpr('CAST(value AS STRING)')
    
    
    df.writeStream \
        .format('csv') \
        .option('path', hdfs_path + 'hashtags') \
        .option('checkpointLocation', hdfs_path + 'checkpoint') \
        .start() 
    
    # Printing the hashtags
    df.withColumn(
        'value', # column
        functions.split(
            functions.regexp_replace(
                functions.col('value'), # column
                r'(^\[)|(\]$)|(\')', # regex
                ''
            ), 
            ', '
        )
    ).writeStream \
        .format('csv') \
        .option('path', hdfs_path + 'hashtags') \
        .option('checkpointLocation', hdfs_path + 'checkpoint') \
        .start()
    
    # Running
    # https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#managing-streaming-queries
    session.streams \
        .awaitAnyTermination()
    