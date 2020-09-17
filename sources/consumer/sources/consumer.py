from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import Window
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import split

DEFAULT_APPLICATION_NAME='consumer'
DEFAULT_HDFS_PATH='hdfs://192.168.1.22:9000/'
DEFAULT_KAFKA_BROKERS='192.168.1.22:9092, kafka:29092'
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

    # Casting string to array
    df=df \
    .selectExpr('CAST(value AS STRING)') \
    .withColumn(
        'value', # column
        split(
            regexp_replace(
                col('value'), # column
                r'(^\[)|(\]$)|(\')', # regex
                ''
            ), 
            ', '
        )
    )
    
    # Enconding to Avro
    df=df \
    .select(
        to_avro('value').alias('value')
    )
    
    # Persisting the hashtags (Avro)
    df.writeStream \
        .format('avro') \
        .option('path', hdfs_path + 'hashtags') \
        .option('checkpointLocation', hdfs_path + 'checkpoint') \
        .start() 
    
    # Printing the hashtags
    df.writeStream \
        .format('console') \
        .start()
    
    # Running
    # https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#managing-streaming-queries
    session.streams \
        .awaitAnyTermination()
    