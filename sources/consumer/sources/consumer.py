from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import Window
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import split

DEFAULT_APPLICATION_NAME='consumer'
DEFAULT_HDFS_PATH='hdfs://192.168.1.22:9000/'
DEFAULT_KAFKA_BROKERS='192.168.1.22:9092, kafka:29092'
DEFAULT_KAFKA_TOPIC='tweets'  
DEFAULT_KAFKA_SCHEMA=types \
    .StructType() \
    .add(
        types.ArrayType(
            types.StructType() \
            .add('timestamp', types.TimestampType()) \
            .add('hashtags', types.ArrayType(types.StringType()))
        )
    )
    
if __name__ == '__main__':
    application_name=DEFAULT_APPLICATION_NAME
    hdfs_path=DEFAULT_HDFS_PATH    
    kafka_brokers=DEFAULT_KAFKA_BROKERS
    kafka_topic=DEFAULT_KAFKA_TOPIC
    kafka_schema=DEFAULT_KAFKA_SCHEMA

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
    .select(from_json(col('value').cast(types.StringType()), kafka_schema).alias('record')) \
    .select('record.*')
    
    # Enconding to Avro
    df=df \
    .select(
        to_avro('timestamp').alias('timestamp'),
        to_avro('hashtags').alias('hashtags')
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