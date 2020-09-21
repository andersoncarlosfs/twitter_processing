from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import Window
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import split

DEFAULT_LOGS=True
DEFAULT_START=True
DEFAULT_HDFS_PATH='hdfs://192.168.1.22:9000/'
DEFAULT_KAFKA_BROKERS='192.168.1.22:9092, kafka:29092'
DEFAULT_KAFKA_TOPIC='tweets'
DEFAULT_MONGODB_URI='mongodb://127.0.0.1:27017/hashtags.trends'

# https://stackoverflow.com/questions/62125482/how-to-write-spark-structure-stream-into-mongodb-collection
def write(df, epoch): 
    df.write \
        .format('mongo') \
        .mode('append') \
        .save()
    
if __name__ == '__main__':
    #
    parser=ArgumentParser(description='consumer')
    
    parser.add_argument('--start',
                        dest='start',
                        type=bool,
                        help='start the consumer')
    
    parser.add_argument('--logs',
                        dest='logs',
                        type=bool,
                        help='logs of the consumer')    
    
    parser.add_argument('--kafka_brokers',
                        dest='kafka_brokers',
                        nargs='+',
                        help='brokers for Kafka'
                       )
    
    parser.add_argument('--kafka_topic',
                        dest='kafka_topic',
                        type=str,
                        help='topic for Kafka'
                       )  
    
    parser.add_argument('--hdfs_path',
                        dest='hdfs_path',
                        type=str,
                        help='path to HDFS'
                       )

    parser.add_argument('--mongodb_uri',
                        dest='mongodb_uri',
                        type=str,
                        help='uri for connecting to MongoDB'
                       )     
    
    parser.add_argument('--recompute',
                    dest='recompute',
                    type=int,
                    help='recompute from a timestamp'
                   )

    parser.set_defaults(logs=DEFAULT_LOGS,
                        start=DEFAULT_START,
                        kafka_brokers=DEFAULT_KAFKA_BROKERS,
                        kafka_topic=DEFAULT_KAFKA_TOPIC,
                        hdfs_path=DEFAULT_HDFS_PATH,
                        mongodb_uri=DEFAULT_MONGODB_URI,
                        recompute=None
                       )
    
    #
    arguments=parser.parse_args()
    
    # Getting a session
    session=SparkSession \
        .builder \
        .appName('consumer') \
        .config('spark.mongodb.input.uri', arguments.mongodb_uri) \
        .config('spark.mongodb.output.uri', arguments.mongodb_uri) \
        .getOrCreate()
    
    if arguments.recompute:
        pass

    else:
        # Setting log level
        session.sparkContext.setLogLevel('WARN')

        # Getting hashtags from a broker
        df=session \
            .readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', arguments.kafka_brokers) \
            .option('subscribe', arguments.kafka_topic) \
            .load()

        # Casting string to array
        df=df \
        .select(
            from_json(col('value').cast(types.StringType()), 
            # Defining thee JSON Schema
            types \
            .StructType([
                types.StructField('timestamp', types.StringType()),
                types.StructField('hashtags', types.ArrayType(types.StringType()))
            ])
        ).alias('record')) \
        .select('record.*')
        
        # Persisting the hashtags (Avro)
        df.select(
                to_avro('timestamp').alias('timestamp'), # Enconding to Avro
                to_avro('hashtags').alias('hashtags') # Enconding to Avro
            ).writeStream \
            .format('avro') \
            .option('path', arguments.hdfs_path + 'hashtags') \
            .option('checkpointLocation', arguments.hdfs_path + 'checkpoint') \
            .start() 

        # Persisting the trends (MongoDB)
        df.writeStream \
            .foreachBatch(write) \
            .start()         
        
        # Printing the hashtags
        if arguments.logs:        
            df.writeStream \
                .format('console') \
                .start()

        # Running
        # https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#managing-streaming-queries
        session.streams \
            .awaitAnyTermination()