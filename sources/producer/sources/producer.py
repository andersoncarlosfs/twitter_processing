#!flask/bin/python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from json import dumps
from kafka import KafkaProducer
from twitter import OAuth
from twitter import TwitterStream

DEFAULT_LOGS=True
DEFAULT_START=True
DEFAULT_TWITTER_AUTHENTICATION_KEY='1286068996777873415-k0l67h05lWTXEx8ksvc9bxUOLtSD0m'
DEFAULT_TWITTER_AUTHENTICATION_SECRET='zXyUtwUtWcQm5LEkgzIp0iE29taFwQfpMAMt0yeeVQBLU'
DEFAULT_TWITTER_CONSUMER_KEY='Cv0PLE2GHm4BF1Kk4wgqCkDok'
DEFAULT_TWITTER_CONSUMER_SECRET='ASzIep1DHNGuzQoLcBOVedO1Z5yB55KiEVxO9NruFqc1jVeUrN'
DEFAULT_TWEET_LANGUAGE='en'
DEFAULT_KAFKA_BROKERS=['0.0.0.0:9092', 'kafka:29092']
DEFAULT_KAFKA_RETRIES=0
DEFAULT_KAFKA_TOPIC='tweets'   

if __name__ == '__main__':
    #
    parser=ArgumentParser(description='producer')
    
    parser.add_argument('--start',
                        dest='start',
                        type=bool,
                        help='start the producer')
    
    parser.add_argument('--logs',
                        dest='logs',
                        type=bool,
                        help='logs of the producer')
    
    parser.add_argument('--twitter_authentication_key',
                        dest='twitter_authentication_key',
                        type=str,
                        help='authentication key for Twitter'
                       )      
    
    parser.add_argument('--twitter_authentication_secret',
                        dest='twitter_authentication_secret',
                        type=str,
                        help='authentication key secret for Twitter'
                       )      
    
    parser.add_argument('--twitter_consumer_key',
                        dest='twitter_consumer_key',
                        type=str,
                        help='consumer key for Twitter'
                       )      
    
    parser.add_argument('--twitter_consumer_secret',
                        dest='twitter_consumer_secret',
                        type=str,
                        help='consumer key secret for Twitter'
                       )      
    
    parser.add_argument('--tweet_language',
                        dest='tweet_language',
                        type=str,
                        help='language of the tweets'
                       )      
    
    parser.add_argument('--kafka_brokers',
                        dest='kafka_brokers',
                        nargs='+',
                        help='brokers for Kafka'
                       )
    
    parser.add_argument('--kafka_retries',
                        dest='kafka_retries',
                        type=int,
                        help='retries for Kafka'
                       )
    
    parser.add_argument('--kafka_topic',
                        dest='kafka_topic',
                        type=str,
                        help='topic for Kafka'
                       )  

    parser.set_defaults(logs=DEFAULT_LOGS,
                        start=DEFAULT_START, 
                        twitter_authentication_key=DEFAULT_TWITTER_AUTHENTICATION_KEY, 
                        twitter_authentication_scret=DEFAULT_TWITTER_AUTHENTICATION_SECRET, 
                        twitter_consumer_key=DEFAULT_TWITTER_CONSUMER_KEY, 
                        twitter_consumer_scret=DEFAULT_TWITTER_CONSUMER_SECRET,                        
                        tweet_language=DEFAULT_TWEET_LANGUAGE,
                        kafka_brokers=DEFAULT_KAFKA_BROKERS,
                        kafka_retries=DEFAULT_KAFKA_RETRIES,
                        kafka_topic=DEFAULT_KAFKA_TOPIC
                       )
    
    #
    arguments=parser.parse_args()
         
    #
    twitter=TwitterStream(auth=OAuth(
        arguments.twitter_authentication_key, 
        arguments.twitter_authentication_scret, 
        arguments.twitter_consumer_key, 
        arguments.twitter_consumer_scret
    ))
    
    # Creating a Producer
    producer=KafkaProducer(
        bootstrap_servers=arguments.kafka_brokers, 
        retries=arguments.kafka_retries, 
        value_serializer=lambda record: dumps(record).encode('utf-8')
    )      

    #
    for tweet in twitter.statuses.sample(language=arguments.tweet_language):
        if 'delete' in tweet:
           # Deleting tweets without text
           continue

        # Printing the text
        if arguments.logs:
            print(tweet['text'])

        # Collecting the hashtags
        hashtags = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
        
        if len(hashtags) > 0:
            future=producer.send(arguments.kafka_topic, hashtags)

            # Printing the hashtags
            if arguments.logs:
                print(hashtags) 

                # Assigning a callback success function  
                future=future.add_callback(lambda metadata: print({
                    'status':'success', 
                    'topic':metadata.topic, 
                    'partition':metadata.partition, 
                    'offset':metadata.offset
                }))

                # Assigning a callback error function  
                future=future.add_errback(lambda exception: print({
                    'status':'error', 
                    'error':exception
                }))

            # Locking until all asynchronous messages are sent
            #producer.flush()