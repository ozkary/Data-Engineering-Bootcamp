#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# 2023 ozkary.com

import argparse
from ast import List
from pathlib import Path
import os
import pyspark
from pyspark.sql import SparkSession, DataFrame, types
import pyspark.sql.functions as F
from settings import read_config, key_serializer, value_serializer
from schema import rides_schema

spark = SparkSession.builder \
        .master("local[*]") \
        .appName('process-taxi-data') \
        .getOrCreate()

# set spark logging 
spark.sparkContext.setLogLevel('WARN')

def write_to_console(df: DataFrame, output_mode: str = 'append', processing_time: str = '5 seconds') -> None:
    """
        Output stream values to the console
    """
    console_query = df.writeStream\
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    console_query.awaitTermination()    
    # return console_query

def read_kafka_stream(topic: str, config: any) -> DataFrame:    
    """
        Configure a spark stream from kafka
    """
    server = config['bootstrap.servers']
    username = config['sasl.username']
    password = config['sasl.password']
    clientid = config['client.id']
    groupid = config['group.id']
    
    
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",F"{server},localhost:9092") \
        .option("subscribe", topic) \
        .option("kafka.client.id", clientid)\
        .option("kafka.group.id", groupid)\
        .option("kafka.sasl.mechanisms", "PLAIN")\
        .option("kafka.security.protocol", "SASL_SSL")\
        .option("kafka.sasl.username",username)\
        .option("kafka.sasl.password", password)\
        .option("startingOffsets", "earliest")\
        .option("checkpointLocation", "checkpoint") \
        .option("minPartitions", "2")\
        .option("failOnDataLoss", "true")  \
        .load()
    
    print('kafka stream schema',df_stream.printSchema())
    return df_stream


def parse_ride_from_stream(df: DataFrame, schema: any):
    """ 
    take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema 
    """
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    # parsed the json document in the parsed_value field
    df_parsed = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                .select(F.col("key"),F.from_json(F.col("value"), schema).alias("parsed_value"))    
    print('parsed stream schema',df_parsed.printSchema())
    return df_parsed


def groupby_count(df: DataFrame, columns: any) -> DataFrame:
    """
        Return a dataframe with aggregates for total count
    """
    df_count = df.groupBy(columns).count()
    print('count  schema',df_count.printSchema())
    return df_count

def prepare_df_to_kafka_topic(df: DataFrame, value_columns: any, key_column=None) -> DataFrame:
    """
        Formats the key/value message for kafka
    """
    columns = df.columns

    df = df.withColumn("value", value_serializer(df))
    if key_column:
        df = df.withColumnRenamed(key_column, "key")
        df = df.withColumn("key", key_serializer(df.key.cast('string')))
    df_messages = df.select(['key', 'value'])
            
    print('count schema',df_messages.printSchema())
    return df_messages

def load_zones() -> DataFrame:
    """
        Loads the zone lookup information
    """
    zones_path = '../data/zones'
    df_zones = spark.read.parquet(zones_path)
    return df_zones.select("LocationID","Zone")


def write_to_kafka_stream(df: DataFrame, topic: str, config: any) -> None:
    """
        write to a kafka stream
    """

    server = config['bootstrap.servers']    
    username = config['sasl.username']
    password = config['sasl.password']
    clientid = config['client.id']
    groupid = config['group.id']

    write_stream = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", server) \
        .option("subscribe", topic) \
        .option("kafka.client.id", clientid)\
        .option("kafka.group.id", groupid)\
        .option("kafka.sasl.mechanisms", "PLAIN")\
        .option("kafka.security.protocol", "SASL_SSL")\
        .option("kafka.sasl.username",username)\
        .option("kafka.sasl.password", password)\
        .option("topic", topic) \
        .option("checkpointLocation", "checkpoint") \
        .outputMode('complete') \
        .start()
    
    write_stream.awaitTermination()     

def main_flow(params) -> None:
    """main flow to process stream messages with spark"""    
    topic = params.topic
    groupid = params.groupid    
    clientid = params.clientid
    config_path = params.config
    topic_all = params.topic_all
    topic_count = params.topic_count
    group_by = params.groupby

    # set the session information
    settings = read_config(config_path)    
    settings['group.id'] = groupid
    settings['client.id'] = clientid

    # get the dataframe stream
    df_stream = read_kafka_stream(topic=topic, config=settings) 

    # write to the output topic   
    write_to_kafka_stream(df=df_stream, topic=topic_all)

    # parse the messages
    df_rides = parse_ride_from_stream(df_stream,schema=rides_schema)
    write_to_console(df_rides)
    
    # get the counts by pickup location
    field_name = group_by    
    df_rides_by_pickup_location = groupby_count(df_rides, [field_name])
    write_to_console(df_rides_by_pickup_location)

    # join to get the location name    
    df_zones = load_zones()
    df_rides_joined_by_location = df_rides_by_pickup_location.join(df_zones, field_name)
    write_to_console(df_rides_joined_by_location)

    # format the df to a kafka message
    df_trip_count_messages = prepare_df_to_kafka_topic(df=df_rides_joined_by_location,
                                                      value_columns=['count'], 
                                                      key_column=field_name)
    write_to_console(df_trip_count_messages)

    # write to the count output stream
    write_to_kafka_stream(df=df_trip_count_messages, topic=topic_count)

    
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    """
        Main entry point for streaming data between kafka and spark        
    """
    # os.system('clear')
    print('Spark streaming running...')
    parser = argparse.ArgumentParser(description='Producer : --topic fhv_trips,yellow_trips --groupby pickup_location_id --topic_all rides_all --topic_count rides_by_location  --groupid spark_group --clientid app1 --config path-to-config')
    
    parser.add_argument('--topic', required=True, help='kafka topics')
    parser.add_argument('--groupby', required=True, help='fields to agggregate by')
    parser.add_argument('--topic_all', required=True, help='kafka all output topic')
    parser.add_argument('--topic_count', required=True, help='kafka count output topic')
    parser.add_argument('--groupid', required=True, help='consumer group')
    parser.add_argument('--clientid', required=True, help='client id group')
    parser.add_argument('--config', required=True, help='confluent cloud setting')    
    args = parser.parse_args()

    main_flow(args)

    print('end')


# usage
# python3 spark-streaming.py --topic rides_fhv,rides_green --groupby pickup_location_id --topic_all rides_all --topic_count rides_by_location --groupid all_rides --clientid appAllRides --config ~/.kafka/confluent.properties
# spark-submit spark-streaming.py --topic rides_fhv,rides_green --groupby pickup_location_id --topic_all rides_all --topic_count rides_by_location --groupid all_rides --clientid appAllRides --config ~/.kafka/confluent.properties
