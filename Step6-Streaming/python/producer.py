#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  2023 ozkary.com.
#

import argparse
from pathlib import Path
import os
import json
import pandas as pd
from time import sleep
from typing import Dict
from pyspark.sql import DataFrame, types
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from kafka.errors import KafkaTimeoutError
from settings import read_config, key_serializer, value_serializer
from schema import cols_alias


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def load_csv(topic: str, file_path: str) -> DataFrame:
    """
        reads the csv files and renames the columns
    """
    # read only a limit set of rows (memory)
    # TODO use a iterator to read a large file        
    df_all = pd.read_csv(file_path, nrows=150)        
    # rename the cols
    new_col_names = cols_alias[topic]    
    if new_col_names is not None:        
        df_all = df_all.rename(columns=new_col_names)
    df_all = df_all.query("pickup_location_id.notna()")[cols_alias['sel_cols']]    
    df_all['source'] = topic
    print(df_all.head(25))
    return df_all


def read_records(topic: str, file_path: str, key: str):
        """
        Read the records from the csv file
        Args:
            topic (str): determines the topic for the data source
            file_path (str): path to the csv file
            key (str): Key to use of the topic
        """        

        df = load_csv(topic, file_path)
                                        
        if not df.empty:    
            records, ride_keys = [], []            
            try:
                document = json.loads(df.to_json(orient="table"))
                schema = document['schema']
                data = document['data']
                for row in data:     
                    print('row',row)           
                    loc_id =  int(row[key])
                    ride_keys.append(loc_id)                    
                    value = row
                    print('Message id=',loc_id, value) 
                    records.append(value)      
                    break      
            # except StopIteration as ex:
            #     print(f"Finished reading file {ex}")
            #     return
            except KeyboardInterrupt:
                pass
            except Exception as ex:
                print(f"Error found {ex}")
                return
                    
            print(f"Messages processed {file_path}")
            return zip(ride_keys, records)

def publish(producer: Producer, topic: str, records: [str, object]):
        """Publish the messages"""
        for key_value in records:
            key, value = key_value
            try:
                producer.produce(topic=topic, key=key_serializer(key), 
                                 value=value_serializer(value), 
                                 on_delivery=delivery_report)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except KafkaTimeoutError as e:
                print(f"Kafka Timeout {e.__str__()}")
            except Exception as e:
                print(f"Exception while producing record - {key} {value}: {e}")

        producer.flush()
        sleep(1) 

def main_flow(params) -> None:
    """
    main flow to read and send the messages
    """    
    topic = params.topic
    csv = params.file
    config_path = params.config
    key = params.key

    settings = read_config(config_path)
    producer = Producer(settings)    
    messages = read_records(topic, csv, key)
    print('got messages', messages)
    if messages:
        publish(producer, topic, messages)    
    else:
        print('No message')


if __name__ == '__main__':
    """main entry point with argument parser"""
    os.system('clear')
    print('publisher running...')
    parser = argparse.ArgumentParser(description='Producer : --topic yellow_trips --key pickup_location_id --file file.csv.gz --config path-to-config')
    
    parser.add_argument('--topic', required=True, help='stream topic')
    parser.add_argument('--key', required=True, help='topic key')
    parser.add_argument('--file', required=True, help='gz file')
    parser.add_argument('--config', required=True, help='confluent cloud setting') 
    
    args = parser.parse_args()

    main_flow(args)
    print('end')

# usage
# python3 producer.py --topic rides_fhv --key pickup_location_id --file ../data/fhv_tripdata_2019-01.csv.gz --config ~/.kafka/confluent.properties
# python3 producer.py --topic rides_green --key pickup_location_id --file ../data/green_tripdata_2019-01.csv.gz --config ~/.kafka/confluent.properties