#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# 2023 ozkary.com

import argparse
from pathlib import Path
import os
from confluent_kafka import Consumer, Producer
from settings import read_config, key_deserializer, value_deserializer


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


def main_flow(params) -> None:
    """main flow to process stream messages"""    
    topic = params.topic
    groupid = params.groupid    
    clientid = params.clientid
    config_path = params.config

    settings = read_config(config_path)
    settings['group.id'] = groupid
    settings['client.id'] = clientid
    settings['auto.offset.reset'] = "earliest"
    settings['enable.auto.commit'] =  True
    
    consumer = Consumer(settings)
    producer = Producer(settings)    
    topics = topic.split(',')
    print(F'Topics {topics}')
    consumer.subscribe(topics)

    # JSON Message format 
    # Pandas(Index=101, dispatching_base_num='B00254', pickup_datetime='2019-01-01 00:33:03', dropOff_datetime='2019-01-01 01:37:24', PUlocationID=140.0, DOlocationID=52.0, SR_Flag=nan, Affiliated_base_number='B02356')    

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg == {} or msg.key() is None:
                    # print('Empty message was received')
                    continue 
                            
            key = key_deserializer(msg.key())
            value = msg.value()

            if value is not None:
                document = value_deserializer(value)
                print(f"Mesage : {key}, {value} Loc: {document['pickup_location_id']}")
                producer.produce(topic='rides_all',
                                  key = msg.key(),
                                  value=msg.value(),
                                  on_delivery=delivery_report
                                  )
                producer.flush()
                                     
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    """main entry point with argument parser"""
    os.system('clear')
    print('consumer is running...')
    parser = argparse.ArgumentParser(description='Producer : --topic green_trips --groupid green_group --clientid app1 --config path-to-config')
    
    parser.add_argument('--topic', required=True, help='kafka topic')
    parser.add_argument('--groupid', required=True, help='consumer group')
    parser.add_argument('--clientid', required=True, help='client id group')
    parser.add_argument('--config', required=True, help='confluent cloud setting')    
    args = parser.parse_args()

    main_flow(args)
    print('end')

# usage
# python3 consumer.py --topic rides_fhv,rides_green --groupid rides --clientid appRideGroup --config ~/.kafka/confluent.properties
