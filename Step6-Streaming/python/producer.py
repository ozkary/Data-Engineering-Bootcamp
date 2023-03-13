import argparse
from pathlib import Path
import os
import pandas as pd
from time import sleep
from typing import Dict
from confluent_kafka import Producer
from settings import read_config


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    
def read_records(file_path: str):
        """Read the records from the csv file"""        
        # read only 5 records 
        # TODO use a iterator to read a large file
        df = pd.read_csv(file_path, nrows=5) 
        if df:    
            records, ride_keys = [], []
            for row in df.itertuples(index = True):            
                try:                                                        
                    records.append(f'{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[9]}, {row[16]}')
                    ride_keys.append(str(row[0]))
                except StopIteration as ex:
                    print(f"Finished reading file {ex}")
                    break
                except Exception as ex:
                    print(f"Error found {ex}")
                    return
                    
            print(f"Messages processed {file_path}")
            return zip(ride_keys, records)

def publish(producer: Producer, topic: str, records: [str, str]):
        """Publish the messages"""
        for key_value in records:
            key, value = key_value
            try:
                producer.Produce(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        producer.flush()
        sleep(1) 

def main_flow(params) -> None:
    """main flow to read and send the messages"""    
    topic = params.topic
    csv = params.file
    config_path = params.config

    settings = read_config(config_path)
    producer = Producer(settings)
    messages = read_records(csv)
    publish(producer, messages)    


if __name__ == '__main__':
    """main entry point with argument parser"""
    os.system('clear')
    print('running...')
    parser = argparse.ArgumentParser(description='Producer : --topic yellow_trips --file file.csv.gz --config path-to-config')
    
    parser.add_argument('--topic', required=True, help='stream topic')
    parser.add_argument('--file', required=True, help='gz file')
    parser.add_argument('--config', required=True, help='confluent cloud setting')    
    args = parser.parse_args()

    main_flow(args)
    print('end')

# usage
# producer --topic rides_green --file ../data/fhv_tripdata_2019-01.csv.gz --config ~/.kafka/confluent.properties