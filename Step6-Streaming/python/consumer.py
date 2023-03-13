import argparse
from pathlib import Path
import os
from confluent_kafka import Consumer
from settings import read_config

    
def main_flow(params) -> None:
    """main flow to process stream messages"""    
    topic = params.topic
    groupid = params.groupid    
    config_path = params.config

    settings = read_config(config_path)
    settings["group.id"] = groupid
    settings["auto.offset.reset"] = "earliest"
    settings['enable_auto_commit']=  True
    settings['key_deserializer']= lambda key: int(key.decode('utf-8'))
    settings['value_deserializer']= lambda value: value.decode('utf-8')

    consumer = Consumer(settings)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg == {}:
                    continue
            for msg_key, msg_values in msg.items():
                for msg_val in msg_values:
                    print(f'Key:{msg_val.key}-type({type(msg_val.key)}), '
                            f'Value:{msg_val.value}-type({type(msg_val.value)})')                        
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    """main entry point with argument parser"""
    os.system('clear')
    print('running...')
    parser = argparse.ArgumentParser(description='Producer : --topic yellow_trips --groupid yellow_group --config path-to-config')
    
    parser.add_argument('--topic', required=True, help='kafka topic')
    parser.add_argument('--groupid', required=True, help='consumer group')
    parser.add_argument('--config', required=True, help='confluent cloud setting')    
    args = parser.parse_args()

    main_flow(args)
    print('end')

# usage
# producer --topic rides_green --file ../data/fhv_tripdata_2019-01.csv.gz --config ~/.kafka/confluent.properties