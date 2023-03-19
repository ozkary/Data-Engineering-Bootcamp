# Kafka Applications


## Load the Producers

- Load both producer with different CSV as sources and topic
  
```

$ python3 producer.py --topic rides_fhv --key pickup_location_id --file ../data/
fhv_tripdata_2019-01.csv.gz --config ~/.kafka/confluent.properties

$ python3 producer.py --topic rides_green --key pickup_location_id --file ../data/green_tripdata_2019-01.csv.gz --config ~/.kafka/confluent.properties

```

## Load the Consumer with Multiple tTopics

- Loads the consumer with two topics

```
$ consumer.py --topic rides_fhv,rides_green --groupid rides --clientid appRideGroup --config ~/.kafka/confluent.properties

```

## Load the PySpark Streaming Consumer 

- Reads different CSV files and sends to two Kafka topics
  - rides_fhv, rides_green
- Forwards the data to another topic
  - rides_all
- Aggregates data based on pickup_location_id
  - Joins with the zones dataframe
  - Send aggregated data to another topic
    - rides_by_location

Note: As with any Spark applications, spark-submit is used to launch your application. spark-sql-kafka-0-10_2.12 and its dependencies can be directly added to spark-submit using --packages

i.e.



```
$ spark-submit # python3 spark-streaming.py --topic rides_fhv,rides_green --groupby pickup_location_id --topic_all rides_all --topic_count rides_by_location --groupid all_rides --clientid appAllRides --config ~/.kafka/confluent.properties
```

