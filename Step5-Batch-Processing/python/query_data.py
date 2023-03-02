import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

os.system('clear')
print('Starting...')
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('process-taxi-data') \
    .getOrCreate()

# Read the parquet files into a data frame
data_path = '../data/fhvhv/'
df = spark.read.parquet(data_path)
                                                                                
# Q3: How many taxi trips were there on June 15 2021?
pickup_dt = '2021-06-15'
from pyspark.sql import functions as F
total = df.withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .filter(f"pickup_date = '{pickup_dt}'") \
    .count()
print(f" Trips in 06/15/2021 {total}")
# using SQL syntax

df.createOrReplaceTempView('fhvhv_tripdata')
spark.sql(f"""
SELECT
    COUNT(1)
FROM 
    fhvhv_tripdata
WHERE
    to_date(pickup_datetime) = '{pickup_dt}';
""").show()

# Q4: Longest trip for each day
# Note: the casting of datetime to long returns the seconds ticks, so we need to divide by minutes and hours (60*60)
print('Longest trip for each day')
df.columns
['hvfhs_license_num',
 'dispatching_base_num',
 'pickup_datetime',
 'dropoff_datetime',
 'PULocationID',
 'DOLocationID',
 'SR_Flag']
df \
    .withColumn('duration', (df.dropoff_datetime.cast('long') - df.pickup_datetime.cast('long'))/( 60 * 60 )) \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .groupBy('pickup_date') \
        .max('duration') \
    .orderBy('max(duration)', ascending=False) \
    .limit(5) \
    .show()

spark.sql("""
SELECT
    to_date(pickup_datetime) AS pickup_date,
    MAX((CAST(dropoff_datetime AS LONG) - CAST(pickup_datetime AS LONG)) / (60 * 60)) AS duration
FROM 
    fhvhv_tripdata
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 5;
""").show()

# Q5 Spark’s User Interface which shows application's dashboard runs on which local port?

# Q6 Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone
zones_path = '../data/zones'
df_zones = spark.read.parquet(zones_path)
df_zones.columns
['LocationID', 'Borough', 'Zone', 'service_zone']

print('Most frequent pickup location zone')
df_zones.createOrReplaceTempView('zones_data')
spark.sql("""
SELECT
   pul.Zone,
   COUNT(1) as Total
FROM 
    fhvhv_tripdata fhv 
    INNER JOIN zones_data pul ON fhv.PULocationID = pul.LocationID    
GROUP BY 
    1
ORDER BY
    2 DESC
LIMIT 5;
""").show()

print('end')