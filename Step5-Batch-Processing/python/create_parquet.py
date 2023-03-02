import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

os.system('clear')

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('process-taxi-data') \
    .getOrCreate()

# Q1 spark version
print(f"Spark Version - {spark.version}")

# Define the schema type from the columns 
# Index(['dispatching_base_num', 'pickup_datetime', 'dropoff_datetime',
#        'PULocationID', 'DOLocationID', 'SR_Flag', 'Affiliated_base_number'],
#       dtype='object')

schema = types.StructType([
    types.StructField('hvfhs_license_num', types.StringType(), True),
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True)
])
#  Load the dataframe
file_path = '../data/fhvhv_tripdata/fhvhv_tripdata_2021-06.csv.gz'
print(f"Reading - {file_path}")

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(file_path)

#  Partition the data frame
folder_path = '../data/fhvhv'
print(f"Creating partitions - 12 folder {folder_path}")
df.head()
df = df.repartition(12)
df.write.mode('overwrite').parquet(folder_path, compression='gzip')

# Q2 What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.
# df = spark.read.parquet(f'{folder_path}/*/*')


#  gzip 16MB
#  snappy 34MB
