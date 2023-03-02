import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('zone-lookup-test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('../data/taxi+_zone_lookup.csv')

print(f"Spark Version - {spark.version}\n")

df.show()

df.write.mode('overwrite').parquet('../data/zones')