from pyspark.sql import types
from datetime import datetime

fhv_schema = {    
    'hvfhs_license_num': str,
    'dispatching_base_num': str,
    'pickup_datetime': str,
    'dropoff_datetime':str,
    'PUlocationID': str,
    'DOlocationID': str,
    'SR_Flag': str,
    'Affiliated_base_number': str
}

green_schema = {
    'VendorID': str,    
    'lpep_pickup_datetime': str,
    'lpep_dropoff_datetime':str,
    'store_and_fwd_flag': str,
    'RatecodeID': str,
    'PULocationID': str,
    'DOLocationID': str    
}

cols_alias = {
    'rides_fhv': { 'PUlocationID':'pickup_location_id', 'DOlocationID':'dropoff_location_id', 'dropOff_datetime':'dropoff_datetime'},
    'rides_green': {'lpep_pickup_datetime': 'pickup_datetime', 'lpep_dropoff_datetime': 'dropoff_datetime', 'PULocationID':'pickup_location_id', 'DOLocationID':'dropoff_location_id'},
    'sel_cols': ['pickup_location_id','dropoff_location_id','pickup_datetime','dropoff_datetime']
}

rides_schema = types.StructType([
    types.StructField('hvfhs_license_num', types.StringType(), True),
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True)
])
