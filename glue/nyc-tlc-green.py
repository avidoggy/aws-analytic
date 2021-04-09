from pyspark.sql.types import (BooleanType, DateType, DoubleType, FloatType,
                               IntegerType, StringType, StructType)
'''
https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf
'''
green_schema = StructType()\
    .add("vendor_name", IntegerType(), True)\
    .add("lpep_pickup_datetime", DateType(), True)\
    .add("lpep_dropoff_datetime", DateType(), True)\
    .add("store_and_fwd_flag", BooleanType(), True)\
    .add("ratecodeid", IntegerType(), True)\
    .add("pickup_longitude", DoubleType(), True)\
    .add("pickup_latitude", DoubleType(), True)\
    .add("dropoff_longitude", DoubleType(), True)\
    .add("dropoff_latitude", DoubleType(), True)\
    .add("passenger_count", IntegerType(), True)\
    .add("trip_distance", FloatType(), True)\
    .add("fare_amount", FloatType(), True)\
    .add("extra", FloatType(), True)\
    .add("mta_tax", FloatType(), True)\
    .add("tip_amount", FloatType(), True)\
    .add("tolls_amount", FloatType(), True)\
    .add("ehail_fee", FloatType(), True)\
    .add("total_amount", FloatType(), True)\
    .add("payment_type", IntegerType(), True)\
    .add("trip_type_#0", StringType(), True)
