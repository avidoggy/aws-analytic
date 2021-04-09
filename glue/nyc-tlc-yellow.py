from pyspark.sql.types import (BooleanType, DateType, DoubleType, FloatType,
                               IntegerType, StringType, StructType)
'''
https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
'''
yello_schema = StructType()\
    .add("vendor_name", IntegerType(), True)\
    .add("trip_pickup_datetime", DateType(), True)\
    .add("trip_dropoff_datetime", DateType(), True)\
    .add("passenger_count", IntegerType(), True)\
    .add("trip_distance", FloatType(), True)\
    .add("start_lon", DoubleType(), True)\
    .add("start_lat", DoubleType(), True)\
    .add("rate_code", IntegerType(), True)\
    .add("store_and_forward", BooleanType(), True)\
    .add("end_lon", DoubleType(), True)\
    .add("end_lat", DoubleType(), True)\
    .add("payment_type", IntegerType(), True)\
    .add("fare_amt", FloatType(), True)\
    .add("surcharge", FloatType(), True)\
    .add("mta_tax", FloatType(), True)\
    .add("tip_amt", FloatType(), True)\
    .add("tolls_amt", FloatType(), True)\
    .add("total_amt", FloatType(), True)
