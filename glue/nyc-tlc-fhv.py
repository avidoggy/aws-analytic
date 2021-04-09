from pyspark.sql.types import (BooleanType, DateType, DoubleType, FloatType,
                               IntegerType, StringType, StructType)
'''
https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_fhv.pdf
'''
fhv_schema = StructType()\
    .add("dispatching_base_num", StringType(), True)\
    .add("pickup_datetime", DateType(), True)\
    .add("dropoff_datetime", DateType(), True)\
    .add("pulocationid", IntegerType(), True)\
    .add("dolocationid", IntegerType(), True)\
    .add("sr_flag", IntegerType(), True)
