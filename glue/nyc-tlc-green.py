# native python
import sys
import itertools

# AWS GLUE
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

# pyspark
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (IntegerType, DoubleType, BooleanType, FloatType, StructField,
                               StructType, TimestampType)

args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME',
    ]
)

glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = glueContext.spark_session
# spark = SparkSession.builder \
#     .master("local") \
#     .appName("etl") \
#     .config("spark.driver.memory", "12g") \
#     .getOrCreate()

'''
https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf
'''
rename = {
    "vendorid": "vendor_name",
    "ratecodeid": "ratecode_id",
    "pulocationid": "pickup_location_id",
    "dolocationid": "dropoff_location_id",
}
green_schema = StructType([
    StructField("vendor_name", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", BooleanType(), True),
    StructField("ratecode_id", IntegerType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("fare_amount", FloatType(), True),
    StructField("extra", FloatType(), True),
    StructField("mta_tax", FloatType(), True),
    StructField("tip_amount", FloatType(), True),
    StructField("tolls_amount", FloatType(), True),
    StructField("ehail_fee", FloatType(), True),
    StructField("improvement_surcharge", FloatType(), True),
    StructField("congestion_surcharge", FloatType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", IntegerType(), True),
])

dataset_name = 'green'
source_path = 's3a://nyc-tlc/trip data/{dataset}_tripdata_{year}-{month:02}.csv'
destination_path = 's3a://gavin-data-lake/nyc-tlc/trip-data/{dataset}/'

spark.createDataFrame(spark.sparkContext.emptyRDD(), green_schema).write.parquet(
    path=destination_path.format(dataset=dataset_name),
    mode="overwrite",
    # partitionBy=["year", "month"],
    compression="snappy"
)

years = range(2013, 2021)
months = range(1, 13)
for year, month in itertools.product(years, months):
    print(source_path.format(
        dataset=dataset_name,
        year=year, month=month
    ))
    try:
        raw = spark.read.csv(
            path=source_path.format(
                dataset=dataset_name,
                year=year, month=month
            ),
            header=True, enforceSchema=False,
            inferSchema=True, samplingRatio=0.01,
            ignoreTrailingWhiteSpace=True,
        )
        for column in raw.columns:
            new_col = column.lower()
            new_col = rename[new_col] if new_col in rename else new_col
            raw = raw.withColumnRenamed(column, new_col)
    except AnalysisException as e:
        print('AnalysisException', e)
        continue
    # raw.printSchema()
    df = raw
    for field in green_schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).astype(field.dataType))
        else:
            df = df.withColumn(field.name, lit(None).astype(field.dataType))
    df = df.select(green_schema.fieldNames())\
        .withColumn('year', lit(year).astype(IntegerType()))\
        .withColumn('month', lit(month).astype(IntegerType()))
    # df.printSchema()
    # df.show(vertical=True, n=5)
    df.write.parquet(
        path=destination_path.format(dataset=dataset_name),
        mode="append",
        partitionBy=["year", "month"],
        compression="snappy"
    )

job.commit()
