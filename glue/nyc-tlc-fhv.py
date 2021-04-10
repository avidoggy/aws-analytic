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
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (BooleanType, DateType, DoubleType, FloatType,
                               IntegerType, StringType, StructField,
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
https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_fhv.pdf
'''
schemas = {
    2015: StructType([
        StructField("dispatching_base_num", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
    ]),
    2016: StructType([
        StructField("dispatching_base_num", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
    ]),
    2017: StructType([
        StructField("dispatching_base_num", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("pulocationid", IntegerType(), True),
        StructField("dolocationid", IntegerType(), True),
        StructField("sr_flag", IntegerType(), True),
    ]),
    2018: StructType([
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("pulocationid", IntegerType(), True),
        StructField("dolocationid", IntegerType(), True),
        StructField("sr_flag", IntegerType(), True),
        StructField("dispatching_base_num", StringType(), True),
    ]),
}
fhv_schema = StructType([
    StructField("dispatching_base_num", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("pulocationid", IntegerType(), True),
    StructField("dolocationid", IntegerType(), True),
    StructField("sr_flag", IntegerType(), True),
])

dataset_name = 'fhv'
source_path = 's3a://nyc-tlc/trip data/{dataset}_tripdata_{year}-{month:02}.csv'
destination_path = 's3a://gavin-data-lake/nyc-tlc/trip-data/{dataset}/'

spark.createDataFrame(spark.sparkContext.emptyRDD(), fhv_schema).write.parquet(
    path=destination_path.format(dataset=dataset_name),
    mode="overwrite",
    # partitionBy=["year", "month"],
    compression="snappy"
)

years = range(2015, 2021)
months = range(1, 13)
for year, month in itertools.product(years, months):
    print(source_path.format(
        dataset=dataset_name,
        year=year, month=month
    ))
    schema = schemas[year] if year in schemas else fhv_schema
    raw = spark.read.csv(
        path=source_path.format(
            dataset=dataset_name,
            year=year, month=month
        ),
        schema=schema, header=True,
    )
    # raw.printSchema()
    df = raw
    for field in fhv_schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).astype(field.dataType))
    df = df.select(fhv_schema.fieldNames())\
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
