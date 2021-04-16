# native python
import itertools
import sys

# AWS GLUE
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
# pyspark
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import (BooleanType, DoubleType, FloatType, IntegerType,
                               StringType, StructField, StructType,
                               TimestampType)
from pyspark.sql.utils import AnalysisException

args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME',
    ]
)

glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = glueContext.spark_session
spark.conf.set("spark.sql.caseSensitive","true")

dataset_name = 'yellow'
source_path = 's3a://nyc-tlc/trip data/{dataset}_tripdata_{year}-{month:02}.csv'
destination_path = 's3a://gavin-data-lake/nyc-tlc/trip-data/{dataset}/'
check_path = 's3a://gavin-data-lake/nyc-tlc/trip-data/{dataset}-{column}/{year}-{month}'

column_mapping = {
    'vendor_id': 'vendor_name',
    'vendorid': 'vendor_name',

    'tpep_pickup_datetime': 'pickup_datetime',
    'trip_pickup_datetime': 'pickup_datetime',

    'tpep_dropoff_datetime': 'dropoff_datetime',
    'trip_dropoff_datetime': 'dropoff_datetime',

    'pulocationid': 'pickup_location_id',
    'dolocationid': 'dropoff_location_id',

    'start_lat': 'pickup_latitude',
    'start_lon': 'pickup_longitude',
    'end_lat': 'dropoff_latitude',
    'end_lon': 'dropoff_longitude',

    'ratecodeid': 'ratecode_id',
    'ratecodeid': 'ratecode_id',
    'rate_code': 'ratecode_id',
    'rate_code': 'ratecode_id',

    'store_and_forward': 'store_and_forward_flag',
    'store_and_fwd_flag': 'store_and_forward_flag',

    'fare_amt': 'fare_amount',
    'tip_amt': 'tip_amount',

    'tolls_amt': 'tolls_amount',
    'total_amt': 'total_amount',
}

schema = StructType([
    StructField('vendor_name', StringType(), False),
    StructField('pickup_datetime', TimestampType(), False),
    StructField('dropoff_datetime', TimestampType(), False),
    StructField('passenger_count', IntegerType(), False),
    StructField('trip_distance', FloatType(), False),
    StructField('pickup_latitude', DoubleType(), False),
    StructField('pickup_longitude', DoubleType(), False),
    StructField('ratecode_id', IntegerType(), False),
    StructField('pickup_location_id', IntegerType(), False),
    StructField('dropoff_location_id', IntegerType(), False),
    StructField('store_and_forward_flag', BooleanType(), False),
    StructField('dropoff_latitude', DoubleType(), False),
    StructField('dropoff_longitude', DoubleType(), False),
    StructField('payment_type', IntegerType(), False),
    StructField('fare_amount', FloatType(), False),
    StructField('surcharge', FloatType(), False),
    StructField('improvement_surcharge', FloatType(), False),
    StructField('congestion_surcharge', FloatType(), False),
    StructField('mta_tax', FloatType(), False),
    StructField('tip_amount', FloatType(), False),
    StructField('tolls_amount', FloatType(), False),
    StructField('total_amount', FloatType(), False),
])

def payment_type_f(v):
    if v is not None:
        mapping = {
            'crd': 1,
            'csh': 2,
            'noc': 3,
            'dis': 4,
            'unk': 5,
            'credit card': 1,
            'credit': 1,
            'cash': 2,
            'no charge': 3,
            'dispute': 4,
            'unknown': 5,
            'voided trip': 6,
            '1': 1,
            '2': 2,
            '3': 3,
            '4': 4,
            '5': 5,
            '6': 6,
        }
        return mapping[v.lower()] if v.lower() in mapping else -1

udf_payment_type = udf(f=payment_type_f, returnType=IntegerType())

years = range(2009, 2021)
months = range(1, 13)
# months = [2, 5, 8, 11]
for year, month in itertools.product(years, months):
    print(f'Reading from {source_path.format(dataset=dataset_name, year=year, month=month)}')

    try:
        # Reading CSV but not infer schema
        raw = spark.read.csv(
            path=source_path.format(
                dataset=dataset_name,
                year=year, month=month
            ),
            header=True, enforceSchema=False, inferSchema=False,
            ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True,
        )
    except AnalysisException as e:
        # For handling path is not existed
        print('AnalysisException', e)
        continue

    # print("1===========")
    # raw.printSchema()
    # raw.show(vertical=True, n=1)

    # Normalizing column names
    df = raw
    for column in df.columns:
        new_column = column.lower()
        new_column = column_mapping[new_column] if new_column in column_mapping else new_column
        df = df.withColumnRenamed(column, new_column)

    # Add partition columns
    df = df.withColumn('year', lit(year).astype(IntegerType()))
    df = df.withColumn('month', lit(month).astype(IntegerType()))

    # print("2===========")
    # df.printSchema()
    # df.show(vertical=True, n=1)

    ##################
    # Data cleansing #
    ##################

    # transfor payment_type
    df = df.withColumn(
        'processed_payment_type',
        udf_payment_type(col('payment_type')).astype(IntegerType())
    )

    # For validating invalid payment_type
    df.select('payment_type', 'processed_payment_type')\
        .filter('processed_payment_type = -1')\
        .repartition(1)\
        .write.csv(
            path=check_path.format(
                dataset=dataset_name, year=year, month=month,
                column='payment_type',
            ),
            mode='overwrite',
            header=True,
        )
    df = df.drop('payment_type').withColumnRenamed('processed_payment_type', 'payment_type')

    # # Data cleansing: changing column types
    for field in schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        else:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))

    # print('3==========================')
    # df.printSchema()
    # df.show(vertical=True, n=1)

    # Data cleansing: removing invalid rows
    # Data cleansing: add calculated columns
    df.createOrReplaceTempView('data')
    df = spark.sql('''
        SELECT
            *,
            UNIX_TIMESTAMP(dropoff_datetime) - UNIX_TIMESTAMP(pickup_datetime) AS trip_in_seconds
        FROM data
        WHERE pickup_datetime IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND passenger_count > 0
        AND trip_distance > 0
        AND payment_type BETWEEN 1 AND 6
        AND total_amount > 0
    ''')

    # print('4==========================')
    # df.printSchema()
    # df.show(vertical=True, n=1)

    print(f'Writing to {destination_path.format(dataset=dataset_name)}')
    df.write.parquet(
        path=destination_path.format(dataset=dataset_name),
        mode="append",
        partitionBy=["year", "month"],
    )

job.commit()
