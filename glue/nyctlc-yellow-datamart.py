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
spark.conf.set("spark.sql.caseSensitive", "true")

dataset = 'yellow'
base_path = f's3a://gavin-data-lake/nyc-tlc/trip-data/{dataset}/'
destination_path = f's3a://gavin-data-lake/nyc-tlc/trip-data/{dataset}-datamart/'

years = range(2015, 2021)
months = range(1, 13)
for year, month in itertools.product(years, months):

    source_path = f's3a://gavin-data-lake/nyc-tlc/trip-data/{dataset}/year={year}/month={month}'
    print(f'Reading from {source_path}')

    try:
        # Reading CSV but not infer schema
        raw = spark.read.option(
            'basePath', base_path
        ).parquet(
            path=source_path
        ),
    except AnalysisException as e:
        # For handling path is not existed
        print('AnalysisException', e)
        continue

    raw.createOrReplaceTempView(dataset)
    df = spark.sql(f'''
        SELECT
            CAST(year AS INT) AS year,
            QUARTER(pickup_datetime) AS quarter,
            CAST(month AS INT) AS month,

            CAST(vendor_name AS INT) AS vendor_name,

            DAY_OF_WEEK(pickup_datetime) AS pickup_day_of_week,
            HOUR(pickup_datetime) AS pickup_hour,

            DAY_OF_WEEK(dropoff_datetime) AS dropoff_day_of_week,
            HOUR(dropoff_datetime) AS dropoff_hour,

            passenger_count,

            trip_distance,
            trip_in_seconds,

            trip_distance/ (trip_in_seconds/3600.0) AS avg_speed_per_hour,

            ratecode_id,
            payment_type,

            pickup_location_id,
            dropoff_location_id

            -- pickup_longitude
            -- pickup_latitude
            -- dropoff_longitude
            -- dropoff_latitude

            total_amount
        FROM {dataset}
        WHERE (vendor_name = '1' OR vendor_name = '2')
        AND (passenger_count < 10)
        AND (trip_distance < 25)
        AND (trip_in_seconds BETWEEN 0 AND 5000)
        AND (ratecode_id BETWEEN 1 AND 6)
        AND (payment_type BETWEEN 1 AND 6)
        AND (total_amount < 90)
        AND store_and_forward_flag IS NOT NULL
    ''')

    df.printSchema()

    print(f'Writing to {destination_path}')
    df.write.mode(
        'append'
    ).partitionBy(
        'year', 'quarter', 'month', 'payment_type',
    ).bucketBy(
        100, 'trip_distance', 'trip_in_seconds', 'avg_speed_per_hour', 'total_amount',
    ).sortBy(
        'pickup_location_id', 'dropoff_location_id',
    ).parquet(
        path=destination_path,
        compression='snappy',
    )

job.commit()
