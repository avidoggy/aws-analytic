import sys

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions, types

args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME',
        'datasets',
    ]
)

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasets = [dataset.strip() for dataset in args['datasets'].split(',')]
url_pattern = 's3://nyc-tlc/trip data/{dataset}_tripdata_*.csv'

for dataset in datasets:
    raw = spark.read.csv(
        path=[
            url_pattern.format(dataset=dataset),
        ],
        header="true", inferSchema="true",
        quote='"', sep=",",
        # samplingRatio=0.01,
    ).withColumn('_input_file_name', functions.input_file_name())
    raw.createOrReplaceTempView(dataset)
    df = spark.sql('''
    select
        *,
        regexp_extract(_filename, '(\\\\w+)_(\\\\w+)_(\\\\d+)-(\\\\d+)\\\\.csv', 3) as year,
        regexp_extract(_filename, '(\\\\w+)_(\\\\w+)_(\\\\d+)-(\\\\d+)\\\\.csv', 4) as month
    from (
        select *, split(_input_file_name, '/')[4] as _filename from {dataset}
    )
    '''.format(dataset=dataset)).drop('_input_file_name', '_filename')
    # Writing by AWS GLUE
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(df, glueContext, dataset),
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": f"s3://gavin-data-lake/nyc-tlc/trip-data/{dataset}/",
            "compression": "snappy",
            "partitionKeys": ['year', 'month']
        },
        transformation_ctx=dataset
    )
job.commit()
