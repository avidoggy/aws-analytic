# native python
import itertools
import sys

# AWS GLUE
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
# pyspark
from pyspark.context import SparkContext
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType
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
# spark = SparkSession.builder \
#     .master("local") \
#     .appName("etl") \
#     .config("spark.driver.memory", "12g") \
#     .getOrCreate()

dataset_name = 'yellow'
source_path = 's3a://nyc-tlc/trip data/{dataset}_tripdata_{year}-{month:02}.csv'
destination_path = 's3a://gavin-data-lake/nyc-tlc/trip-data/{dataset}-l0/'

years = range(2009, 2021)
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
            header=True, enforceSchema=False, inferSchema=False,
            ignoreLeadingWhiteSpace=True,
            ignoreTrailingWhiteSpace=True,
        )
    except AnalysisException as e:
        print('AnalysisException', e)
        continue
    # raw.printSchema()
    df = raw
    df = df.withColumn('year', lit(year).astype(IntegerType()))
    df = df.withColumn('month', lit(month).astype(IntegerType()))
    df.write.parquet(
        path=destination_path.format(dataset=dataset_name),
        mode="overwrite",
        partitionBy=["year", "month"],
    )

job.commit()
