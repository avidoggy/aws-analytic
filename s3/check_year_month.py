import boto3
from botocore.errorfactory import ClientError
import itertools

s3 = boto3.client('s3')

datasets = ['fhvhv', 'fhv', 'yellow', 'green']
years = range(2015, 2021)
months = range(1, 13)
for dataset, year, month in itertools.product(datasets, years, months):
    bucket = 'nyc-tlc'
    key = f'trip data/{dataset}_tripdata_{year}-{month:02}.csv'
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(bucket, key, 'not found')
