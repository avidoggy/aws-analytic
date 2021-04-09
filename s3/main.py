
import datetime
from tqdm import tqdm

import boto3
from pydantic import BaseModel


class Model(BaseModel):
    source_bucket: str
    source_key: str
    category_1: str
    category_2: str
    year: str
    month: str
    file_format: str
    target_bucket: str = 'gavin-nyc-tlc'

    def source(self):
        return {
            'Bucket': self.source_bucket,
            'Key': self.source_key,
        }

    def target(self):
        return [
            self.target_bucket,
            f"category_1={self.category_1}/category_2={self.category_2}/year={self.year}/month={self.month}/data.{self.file_format}"
        ]


client = boto3.client('s3')

source_bucket = 'nyc-tlc'
response = client.list_objects_v2(
    Bucket=source_bucket,
    Prefix='trip data',
    RequestPayer='requester',
    # MaxKeys=10,
)

keys = list[Model]()
print('preparing objects')
for content in tqdm(response['Contents']):
    if content['Size'] > 0:
        source_key = content['Key']
        key = content['Key'].split('/')[1]
        (path, file_format, *_) = key.split('.')
        (category_2, category_1, year_month, *_) = path.split('_')
        dt = datetime.datetime.strptime(year_month, '%Y-%m')
        (year, month) = (dt.year, dt.month)
        keys.append(Model(
            source_bucket=source_bucket,
            source_key=source_key,
            category_1=category_1,
            category_2=category_2,
            year=year,
            month=month,
            file_format=file_format,
        ))

s3 = boto3.resource('s3')

print('copying objects')
for key in tqdm(keys):
    print(key.source())
    print(key.target())
    copy_source = key.source()
    (target_bucket, target_key) = key.target()
    s3.meta.client.copy(copy_source, target_bucket, target_key)
