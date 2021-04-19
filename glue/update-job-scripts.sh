#!/bin/bash

SCRIPT_BUCKET="aws-glue-scripts-727247251057-us-east-1"
SCRIPT_KEY_PREFIX="gavin.lin"

echo "aws s3 cp nyc-tlc-fhv.py s3://${SCRIPT_BUCKET}/${SCRIPT_KEY_PREFIX}/nyc-tlc-fhv-job.py"
aws s3 cp nyc-tlc-fhv.py s3://${SCRIPT_BUCKET}/${SCRIPT_KEY_PREFIX}/nyc-tlc-fhv-job.py
aws glue create-job --cli-input-yaml file://nyc-tlc-fhv-job.yaml

echo "aws s3 cp nyc-tlc-fhvhv.py s3://${SCRIPT_BUCKET}/${SCRIPT_KEY_PREFIX}/nyc-tlc-fhvhv-job.py"
aws s3 cp nyc-tlc-fhvhv.py s3://${SCRIPT_BUCKET}/${SCRIPT_KEY_PREFIX}/nyc-tlc-fhvhv-job.py
aws glue create-job --cli-input-yaml file://nyc-tlc-fhvhv-job.yaml

echo "aws s3 cp nyc-tlc-green.py s3://${SCRIPT_BUCKET}/${SCRIPT_KEY_PREFIX}/nyc-tlc-green-job.py"
aws s3 cp nyc-tlc-green.py s3://${SCRIPT_BUCKET}/${SCRIPT_KEY_PREFIX}/nyc-tlc-green-job.py
aws glue create-job --cli-input-yaml file://nyc-tlc-green-job.yaml

echo "aws s3 cp nyc-tlc-yellow.py s3://${SCRIPT_BUCKET}/${SCRIPT_KEY_PREFIX}/nyc-tlc-yellow-job.py"
aws s3 cp nyc-tlc-yellow.py s3://${SCRIPT_BUCKET}/${SCRIPT_KEY_PREFIX}/nyc-tlc-yellow-job.py
aws glue create-job --cli-input-yaml file://nyc-tlc-yellow-job.yaml

echo "aws s3 cp nyctlc-yellow-datacube.py s3://${SCRIPT_BUCKET}/${SCRIPT_KEY_PREFIX}/nyctlc-yellow-datacube-job.py"
aws s3 cp nyctlc-yellow-datacube.py s3://${SCRIPT_BUCKET}/${SCRIPT_KEY_PREFIX}/nyctlc-yellow-datacube-job.py
aws glue create-job --cli-input-yaml file://nyctlc-yellow-datacube-job.yaml
