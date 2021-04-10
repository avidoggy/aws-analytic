#!/bin/bash

aws s3 cp s3://nyc-tlc/trip\ data/fhvhv_tripdata_2019-03.csv fhvhv_tripdata_2019-03.csv
aws s3 cp s3://nyc-tlc/trip\ data/fhvhv_tripdata_2019-11.csv fhvhv_tripdata_2019-11.csv

aws s3 cp s3://nyc-tlc/trip\ data/fhvhv_tripdata_2020-03.csv fhvhv_tripdata_2020-03.csv
aws s3 cp s3://nyc-tlc/trip\ data/fhvhv_tripdata_2020-11.csv fhvhv_tripdata_2020-11.csv
