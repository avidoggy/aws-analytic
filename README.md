# aws-analytic

## Demo

![demo-diagram](./demo-diagram.png)

### ETL - csv to parquet with data-type fine tune

- glue job configuration: [nyc-tlc-yellow-job.yaml](glue/nyc-tlc-yellow-job.yaml)
- python: [nyc-tlc-yellow.py](glue/nyc-tlc-yellow.py)
- description
    - Normalizing column names base on variable `column_mapping`
    - Declaring column types using variable `schema`
    - Mapping payment_type by user-defined-funtion `payment_type_f`
    - Transforming monthly csv data to parquet format by each year
        - Using `year` and `month` as partition keys

### Exploratory Data Analysis

- [validation-yellow-not-null.sql](athena/validation-yellow-not-null.sql)
    - Check data quality
- [validation-yellow-distinct.sql](athena/validation-yellow-distinct.sql)
    - Verify categorical data
- [validation-yellow-percentile.sql](athena/validation-yellow-percentile.sql)
    - Identify outlier value

### ETL - Filter outliers

- glue job configuration: [nyctlc-yellow-datacube-job.yaml](glue/nyctlc-yellow-datacube-job.yaml)
- python: [nyctlc-yellow-datacube.py](glue/nyctlc-yellow-datacube.py)
- description
    - Reading data which produced by `nyc-tlc-yellow.py`
    - Filtering data according to insight from `validation-yellow-percentile.sql`
    - Writing data to S3 for QuickSight as datasource
    - Visualizing metrics in QuickSight which use Athena as query engine to query data from S3
