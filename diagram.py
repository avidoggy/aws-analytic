from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import (Athena, Glue, GlueCrawlers,
                                    GlueDataCatalog, Quicksight)
from diagrams.aws.storage import S3

with Diagram("demo-diagram", show=True):

    with Cluster('ETL - csv to parquet with data-type fine tune'):
        raw_data = S3('raw nyc-tlc-data')
        elt = Glue('Glue Job')
        parquet_data = S3('Parquet')
        raw_data >> elt >> parquet_data

    with Cluster('Exploratory Data Analysis'):
        crawler = GlueCrawlers('Crawler')
        catalog = GlueDataCatalog('DataCatalog')
        eda = Athena('Athena')
    parquet_data >> crawler >> catalog >> eda

    with Cluster('ETL - Filter outliers'):
        etl = Glue('Glue Job')
        clear_data = S3('DataCube')
        crawler = GlueCrawlers('Crawler')
        catalog = GlueDataCatalog('DataCatalog')
        engine = Athena('Athena')
        dashboard = Quicksight('Dashboard')
    parquet_data >> etl >> clear_data
    clear_data >> crawler >> catalog >> engine >> dashboard

    insight = Edge(label='Insight', style="dashed")
    eda >> insight >> etl
