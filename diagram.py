from diagrams import Diagram
from diagrams.aws.analytics import (Athena, GlueCrawlers, GlueDataCatalog, Quicksight, Glue,
                                    KinesisDataFirehose, KinesisDataStreams)
from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3
from diagrams.aws.general import MobileClient
from diagrams.aws.database import Dynamodb
# from diagrams.custom import Custom

with Diagram("ride-hailing", show=True):
    passengers = MobileClient("passengers")
    drivers = MobileClient("drivers")

    collector = KinesisDataStreams("Collector with lambda")

    passengers >> collector
    drivers >> collector

    # near real-time dashboard
    processed = KinesisDataStreams("Processed Activities")
    real_time_processor = Lambda("Time Series Processor")
    time_series_data = Dynamodb("Time Series Data")

    collector >> processed >> real_time_processor >> time_series_data

    # batch processing for trend analytics
    firehose = KinesisDataFirehose("Batch Persistent")
    data_lake = S3("DataLake")
    crawler = GlueCrawlers("Crawlers")
    catalog = GlueDataCatalog("Catalog")

    collector >> firehose >> data_lake >> crawler >> catalog

    # Dashboard
    athena = Athena("Athena")
    dashboard = Quicksight("Quicksight")

    [time_series_data, catalog] >> athena >> dashboard
