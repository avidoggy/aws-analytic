CREATE EXTERNAL TABLE `taxizones`(
  `object_id` INT,
  `shape_leng` FLOAT,
  `the_geom` STRING,
  `shape_area` FLOAT,
  `zone` STRING,
  `location_id` INT,
  `borough` STRING
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://gavin-data-lake/nyc-tlc/geolocation-data/'
TBLPROPERTIES (
  "skip.header.line.count"="1",
  'classification'='csv'
);
