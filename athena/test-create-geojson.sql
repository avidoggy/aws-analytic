CREATE EXTERNAL TABLE `geolocation_data`(
    objectid INT,
    location_id INT,
    zone string,
    borough string
)
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.GeoJsonSerDe'
STORED AS INPUTFORMAT 'com.esri.json.hadoop.EnclosedGeoJsonInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://gavin-data-lake/nyc-tlc/geolocation-data/';

create table nyctlc.esri_data
row format serde 'com.esri.hadoop.hive.serde.GeoJsonSerDe'
stored as inputformat 'com.esri.json.hadoop.UnenclosedGeoJsonInputFormat'
outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://gavin-data-lake/nyc-tlc/geolocation-data/'
as select * from nyctlc.geolocation_data;

type string,
features array<
    struct<
        type: string,
        properties: struct<
            shape_area: float,
            objectid: int,
            shape_leng: float,
            location_id: int,
            zone: string,
            borough: string
        >,
        geometry: struct<
            type: string,
            coordinates: binary
        >
    >
>