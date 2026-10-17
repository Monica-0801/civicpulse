CREATE DATABASE IF NOT EXISTS civicpulse;

USE civicpulse;

CREATE EXTERNAL TABLE IF NOT EXISTS complaint_clean (
    unique_key STRING,
    created_date STRING,
    agency STRING,
    agency_name STRING,
    complaint_type STRING,
    descriptor STRING,
    location_type STRING,
    incident_zip STRING,
    city STRING,
    borough STRING,
    status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/civicpulse/processed/pig_clean_20261013_v2';

CREATE EXTERNAL TABLE IF NOT EXISTS persistent_hotspots (
    area STRING,
    complaint_type STRING,
    months_observed INT,
    total_complaints BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/civicpulse/analytics/persistent_hotspots_20261013_v2';
