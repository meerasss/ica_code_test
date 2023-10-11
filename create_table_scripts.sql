CREATE EXTERNAL TABLE IF NOT EXISTS temperature_data(
  `day_dd` STRING,
  `morn` STRING,
  `noon` STRING,
  `eve` STRING,
  `tmax` STRING,
  `tmin` STRING,
  `est_tmean` STRING)
PARTITIONED BY (year_yyyy string,month_mm string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS PARQUET
LOCATION '/data/temperature_data/';

-------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS pressure_data(
  `day_dd` STRING,
  `obs_val1` STRING,
  `obs_val2` STRING,
  `obs_val3` STRING,
  `obs_val4` STRING,
  `obs_val5` STRING,
  `obs_val6` STRING,
  `obs_val7` STRING,
  `obs_val8` STRING,
  `obs_val9` STRING)
PARTITIONED BY year_yyyy string,month_mm string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS PARQUET
LOCATION '/data/pressure_data/';
