from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('ReadPressureFile').getOrCreate()
#Creatng schema for the data in file#
schema = StructType([
    StructField("year_yyyy", StringType(), True),
    StructField("month_mm", StringType(), True),
    StructField("day_dd", StringType(), True),
    StructField("obs_val1", StringType(), True),
    StructField("obs_val2", StringType(), True),
    StructField("obs_val3", StringType(), True),
    StructField("obs_val4", StringType(), True),
    StructField("obs_val5", StringType(), True),
    StructField("obs_val6", StringType(), True),
    StructField("obs_val7", StringType(), True),
    StructField("obs_val8", StringType(), True),
    StructField("obs_val9", StringType(), True)
])

#The files with fewer columns will be added with null values for mode equal to permissive
df = spark.read.csv('path2//pressure_data.txt', schema=schema, sep="\t", mode="PERMISSIVE")

# column values in case it has null, replaced with 0 value as part of data cleanup.
#Assuming first 3 columns of date values will not be empty at any case.
df = df.fillna({'obs_val1':'0'},{'obs_val2':'0'},{'obs_val3':'0'},
{'obs_val4':'0'},{'obs_val5':'0'},{'obs_val6':'0'},{'obs_val7':'0'},{'obs_val8':'0'},{'obs_val9':'0')

#Save as parquet in partitioned table location for efficient analytical purposes.
df.write.partitionBy('year_yyyy', 'month_mm').parquet("/data/pressure_data/pressure.parquet")

