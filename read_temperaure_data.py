from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('ReadTemperatueFile').getOrCreate()
#Creatng schema for the data in file#
schema = StructType([
    StructField("year_yyyy", StringType(), True),
    StructField("month_mm", StringType(), True),
    StructField("day_dd", StringType(), True),
    StructField("morn", StringType(), True),
    StructField("noon", StringType(), True),
    StructField("eve", StringType(), True),
    StructField("tmax", StringType(), True),
    StructField("tmin", StringType(), True),
    StructField("est_tmean", StringType(), True)
])
#Added schema to read  tab separated file without header to convert to dataframe
df = spark.read.csv('path1//temperature_data.txt', schema=schema, sep="\t", mode="PERMISSIVE")

#Save as parquet in partitioned table location for efficient analytical purposes.
df.write.partitionBy('year_yyyy', 'month_mm').parquet("/data/temperature_data/temperature.parquet")
