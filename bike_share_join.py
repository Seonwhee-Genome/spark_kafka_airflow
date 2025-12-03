"""
spark-submit --master yarn --deploy-mode client --num-executors 2 --driver-memory 2g --executor-memory 2g --executor-cores 1 bike_share_join.py 3
"""
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    spark = SparkSession.builder.appName("BikeShareJoin").getOrCreate()

    schema_stations = StructType() \
    .add("station_id",IntegerType(),True) \
    .add("name",StringType(),True) \
    .add("lat",DoubleType(),True) \
    .add("long",DoubleType(),True) \
    .add("dockcount",IntegerType(),True) \
    .add("landmark",StringType(),True) \
    .add("installation",StringType(),True)

    df_stations = spark.read\
    .schema(schema_stations)\
    .csv('s3://skax-aws-edu-data/bike-share/stations.csv')

    df_stations.printSchema()
    df_stations.show(5)

    schema_status = StructType() \
    .add("station_id",IntegerType(),True) \
    .add("bikes_available",IntegerType(),True) \
    .add("docks_available",IntegerType(),True) \
    .add("time",TimestampType(),True)

    df_status = spark.read\
    .schema(schema_status)\
    .csv('s3://skax-aws-edu-data/bike-share/status.csv')

    df_status.printSchema()
    df_status.show(5)

    join_df = df_stations.join(df_status, df_stations.station_id == df_status.station_id,"inner")\
    .select(df_stations.name, df_status.bikes_available)

    join_df.show(5)

    join_df.count()

    spark.stop()

