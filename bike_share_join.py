"""
spark-submit --master yarn --deploy-mode client --num-executors 2 --driver-memory 2g --executor-memory 2g --executor-cores 1 bike_share_join.py 3
"""
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def load_data(spark, schema, file):
    df = spark.read\
    .schema(schema)\
    .csv(file)

    df.printSchema()
    df.show(5)

    return df


def join_data(df_a, df_b, key, join_type, select_columns):
    join_df = df_a.join(df_b, df_a[key] == df_b[key],join_type)\
    .select(select_columns)

    join_df.show(5)
    join_df.count()
    return join_df


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

    df_stations = load_data(spark, schema_stations, 's3://skax-aws-edu-data/bike-share/stations.csv')
   
    schema_status = StructType() \
    .add("station_id",IntegerType(),True) \
    .add("bikes_available",IntegerType(),True) \
    .add("docks_available",IntegerType(),True) \
    .add("time",TimestampType(),True)

    df_status = load_data(spark, schema_status, 's3://skax-aws-edu-data/bike-share/status.csv')
    
    join_df = join_data(df_stations, df_status, "station_id", "inner", ["name", "bikes_available"])

    spark.stop()

