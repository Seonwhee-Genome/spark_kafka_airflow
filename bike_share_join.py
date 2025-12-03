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

    df_stations2 = df_stations.filter(df_stations.landmark=="San Jose")\
    .select(df_stations.station_id,df_stations.name)
    df_stations2.show()
    df_stations2.count()

    df_status = load_data(spark, schema_status, 's3://skax-aws-edu-data/bike-share/status.csv')
    df_status2 = df_status.filter(date_format(df_status.time, 'yyyy')=='2015')\
    .filter(date_format(df_status.time, 'MM')=='02')\
    .filter(date_format(df_status.time, 'dd')>='22')\
    .select(df_status.station_id, df_status.bikes_available, date_format(df_status.time, 'hh').alias('hour'))

    df_status2.show(5)
    df_status2.count()
    
    join_df = join_data(df_stations, df_status, "station_id", "inner", ["name", "bikes_available"])
    join_df2 = join_data(df_stations2, df_status2, "station_id", "inner", ["name", "bikes_available", "hour"])

    avg_df = join_df2.groupBy("name", "hour")\
    .avg("bikes_available")\
    .orderBy("avg(bikes_available)", ascending=False)
    avg_df.show(5)
    avg_df.count()

    avg_df.write.options(header='True', delimiter=',')\
    .mode('overwrite')\
    .csv("bikesharedataframe")

    spark.stop()

