"""
spark-submit --master yarn --deploy-mode client --num-executors 2 --driver-memory 2g --executor-memory 2g --executor-cores 1 bike_share_join.py 3
"""
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def execute_sql(spark, sql_cmd):
    df = spark.sql(sql_cmd)
    df.show(5)
    df.count()
    return df

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

    df_stations.createOrReplaceTempView("stations")
    df_status.createOrReplaceTempView("status")
    sql_cmd = """ select station_id, name 
    from stations 
    where landmark='San Jose'
    """
    execute_sql(spark, sql_cmd)
    sql_cmd = """ select station_id, bikes_available, time
    from status 
    """
    execute_sql(spark, sql_cmd)
    sql_cmd = """ 
    select station_id, bikes_available, date_format(time,'hh') as hour
    from status
    where INT(date_format(time,'yyyy'))=2015
    and INT(date_format(time,'MM'))=02
    and INT(date_format(time,'dd'))>=22
    """
    execute_sql(spark, sql_cmd)

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

    sql_cmd = """ 
    select b.name, date_format(a.time,'hh') as hour, avg(a.bikes_available) as avg
    from status a inner join stations b
    on a.station_id=b.station_id
    where b.landmark='San Jose'
    and INT(date_format(a.time,'yyyy'))=2015
    and INT(date_format(a.time,'MM'))=02
    and INT(date_format(a.time,'dd'))>=22
    group by b.name, date_format(a.time,'hh')
    order by avg(a.bikes_available) desc
    """
    execute_sql(spark, sql_cmd)


    schema_trips = StructType() \
    .add("trip_id",IntegerType(),True) \
    .add("duration",IntegerType(),True) \
    .add("start_date",StringType(),True) \
    .add("start_station",StringType(),True) \
    .add("start_terminal",IntegerType(),True) \
    .add("end_date",StringType(),True) \
    .add("end_station",StringType(),True) \
    .add("end_terminal",IntegerType(),True) \
    .add("bike",IntegerType(),True) \
    .add("subscription",StringType(),True) \
    .add("zip_code",IntegerType(),True)
    
    df_trips = load_data(spark, schema_trips, 's3://skax-aws-edu-data/bike-share/trips.csv')
    df_trips2 = df_trips.filter(df_trips.subscription == "Customer")\
    .filter(col("start_station") == col("end_station"))\
    .groupBy(col("start_station").alias("station"))\
    .count()\
    .orderBy("count", ascending = False)\
    .show(5)

    df_trips.createOrReplaceTempView("trips")
    sql_cmd = """ select start_station as station, count(*) as count
    from trips 
    where subscription="Customer"
    and start_station = end_station
    group by start_station
    order by count desc
    limit 5
    """
    execute_sql(spark, sql_cmd)    



    schema_weather = StructType() \
    .add("date",StringType(),True) \
    .add("max_temperature_f",IntegerType(),True) \
    .add("mean_temperature_f",IntegerType(),True) \
    .add("min_temperature_f",IntegerType(),True) \
    .add("max_dew_point_f",IntegerType(),True) \
    .add("mean_dew_point_f",IntegerType(),True) \
    .add("min_dew_point_f",IntegerType(),True) \
    .add("max_humidity",IntegerType(),True) \
    .add("mean_humidity",IntegerType(),True) \
    .add("min_humidity",IntegerType(),True) \
    .add("max_sea_level_pressure_inches",DoubleType(),True) \
    .add("mean_sea_level_pressure_inches",DoubleType(),True) \
    .add("min_sea_level_pressure_inches",DoubleType(),True) \
    .add("max_visibility_miles",IntegerType(),True) \
    .add("mean_visibility_miles",IntegerType(),True) \
    .add("min_visibility_miles",IntegerType(),True) \
    .add("max_wind_Speed_mph",IntegerType(),True) \
    .add("mean_wind_speed_mph",IntegerType(),True) \
    .add("max_gust_speed_mph",IntegerType(),True) \
    .add("precipitation_inches",DoubleType(),True) \
    .add("cloud_cover",IntegerType(),True) \
    .add("events",StringType(),True) \
    .add("wind_dir_degrees",IntegerType(),True) \
    .add("zip_code",IntegerType(),True)

    df_weather = load_data(spark, schema_weather, 's3://skax-aws-edu-data/bike-share/weather.csv')

    
    spark.stop()

