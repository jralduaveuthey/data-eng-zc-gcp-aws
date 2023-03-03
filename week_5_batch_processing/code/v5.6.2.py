#!/usr/bin/env python
# coding: utf-8

# # Corresponding video: DE Zoomcamp DE Zoomcamp 5.6.2 - Creating a Local Spark Cluster
from pyspark.sql import SparkSession

# before this, you need to start the spark master in the terminal: ...spark-3.3.2-bin-hadoop3> bin\spark-class org.apache.spark.deploy.master.Master
spark = SparkSession.builder \
    .appName('test2') \
    .getOrCreate()

# before this, you need to start the spark worker in the terminal: ...spark-3.3.2-bin-hadoop3> bin\spark-class org.apache.spark.deploy.worker.Worker spark://192.168.178.72:7077
df_green = spark.read.parquet('week_5_batch_processing/code/data/pq/green/*/*')

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.parquet('data/pq/yellow/*/*')


df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

set(df_green.columns) & set(df_yellow.columns)

#When we do set(df_green.columns) & set(df_yellow.columns)...it does not keeep the order of the columns so we use a for loop to keep the same order
common_colums = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_colums.append(col)

common_colums

from pyspark.sql import functions as F
df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.groupBy('service_type').count().show()

df_trips_data.columns

df_trips_data.registerTempTable('trips_data') #We need to register this as table so we can use it below in the SQL query after FROM

spark.sql("""
SELECT
    service_type,
    count(1)
FROM
    trips_data
GROUP BY 
    service_type
""").show()

#Original query comes from week_4_analytics_engineering\taxi_rides_ny\models\core\dm_monthly_zone_revenue.sql
df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

df_result.show()

df_result.write.parquet('data/report/revenue/')
#when running this in http://localhost:4040/jobs it is skipping most of the jobs => the skipping is okay, see https://stackoverflow.com/questions/34580662/what-does-stage-skipped-mean-in-apache-spark-web-ui

# and it is not writing as many parquet files as in the video 5.3.4 but it does not seem very important since with df_result.repartition(20) as done below I can get more partitions

# length of df_result
df_result.count()

df_result.coalesce(1).write.parquet('data/report/revenue/', mode='overwrite')
#coalesce(1) to reduce it to only one partition

df_result.repartition(20).write.parquet('data/report/revenue/', mode='overwrite')