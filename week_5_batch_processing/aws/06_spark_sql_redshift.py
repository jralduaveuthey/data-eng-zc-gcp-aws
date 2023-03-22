#!/usr/bin/env python
# coding: utf-8

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--redshift_url', required=True)
parser.add_argument('--redshift_table', required=True)
parser.add_argument('--aws_access_key_id', required=True)
parser.add_argument('--aws_secret_access_key', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
redshift_url = args.redshift_url
redshift_table = args.redshift_table
aws_access_key_id = args.aws_access_key_id
aws_secret_access_key = args.aws_secret_access_key

spark = SparkSession.builder \
    .appName('test') \
    .config("spark.jars.packages", "io.github.spark-redshift-community:spark-redshift_2.12:4.2.0") \
    .getOrCreate()

df_green = spark.read.parquet(input_green)

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.parquet(input_yellow)

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

common_colums = [
    # ...
]

df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.registerTempTable('trips_data')

df_result = spark.sql("""
-- ...
""")

df_result.write \
    .format("io.github.spark_redshift_community.spark.redshift") \
    .option("url", redshift_url) \
    .option("tempdir", f"s3a://{aws_access_key_id}:{aws_secret_access_key}@temp/") \
    .option("dbtable", redshift_table) \
    .mode("overwrite") \
    .save()
