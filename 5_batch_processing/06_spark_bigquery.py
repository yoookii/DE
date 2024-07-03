#!/usr/bin/env python
# coding: utf-8

import argparse


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument("--input_green", required=True)
parser.add_argument("--input_yellow", required=True)
parser.add_argument("--output", required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder \
    .appName('test') \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.39.1") \
    .config('spark.sql.extensions', 'com.google.cloud.spark.bigquery.SparkBigQueryExtension') \
    .config('spark.sql.catalog.spark_bigquery', 'com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider') \
    .getOrCreate()


# spark = SparkSession.builder \
#     .master("spark://Yokis-MacBook-Pro.local:7077") \
#     .appName('test') \
#     .getOrCreate()

# spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-central1-857637254994-gjiaryos')

df_green = spark.read.parquet(input_green)
df_yellow = spark.read.parquet(input_yellow)

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

# combine dataframes into one 
set(df_green.columns) & set(df_yellow.columns)

common_columns = []
yellow_columns = set(df_yellow.columns)
for col in df_green.columns: 
    if col in yellow_columns: 
        common_columns.append(col)

df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.createOrReplaceTempView('trips_data')

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

df_result.write.format('bigquery') \
  .option('table', output) \
  .save()