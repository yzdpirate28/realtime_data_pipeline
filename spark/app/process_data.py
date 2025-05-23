#!/usr/bin/env python
# Spark application to process data from Kafka and send to BigQuery

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, expr, sum as spark_sum, from_json, col, window

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, \
    TimestampType, BooleanType, MapType

# Define schema for machine sensor data
machine_schema = StructType([
    StructField("machine_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("runtime_hours", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("operator_id", StringType(), True),
    StructField("power_consumption_kw", DoubleType(), True),
    StructField("last_maintenance_date", IntegerType(), True),
    StructField("maintenance_due_date", IntegerType(), True),
    StructField("production_count", IntegerType(), True),
    StructField("error_code", StringType(), True),
    StructField("error_description", StringType(), True),
    StructField("motor_current_amps", DoubleType(), True),
    StructField("ambient_humidity", IntegerType(), True),
    StructField("defect_count", IntegerType(), True),
    StructField("bearing_wear_mm", DoubleType(), True),
    StructField("gps_coordinates", MapType(StringType(), DoubleType()), True),
    StructField("energy_cost_estimate", DoubleType(), True),
    StructField("record_id", StringType(), True),
    StructField("source_topic", StringType(), True),
    StructField("processing_timestamp", IntegerType(), True)
])

def main():
    # Create Spark session with BigQuery connector configuration
    spark = SparkSession \
        .builder \
        .appName("MachineDataProcessor") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/bigquery-key.json")) \
        .getOrCreate()
        
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    print("APPLICATION STARTED")
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")) \
        .option("subscribe", "machine-data") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse the JSON data from Kafka
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_data") \
        .select(from_json(col("json_data"), machine_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", expr("CAST(timestamp AS TIMESTAMP)"))
    
    # Create a temporary view for SQL queries
    parsed_df.createOrReplaceTempView("machine_data")
    
    # Print schema
    parsed_df.printSchema()
    
    # Process data - Basic cleaning and transformation
    processed_df = spark.sql("""
        SELECT 
            machine_id,
            COALESCE(temperature, 0.0) as temperature,
            COALESCE(vibration, 0.0) as vibration,
            COALESCE(runtime_hours, 0) as runtime_hours,
            status,
            CAST(timestamp AS TIMESTAMP) as event_timestamp,
            location,
            operator_id,
            COALESCE(power_consumption_kw, 0.0) as power_consumption_kw,
            CAST(last_maintenance_date AS TIMESTAMP) as last_maintenance_date,
            CAST(maintenance_due_date AS TIMESTAMP) as maintenance_due_date,
            COALESCE(production_count, 0) as production_count,
            error_code,
            error_description,
            COALESCE(motor_current_amps, 0.0) as motor_current_amps,
            COALESCE(ambient_humidity, 0) as ambient_humidity,
            COALESCE(defect_count, 0) as defect_count,
            COALESCE(bearing_wear_mm, 0.0) as bearing_wear_mm,
            gps_coordinates,
            COALESCE(energy_cost_estimate, 0.0) as energy_cost_estimate,
            record_id,
            source_topic,
            CAST(processing_timestamp AS TIMESTAMP) as processing_timestamp
        FROM machine_data
    """)
    
    # Output 1: Console for debugging (only showing limited records to avoid spam)
    console_query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 3) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Output 2: Aggregated data for analytics (5-minute windows)
    aggregated_df = processed_df \
    .withWatermark("event_timestamp", "10 minutes") \
    .groupBy(
        window(col("event_timestamp"), "5 minutes"),
        col("machine_id"),
        col("location")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        max("temperature").alias("max_temperature"),
        avg("vibration").alias("avg_vibration"),
        max("vibration").alias("max_vibration"),
        avg("power_consumption_kw").alias("avg_power"),
        spark_sum("defect_count").alias("total_defects"),
        count("*").alias("record_count")
    )
    
    # Prepare the data for BigQuery (flatten window struct)
    bq_ready_df = aggregated_df \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("machine_id"),
            col("location"),
            col("avg_temperature"),
            col("max_temperature"),
            col("avg_vibration"),
            col("max_vibration"),
            col("avg_power"),
            col("total_defects"),
            col("record_count")
        )
    
    # Get BigQuery project, dataset and table from environment variables or use defaults
    project_id = os.environ.get("BQ_PROJECT_ID", "your-project-id")
    dataset_id = os.environ.get("BQ_DATASET_ID", "machine_sensor_data")
    table_id = os.environ.get("BQ_TABLE_ID", "aggregated_metrics")
    temp_bucket = os.environ.get("BQ_TEMP_BUCKET", "your-temporary-gcs-bucket")
    
    # Output 3: BigQuery for aggregated metrics
    bigquery_query = bq_ready_df \
        .writeStream \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.{table_id}") \
        .option("temporaryGcsBucket", temp_bucket) \
        .option("checkpointLocation", "/tmp/checkpoints/bigquery") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .start()
    
    # Keep the Spark application running until manually terminated
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Application terminated by user")
    finally:
        print("Stopping streaming queries...")
        for query in spark.streams.active:
            query.stop()
        print("Application stopped")

if __name__ == "__main__":
    main()