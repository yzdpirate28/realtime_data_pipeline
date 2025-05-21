from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 8, 17),
}

# Create the DAG
dag = DAG(
    'mqtt_data_pipeline',
    default_args=default_args,
    description='A DAG for processing MQTT sensor data',
    schedule_interval=timedelta(seconds=100),  # Run every 100 seconds
    catchup=False,
)

# Monitor Kafka topics task
def monitor_kafka_topics():
    """Log information about Kafka topics"""
    from kafka import KafkaConsumer, TopicPartition
    import json
    import logging
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=['kafka:9092'],
            group_id='airflow-monitor',
            auto_offset_reset='latest',
            enable_auto_commit=True,
        )
        topics = consumer.topics()
        logging.info(f"Available Kafka topics: {topics}")
        
        target_topic = 'machine-data'
        if target_topic in topics:
            partitions = consumer.partitions_for_topic(target_topic)
            if partitions:
                topic_partitions = [TopicPartition(target_topic, p) for p in partitions]
                consumer.assign(topic_partitions)
                end_offsets = consumer.end_offsets(topic_partitions)
                logging.info(f"Kafka topic '{target_topic}' end offsets: {end_offsets}")
                
                # Get a sample message (non-blocking)
                records = consumer.poll(timeout_ms=10000, max_records=30)  
                if records:
                    logging.info(f"Sample messages from topic '{target_topic}': {records}")
                else:
                    logging.info(f"No messages available in topic '{target_topic}'")
                
                for tp, msgs in records.items():
                    for msg in msgs:
                        try:
                            data = json.loads(msg.value.decode('utf-8'))
                            logging.info(f"Sample message: {data}")
                            break
                        except Exception as decode_err:
                            logging.warning(f"Message decode failed: {decode_err}")
                    break  # We only want one sample
        
        consumer.close()
    except Exception as e:
        logging.error(f"Error monitoring Kafka: {e}")
        raise

# Verify BigQuery authentication and access
def verify_bigquery_access():
    """Verify access to BigQuery and create dataset if it doesn't exist"""
    from google.cloud import bigquery
    import os
    import logging
    
    try:
        # Get environment variables or use defaults
        project_id = os.environ.get("BQ_PROJECT_ID")
        dataset_id = os.environ.get("BQ_DATASET_ID", "machine_sensor_data")
        table_id = os.environ.get("BQ_TABLE_ID", "aggregated_metrics")
        
        if not project_id:
            logging.error("BQ_PROJECT_ID environment variable is not set")
            raise ValueError("BQ_PROJECT_ID environment variable is not set")
            
        logging.info(f"Verifying BigQuery access for project: {project_id}")
        
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        
        # Check if dataset exists, create if not
        dataset_ref = client.dataset(dataset_id)
        try:
            client.get_dataset(dataset_ref)
            logging.info(f"Dataset {project_id}.{dataset_id} already exists")
        except Exception:
            # Dataset does not exist, create it
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Set the location
            dataset = client.create_dataset(dataset)
            logging.info(f"Created dataset {project_id}.{dataset_id}")
        
        # Define schema for the table
        schema = [
            bigquery.SchemaField("window_start", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("window_end", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("machine_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("avg_temperature", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("max_temperature", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("avg_vibration", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("max_vibration", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("avg_power", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("total_defects", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("record_count", "INTEGER", mode="NULLABLE"),
        ]
        
        # Check if table exists, create if not
        table_ref = dataset_ref.table(table_id)
        try:
            client.get_table(table_ref)
            logging.info(f"Table {project_id}.{dataset_id}.{table_id} already exists")
        except Exception:
            # Table does not exist, create it
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="window_start"
            )
            table = client.create_table(table)
            logging.info(f"Created table {project_id}.{dataset_id}.{table_id}")
        
        logging.info("BigQuery configuration verified successfully")
        
    except Exception as e:
        logging.error(f"Error verifying BigQuery access: {e}")
        raise

# Define tasks
monitor_task = PythonOperator(
    task_id='monitor_kafka_topics',
    python_callable=monitor_kafka_topics,
    dag=dag,
)

# Check BigQuery configuration
bigquery_check_task = PythonOperator(
    task_id='verify_bigquery_access',
    python_callable=verify_bigquery_access,
    dag=dag,
)

# Environment variables needed for Spark job
env_vars = {
    'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092',
    'BQ_PROJECT_ID': '{{ var.value.bq_project_id }}',
    'BQ_DATASET_ID': '{{ var.value.bq_dataset_id or "machine_sensor_data" }}',
    'BQ_TABLE_ID': '{{ var.value.bq_table_id or "aggregated_metrics" }}',
    'BQ_TEMP_BUCKET': '{{ var.value.bq_temp_bucket }}',
    'GOOGLE_APPLICATION_CREDENTIALS': '/secrets/bigquery-key.json'
}

# Spark job to process data from Kafka and send to BigQuery
spark_task = SparkSubmitOperator(
    task_id='process_data_with_spark',
    conn_id='spark_default',
    application='/opt/spark/app/process_data.py',
    conf={
        'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0',
        'spark.hadoop.google.cloud.auth.service.account.enable': 'true',
        'spark.hadoop.google.cloud.auth.service.account.json.keyfile': '/secrets/bigquery-key.json',
    },
    env_vars=env_vars,
    executor_cores=1,
    executor_memory='2g',
    name='machine_data_processor',
    dag=dag,
)

# Define task dependencies
monitor_task >> bigquery_check_task >> spark_task