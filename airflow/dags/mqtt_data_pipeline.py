from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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

# Define tasks
monitor_task = PythonOperator(
    task_id='monitor_kafka_topics',
    python_callable=monitor_kafka_topics,
    dag=dag,
)

# Spark job to process data from Kafka and send to BigQuery
spark_task = SparkSubmitOperator(
    task_id='process_data_with_spark',
    conn_id='spark_default',
    application='/opt/spark/app/process_data.py',
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.google.cloud:google-cloud-bigquery:2.20.0',
    executor_cores=1,
    executor_memory='1g',
    name='machine_data_processor',
    dag=dag,
)

# Define task dependencies
monitor_task >> spark_task