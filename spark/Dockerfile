FROM bitnami/spark:3.3.0

USER root

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Create app directory
RUN mkdir -p /opt/spark/app && \
    chmod -R 777 /opt/spark/app

# Download BigQuery and GCS connectors
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.30.0/spark-bigquery-with-dependencies_2.12-0.30.0.jar /opt/bitnami/spark/jars/

USER 1001
