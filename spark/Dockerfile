FROM bitnami/spark:3.3.0

USER root

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Create app directory
RUN mkdir -p /opt/spark/app && \
    chmod -R 777 /opt/spark/app

USER 1001