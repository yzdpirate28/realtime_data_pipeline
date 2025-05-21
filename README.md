# IoT Data Pipeline Project

A scalable data pipeline for processing IoT sensor data from MQTT to BigQuery using Apache Airflow, Kafka, and Spark.

## 🛠️ Architecture

```
HiveMQ MQTT Broker → MQTT Consumer → Kafka → Spark → BigQuery
                            ↑
                       Airflow DAG
                    (orchestration)
```

## 📁 Project Structure

```
iot-data-pipeline/
├── README.md                      # Project documentation
├── docker-compose.yml             # Main Docker Compose configuration
├── .env                           # Environment variables
├── scripts/
│   ├── mqtt_publisher.py          # MQTT Publisher script
│   └── mqtt_consumer.py           # MQTT Consumer script that forwards to Kafka
├── airflow/
│   ├── Dockerfile                 # Slim Airflow Dockerfile
│   ├── dags/
│   │   └── mqtt_data_pipeline.py  # Airflow DAG for data processing
│   └── requirements.txt           # Airflow Python dependencies
├── spark/
│   ├── Dockerfile                 # Slim Spark Dockerfile
│   ├── app/
│   │   └── process_data.py        # Spark script for ETL and forwarding to BigQuery
│   └── requirements.txt           # Spark Python dependencies
└── logs/                          # Logs directory for debugging
```

## ✅ Prerequisites

* Docker and Docker Compose
* Google Cloud account with BigQuery enabled
* HiveMQ Cloud account (already set up in the provided configuration)

## 🚀 Setup Instructions

1. **Clone the repository**

   ```bash
   git clone https://github.com/yourusername/iot-data-pipeline.git
   cd iot-data-pipeline
   ```

2. **Create necessary directories**

   ```bash
   mkdir -p logs airflow/dags spark/app scripts
   ```

3. **Set up environment variables**

   Edit the `.env` file with your HiveMQ credentials
   *(The provided ones may already be configured).*

4. **Build and start the containers**

   ```bash
   docker-compose up -d
   ```

5. **Check if services are running**

   ```bash
   docker-compose ps
   ```

6. **Run the MQTT publisher (in a separate terminal)**

   ```bash
   pip install paho-mqtt
   python scripts/mqtt_publisher.py
   ```

## 📊 Monitoring and Management

* **Airflow UI**: [http://localhost:8080](http://localhost:8080)
  *(Username: `admin`, Password: `admin`)*

* **Spark UI**: [http://localhost:4040](http://localhost:4040)
  *(when Spark jobs are running)*

## 🧐 Development Notes

* The MQTT consumer subscribes to HiveMQ topics and forwards data to Kafka.
* Airflow DAG runs every 5 minutes to monitor Kafka and trigger Spark jobs.
* Spark processes data in streaming mode and sends results to BigQuery.

## 🌐 Production Considerations

* Use proper authentication and secrets management
* Configure BigQuery access using service account keys
* Add monitoring, logging, and alerting for all components
* Scale Kafka and Spark services based on data volume
* Include data validation and error handling mechanisms

## 🛠️ Troubleshooting

* Check logs:

  ```bash
  docker-compose logs -f [service_name]
  ```

* Ensure required ports are free and not used by other services

* Confirm connectivity to HiveMQ Cloud broker

---

Feel free to contribute or raise issues to improve the pipeline!
