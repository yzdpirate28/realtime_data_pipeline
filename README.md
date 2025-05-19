IoT Data Pipeline Project
A scalable data pipeline for processing IoT sensor data from MQTT to BigQuery using Apache Airflow, Kafka, and Spark.
Architecture
HiveMQ MQTT Broker → MQTT Consumer → Kafka → Spark → BigQuery
                           ↑
                      Airflow DAG
                   (orchestration)
Project Structure
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
Prerequisites

Docker and Docker Compose
Google Cloud account with BigQuery enabled (for production use)
HiveMQ Cloud account (already set up in the provided configuration)

Setup Instructions
1. Clone the repository
bashgit clone https://github.com/yourusername/iot-data-pipeline.git
cd iot-data-pipeline
2. Create necessary directories
bashmkdir -p logs airflow/dags spark/app scripts
3. Set up environment variables
Edit the .env file with your HiveMQ credentials (the provided ones are already set up).
4. Build and start the containers
bashdocker-compose up -d
5. Check if services are running
bashdocker-compose ps
6. Run the MQTT publisher (in a separate terminal)
bash# Run the publisher locally
pip install paho-mqtt
python scripts/mqtt_publisher.py
Monitoring and Management

Airflow UI: http://localhost:8080 (username: admin, password: admin)
Spark UI: http://localhost:4040 (when Spark jobs are running)

Development Notes

The MQTT consumer subscribes to HiveMQ topics and forwards data to Kafka
Airflow DAG runs every 5 minutes to monitor Kafka and trigger Spark jobs
Spark processes data in streaming mode and prepares it for BigQuery

Production Considerations
For a production environment:

Set up proper authentication for all services
Configure BigQuery credentials using service accounts
Implement proper monitoring and alerting
Scale Kafka and Spark clusters based on data volume
Add data quality checks and error handling

Troubleshooting

Check container logs with docker-compose logs -f [service_name]
Ensure all required ports are open and not used by other applications
Verify connectivity to HiveMQ Cloud broker
