import paho.mqtt.client as mqtt
import json
import time
import os
import uuid
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Broker details
BROKER_URL = os.environ.get("BROKER_URL", "0aed2badc27d48c496c3f98bd955ca42.s1.eu.hivemq.cloud")
PORT = int(os.environ.get("BROKER_PORT", 8883))
USERNAME = "yazidhoudayfa"
PASSWORD = "Yazidhoudayfa1"
CLIENT_ID = "mqtt_consumer"
TOPIC = "machine/#"  # Subscribe to all machine topics
QOS = 1

# Kafka details
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = "machine-data"

# Initialize Kafka producer
def create_kafka_producer():
    for i in range(10):  # Retry logic
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Successfully connected to Kafka")
            return producer
        except Exception as e:
            logger.error(f"Failed to connect to Kafka (attempt {i+1}/10): {e}")
            if i == 9:  # Last attempt
                raise
            time.sleep(10)  # Wait before retrying

# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Connected to HiveMQ broker! Subscribing to topic '{TOPIC}'")
        client.subscribe(TOPIC, qos=QOS)
    else:
        logger.error(f"Failed to connect to broker with result code {rc}")

def on_message(client, userdata, msg):
    print("✅ MQTT Message received")
    try:
        topic = msg.topic
        payload = msg.payload.decode('utf-8')
        logger.info(f"Received message on topic {topic}: {payload[:100]}...")  # Print first 100 chars
        
        # Parse the JSON payload
        data = json.loads(payload)
        
        # Add a unique ID to each record
        data['record_id'] = str(uuid.uuid4())
        data['source_topic'] = topic
        data['processing_timestamp'] = int(time.time())
        
        # Send to Kafka
        userdata['producer'].send(KAFKA_TOPIC, data)
        userdata['producer'].flush()
        logger.info(f"Sent message to Kafka topic '{KAFKA_TOPIC}'")
    
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main():
    try:
        # Create Kafka producer
        producer = create_kafka_producer()
        
        # Create MQTT client
        client = mqtt.Client(CLIENT_ID, protocol=mqtt.MQTTv311, userdata={'producer': producer})
        client.username_pw_set(USERNAME, PASSWORD)
        
        # Set callbacks
        client.on_connect = on_connect
        client.on_message = on_message
        
        # Connect to HiveMQ
        client.tls_set()  # Use SSL for secured connection
        client.connect(BROKER_URL, PORT)
        client.loop_start()
        
        logger.info(f"Connecting to HiveMQ broker at {BROKER_URL}...")
        
        # Keep the client connected to receive messages
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Stopped receiving messages.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if 'client' in locals():
            client.loop_stop()
            client.disconnect()
        logger.info("MQTT consumer shutdown complete")

if __name__ == "__main__":
    main()