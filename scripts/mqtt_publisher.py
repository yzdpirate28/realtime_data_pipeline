import paho.mqtt.client as mqtt
import time
import random
import json
import os

# Broker details
BROKER_URL = os.environ.get("BROKER_URL", "0aed2badc27d48c496c3f98bd955ca42.s1.eu.hivemq.cloud")
PORT = int(os.environ.get("BROKER_PORT", 8883))
USERNAME = os.environ.get("USERNAME", "yazidhoudayfa")
PASSWORD = os.environ.get("PASSWORD", "Yazidhoudayfa1")
CLIENT_ID = "mqtt_publisher"
TOPIC = "machine/sensors"
QOS = 1

# Define the MQTT client and authentication
client = mqtt.Client(CLIENT_ID, protocol=mqtt.MQTTv311)
client.username_pw_set(USERNAME, PASSWORD)

# Connect to HiveMQ
client.tls_set()  # Use SSL for secured connection
client.connect(BROKER_URL, PORT)
client.loop_start()

# Define a function to publish random data
def publish_data():
    while True:
        # Sample data structure to publish
        data = {
            "machine_id": f"M-{random.randint(1, 100)}",
            "temperature": round(random.uniform(20.0, 100.0), 2),            
            "vibration": round(random.uniform(0.0, 5.0), 2),
            "runtime_hours": random.randint(0, 2000),
            "status": random.choice(["active", "idle", "maintenance_required"]),
            "timestamp": int(time.time()),
            "location": random.choice(["Factory A", "Factory B", "Warehouse 1", "Warehouse 2"]),
            "operator_id": f"O-{random.randint(1, 50)}",
            "power_consumption_kw": round(random.uniform(0.5, 10.0), 2),
            "last_maintenance_date": int(time.time() - random.randint(0, 60 * 60 * 24 * 365)),
            "maintenance_due_date": int(time.time() + random.randint(0, 60 * 60 * 24 * 30)),
            "production_count": random.randint(100, 5000),
            "error_code": random.choice([None, "E001", "E002", "E003"]),
            "error_description": random.choice([None, "Overheating", "Vibration anomaly", "Power surge"]),
            "motor_current_amps": round(random.uniform(5.0, 15.0), 1),
            "ambient_humidity": random.randint(20, 80),
            "defect_count": random.randint(0, 5),
            "bearing_wear_mm": round(random.uniform(0.0, 0.5), 3),
            "gps_coordinates": {
                "lat": round(random.uniform(-90.0, 90.0), 6),
                "lon": round(random.uniform(-180.0, 180.0), 6)
            },
            "energy_cost_estimate": round(random.uniform(1.5, 10.0), 2),
        }
        
        # Publish the JSON-encoded data
        client.publish(TOPIC, json.dumps(data), qos=QOS)
        print(f"Published message to topic '{TOPIC}': {data}")
        time.sleep(0.5)  # Publish every 0.5 seconds

# Run the publisher
try:
    publish_data()
except KeyboardInterrupt:
    print("Stopped publishing messages.")
    client.loop_stop()
    client.disconnect()

