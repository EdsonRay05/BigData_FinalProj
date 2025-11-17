# kafka_proc.py

import json
import time
import random
from datetime import datetime
import requests
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'weather_topic'
CITY = 'Antipolo City'
API_KEY = '100a505f5ee7872f379bd184039ebd46'
LAT = 14.6255
LON = 121.1245
API_URL = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Producer started, streaming weather data for {CITY}...")

while True:
    try:
        response = requests.get(API_URL)
        data = response.json()
        main = data.get('main', {})

        # Use some random jitter to simulate live sensor changes
        base_temp = main.get("temp", 0) - 273.15 if "temp" in main else None
        base_humidity = main.get("humidity", None)
        base_pressure = main.get("pressure", None)

        message = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "temperature": (base_temp + random.uniform(-0.5, 0.5)) if base_temp is not None else None,
            "humidity": (base_humidity + random.randint(-2, 2)) if base_humidity is not None else None,
            "pressure": (base_pressure + random.randint(-1, 1)) if base_pressure is not None else None,
            "city": CITY
        }
        producer.send(TOPIC, value=message)
        producer.flush()
        print("Sent:", message)
    except Exception as e:
        print("API or Kafka Error:", e)
    time.sleep(15)