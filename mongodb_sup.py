# mongodb_sup.py
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# -------------------------
# CONFIGURATION
# -------------------------
KAFKA_BROKER = 'localhost:9092'                 # Your Kafka broker address
TOPIC = 'weather_topic'                         # Kafka topic to listen to
# MongoDB connection URI (replace with your own!)
MONGO_URI = # Replace with your mongodb uri
DB_NAME = 'weather_db'                          # Database name in MongoDB
COLLECTION_NAME = 'weather_data'                # Collection inside DB

# MongoDB connection
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

print("Consumer started, saving data to MongoDB...")

for message in consumer:
    data = message.value
    collection.insert_one(data)

    print("Saved to MongoDB:", data)
