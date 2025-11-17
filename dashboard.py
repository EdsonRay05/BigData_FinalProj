# dashboard.py
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import time

# -------------------------
# CONFIGURATION
# -------------------------
MONGO_URI = 'mongodb+srv://qeresanjuan:nosdeyar@cluster0.sgf8rsf.mongodb.net/?appName=Cluster0'
DB_NAME = 'weather_db'
COLLECTION_NAME = 'weather_data'

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

st.title("Live Weather Dashboard - Rizal")
REFRESH_INTERVAL = 15  # seconds

data_placeholder = st.empty()

def parse_timestamp(ts):
    """Convert timestamps to pandas datetime, handling both Unix and ISO formats."""
    try:
        return pd.to_datetime(float(ts), unit='s')
    except:
        return pd.to_datetime(ts)

while True:
    # Fetch latest data from MongoDB, newest records first
    data_cursor = collection.find().sort("timestamp", -1)
    data_list = list(data_cursor)
    
    if data_list:
        df = pd.DataFrame(data_list)
        df['timestamp'] = df['timestamp'].apply(parse_timestamp)
        
        with data_placeholder.container():
            st.subheader("Latest Weather Data")
            st.write(df.head(1))  # Display the newest record
            st.subheader("History")
            st.dataframe(df)      # Show all data
    else:
        with data_placeholder.container():
            st.write("No data available yet.")
    
    time.sleep(REFRESH_INTERVAL)
