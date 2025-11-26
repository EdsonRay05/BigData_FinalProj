# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh

# ------ Page configuration ------
st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="\U0001f4ca",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ------ Sidebar Configuration ------
def setup_sidebar():
    st.sidebar.title("Dashboard Controls")
    st.sidebar.subheader("MongoDB Configuration")
    st.sidebar.text("Using your MongoDB connection for historical data")
    
    return {
        "mongodb_uri": # Replace with your mongodb uri,
        "database": "weather_db",
        "collection": "weather_data"
    }

# ------ Historical Data from MongoDB ------
def query_historical_data(time_range="1h", metrics=None, config=None):
    try:
        client = MongoClient(config["mongodb_uri"])
        db = client.get_database(config["database"])
        collection = db.get_collection(config["collection"])

        query = {}
        if metrics:
            query["metric_type"] = {"$in": metrics}

        data = list(collection.find(query))
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df

    except Exception as e:
        st.error(f"Error fetching historical data: {e}")
        return pd.DataFrame()

# ------ Real-time Streaming View ------
def display_real_time_view(refresh_interval, config):
    st.header("\U0001f4c8 Real-time Streaming Dashboard")
    st.info(f"Auto-refresh every {refresh_interval} seconds")
    with st.spinner("Fetching real-time data..."):
        real_time_data = query_historical_data(config=config)
    if not real_time_data.empty:
        st.write("Current columns:", list(real_time_data.columns))  # Debug line
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Records Received", len(real_time_data))
        with col2:
            if 'value' in real_time_data.columns and not real_time_data['value'].empty:
                st.metric("Latest Value", f"{real_time_data['value'].iloc[-1]:.2f}")
            else:
                st.warning("No 'value' column found in real-time data!")
                st.write(real_time_data.head())
        with col3:
            if 'timestamp' in real_time_data.columns and not real_time_data['timestamp'].empty:
                st.metric(
                    "Data Range",
                    f"{real_time_data['timestamp'].min().strftime('%H:%M')} - {real_time_data['timestamp'].max().strftime('%H:%M')}"
                )
            else:
                st.warning("No 'timestamp' column in your data!")
        if 'timestamp' in real_time_data.columns and 'value' in real_time_data.columns:
            fig = px.line(
                real_time_data,
                x='timestamp',
                y='value',
                title=f"Real-time Data Stream (Last {len(real_time_data)} records)",
                labels={'value': 'Sensor Value', 'timestamp': 'Time'},
                template='plotly_white'
            )
            st.plotly_chart(fig, use_container_width=True)
        with st.expander("\U0001f4cb View Raw Data"):
            # Prepare latest 100 records, sorted, and with 1-based top-down "No." column
            df_raw = df.sort_values('timestamp', ascending=False).head(100).sort_values('timestamp').reset_index(drop=True)

            # Insert the index/row "No." column as last-in = 100, first-in = 1
            df_raw.insert(0, "No.", range(1, len(df_raw) + 1))

            # Optionally remove Mongo _id if you don't want to show it
            if "_id" in df_raw.columns:
                df_raw = df_raw.drop("_id", axis=1)

            st.dataframe(df_raw, height=300)

# ------ Historical Data View ------
def display_historical_view(config):
    st.header("\U0001f4ca Historical Data Analysis")
    st.subheader("Data Filters")
    col1, col2 = st.columns(2)
    with col1:
        metric_type = st.selectbox(
            "Metric Type",
            ["temperature", "humidity", "pressure", "all"]
        )
    with col2:
        aggregation = st.selectbox(
            "Aggregation",
            ["raw", "hourly", "daily"]
        )

    # Query WITHOUT using 'metric_type' as a filter!
    historical_data = query_historical_data(config=config)

    if not historical_data.empty:
        # Select which metric to plot/show, default to all if 'all' is selected
        show_all = metric_type == "all"
        cols_to_show = ['timestamp']
        if show_all:
            cols_to_show += [col for col in ['temperature', 'humidity', 'pressure'] if col in historical_data.columns]
        else:
            if metric_type in historical_data.columns:
                cols_to_show.append(metric_type)
            else:
                st.warning(f"No data for '{metric_type}' in your records!")
        st.subheader("Historical Data Table")
        st.dataframe(
            historical_data[cols_to_show].sort_values('timestamp', ascending=False).reset_index(drop=True),
            hide_index=True
        )

        # Plot trend(s)
        if not show_all and metric_type in historical_data.columns:
            st.subheader("Historical Trends")
            fig = px.line(
                historical_data,
                x='timestamp',
                y=metric_type,
                title=f"{metric_type.capitalize()} Historical Trend"
            )
            st.plotly_chart(fig, use_container_width=True)
        elif show_all:
            st.subheader("Historical Trends (Temperature; use other metric types for specific plots)")
            if 'temperature' in historical_data.columns:
                fig = px.line(
                    historical_data,
                    x='timestamp',
                    y='temperature',
                    title="Temperature Historical Trend"
                )
                st.plotly_chart(fig, use_container_width=True)
        st.subheader("Data Summary")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Records", len(historical_data))
            st.metric(
                "Date Range",
                f"{historical_data['timestamp'].min().strftime('%Y-%m-%d')} to {historical_data['timestamp'].max().strftime('%Y-%m-%d')}"
            )
        with col2:
            if not show_all and metric_type in historical_data.columns:
                st.metric(
                    f"Average {metric_type.capitalize()}",
                    f"{historical_data[metric_type].mean():.2f}"
                )
                st.metric(
                    f"{metric_type.capitalize()} Variability",
                    f"{historical_data[metric_type].std():.2f}"
                )
    else:
        st.warning("No historical data available")


# Graph
def display_real_time_view(refresh_interval, config):
    st.header("\U0001f4c8 Real-time Streaming Dashboard")
    st.info(f"Auto-refresh every {refresh_interval} seconds")
    with st.spinner("Fetching real-time data..."):
        df = query_historical_data(config=config)

    if not df.empty:
        # Only last 100 records, sorted by timestamp
        df = df.sort_values('timestamp', ascending=False).head(100).sort_values('timestamp')

        st.write("Current columns:", list(df.columns))  # Debug line

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Records Received", len(df))
        with col2:
            if "temperature" in df.columns and not df["temperature"].empty:
                st.metric("Latest Temperature", f"{df['temperature'].iloc[-1]}")
            else:
                st.warning("No 'temperature' column found in real-time data!")
                st.write(df.head())
        with col3:
            if "timestamp" in df.columns and not df["timestamp"].empty:
                st.metric(
                    "Data Range",
                    f"{df['timestamp'].min().strftime('%H:%M')} - {df['timestamp'].max().strftime('%H:%M')}"
                )
            else:
                st.warning("No 'timestamp' column in your data!")

        # Plot temperature graph for last 100 records
        if "timestamp" in df.columns and "temperature" in df.columns:
            fig = px.line(
                df,
                x='timestamp',
                y='temperature',
                title=f"Temperature (Last {len(df)} records)",
                labels={'temperature': 'Temperature (°C)', 'timestamp': 'Time'},
                template='plotly_white'
            )
            st.plotly_chart(fig, use_container_width=True)

        with st.expander("\U0001f4cb View Raw Data"):
            st.dataframe(df.sort_values('timestamp', ascending=False), height=300)
    else:
        st.warning("No data available for real-time view!")


# ------ Main App ------
def main():
    st.title("\U0001f680 Streaming Data Dashboard")
    config = setup_sidebar()
    refresh_interval = st.sidebar.slider(
        "Refresh Interval (seconds)",
        min_value=5,
        max_value=60,
        value=15
    )
    st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    tab1, tab2 = st.tabs(["\U0001f4c8 Real-time Streaming", "\U0001f4ca Historical Data"])
    with tab1:
        display_real_time_view(refresh_interval, config)
    with tab2:
        display_historical_view(config)

if __name__ == "__main__":
    main()

