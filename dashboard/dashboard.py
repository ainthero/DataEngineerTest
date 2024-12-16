import os
import redis
import streamlit as st
import pandas as pd
import json

# Redis Configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

# Streamlit Configuration
st.set_page_config(
    page_title="Portugal Real Estate Dashboard",
    page_icon="🏠",
    layout="wide"
)

st.title("🏠 **Portugal Real Estate Dashboard**")

# Cache data fetching
@st.cache_data(ttl=30)
def get_global_metrics(_redis_conn):
    raw_data = _redis_conn.get("global_metrics")
    if raw_data:
        return json.loads(raw_data)
    return {}

@st.cache_data(ttl=30)
def get_grouped_data(_redis_conn):
    keys = _redis_conn.keys()
    data = []
    for key in keys:
        if b":" in key:
            record = _redis_conn.get(key)
            if record:
                data.append({**{"Key": key.decode("utf-8")}, **json.loads(record)})
    return data

# Fetching data
global_metrics = get_global_metrics(r)
grouped_data = get_grouped_data(r)

# Display global metrics
if global_metrics:
    st.subheader("📊 **Global Metrics**")
    col1, col2, col3, col4 = st.columns(4)
    total_listings = int(global_metrics["GlobalTotalListings"])
    avg_price = int(global_metrics["GlobalAveragePrice"])
    avg_area = int(global_metrics["GlobalAverageArea"])
    avg_price_per_m2 = int(global_metrics["GlobalAveragePricePerSquareMeter"])

    col1.metric("🏘️ Total Listings", f"{total_listings}")
    col2.metric("💰 Avg. Price", f"{avg_price} €")
    col3.metric("📏 Avg. Area", f"{avg_area} m²")
    col4.metric("📈 Price per m²", f"{avg_price_per_m2} €/m²")
else:
    st.warning("⚠️ No global metrics available.")

# Process grouped data
if grouped_data:
    df = pd.DataFrame(grouped_data)
    df["PropertyType"], df["District"] = zip(*df["Key"].str.split(":"))
    df.drop(columns=["Key"], inplace=True)

    # Add filter section
    st.subheader("🔍 **Filter Options**")
    col1, col2 = st.columns(2)

    # Dropdown checklist for property types
    with col1:
        selected_property_types = st.multiselect(
            "🏘️ Select Property Types",
            options=df["PropertyType"].unique(),
            default=df["PropertyType"].unique()
        )

    # Dropdown checklist for districts
    with col2:
        selected_districts = st.multiselect(
            "📍 Select Districts",
            options=df["District"].unique(),
            default=df["District"].unique()
        )

    # Apply filters
    filtered_df = df[
        (df["PropertyType"].isin(selected_property_types)) &
        (df["District"].isin(selected_districts))
    ]

    # Display filtered data
    st.subheader("📋 **Detailed Data View**")
    st.dataframe(
        filtered_df.style.format({
            "AveragePrice": "{:.1f}",
            "AverageArea": "{:.1f}",
            "Price per m² (€)": "{:.1f}",
            "TotalListings": "{:.0f}"
        }),
        use_container_width=True
    )
else:
    st.warning("⚠️ No grouped data available.")
