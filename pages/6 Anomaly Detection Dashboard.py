from datetime import datetime
import streamlit as st
from streamlit_app import _get_session
from utils.dashboard_utils import load_anomalies

st.header("📊 Step 6 — Review anomalies & insights")

anomalies_df = load_anomalies(_get_session())

if anomalies_df.empty:
    st.info("No anomalies yet. Run the detector first.")
    st.stop()

# KPI cards
k1, k2, k3 = st.columns(3)
k1.metric("Total anomalies", len(anomalies_df))
k2.metric("Unique vendors", anomalies_df["vendor_id"].nunique())
k3.metric("Last refresh", datetime.now().strftime("%d‑%b %H:%M"))

# Filters
st.markdown("### Filters")
with st.container(border=True):
    col1, col2 = st.columns(2)
    with col1:
        vendor_filter = st.selectbox(
            "Vendor", ["All"] + sorted(anomalies_df["vendor_id"].dropna().unique())
        )
    with col2:
        type_filter = st.selectbox(
            "Anomaly type", ["All"] + sorted(anomalies_df["anomaly_type"].dropna().unique())
        )

filtered_df = anomalies_df.copy()
if vendor_filter != "All":
    filtered_df = filtered_df[filtered_df.vendor_id == vendor_filter]
if type_filter != "All":
    filtered_df = filtered_df[filtered_df.anomaly_type == type_filter]

st.dataframe(filtered_df, use_container_width=True)
st.bar_chart(filtered_df["anomaly_type"].value_counts())