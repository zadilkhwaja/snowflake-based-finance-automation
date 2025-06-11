import streamlit as st
from streamlit_app import _get_session
from utils.anomaly_detector import detect_payment_anomalies

st.header("🔍 Step 5 — Reconciliation & anomaly detection")

if st.button("Run reconciliation + ML anomaly detector"):
    with st.spinner("Crunching numbers with Snowpark ML …"):
        try:
            detect_payment_anomalies(_get_session())
            st.success("Finished! View results under *Dashboard* →")
        except Exception as e:
            st.error(f"Detection failed: {e}")

st.info("Tip: the detector uses synthetic thresholds for the demo. Tweak in `utils/anomaly_detector.py`.")

if st.button("Anomaly Dashboard"):
    st.switch_page("pages/6 Anomaly Detection Dashboard.py")