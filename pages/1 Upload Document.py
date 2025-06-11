import streamlit as st
from utils.file_utils import read_uploaded_file

st.header("📂 Step 1 — Upload a vendor file")

uploaded_file = st.file_uploader(
    "Upload a **CSV** or **Excel** file", type=["csv", "xlsx"], key="upload_widget"
)

if uploaded_file:
    with st.spinner("Reading file…"):
        try:
            df = read_uploaded_file(uploaded_file)
            st.session_state.uploaded_df = df
            st.session_state.actual_columns = list(df.columns)
            st.success(f"Loaded {len(df):,} rows — preview below 👇")
            st.dataframe(df.head(), use_container_width=True)
        except Exception as e:
            st.error(f"Upload failed: {e}")
            st.stop()

if st.button("Map Schema"):
    st.switch_page("pages/2 Map Schema.py")