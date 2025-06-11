import pandas as pd

def read_uploaded_file(uploaded_file):
    """
    Reads an uploaded file (CSV or Excel) and returns a Pandas DataFrame.
    """
    if uploaded_file.name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    elif uploaded_file.name.endswith(".xlsx"):
        return pd.read_excel(uploaded_file, engine="openpyxl")
    else:
        raise ValueError("Unsupported file format. Please upload CSV or Excel.")

