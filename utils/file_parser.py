import pandas as pd
from PyPDF2 import PdfReader

def parse_file(uploaded_file):
    name = uploaded_file.name.lower()

    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    elif name.endswith((".xls", ".xlsx")):
        return pd.read_excel(uploaded_file)
    elif name.endswith(".pdf"):
        reader = PdfReader(uploaded_file)
        text = "\n".join([page.extract_text() or "" for page in reader.pages])
        return pd.DataFrame({'raw_text': [text]})
    else:
        raise ValueError("Unsupported file format")
