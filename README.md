# 🤖 AIVA – AI For Value and Advice

AIVA is a Snowflake-native finance automation platform that detects inconsistencies, mismatches, and anomalies across Accounts Payable (AP) and Accounts Receivable (AR) workflows — powered by Streamlit, Snowpark ML, and Cortex.

> Automate PO matching, invoice validation, payment reconciliation, and early payment discount recommendations — natively in your Snowflake environment.

---

## ✨ Features

- 📤 Upload vendor invoice/PO/payment files (CSV/Excel)
- 🧠 Auto-schema detection and dynamic column mapping
- 🔁 Reusable per-vendor header mapping
- 🧾 Reconciliation via SQL + Snowpark
- 🤖 ML-based anomaly detection (e.g., overbilling, duplicates)
- 💰 Early payment discount insights
- 📊 Real-time dashboard with filters & charts
- 💬 Query-based assistance using Snowflake Cortex

---

## 🧱 Architecture

```text
[User] → [Streamlit UI] 
       → Header Detection → Column Mapping 
       → Data Load → Snowflake Tables
       → Reconciliation + ML Anomaly Detection
       → Dashboards / Query Assist via Cortex
```



