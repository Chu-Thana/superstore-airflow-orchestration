# 🛠 Airflow ETL Orchestration Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow-orange)
![Docker](https://img.shields.io/badge/Container-Docker-blue)
![AWS](https://img.shields.io/badge/Storage-S3-yellow)
![Alerting](https://img.shields.io/badge/Alert-Telegram-green)

Production-style **end-to-end data pipeline orchestration** using Apache Airflow with AWS S3, Redshift, and real-time alerting.

---

# 🚀 Overview

This project demonstrates a **production-grade Airflow pipeline** orchestrating ETL workflows on top of a modern data lake architecture.

---

## 🔑 Highlights

- End-to-end ETL orchestration using Airflow DAGs  
- Medallion architecture (raw → silver → gold)  
- AWS S3 + Redshift integration  
- Fault-tolerant pipeline with retries  
- Real-time alerting via Telegram  
- Business event monitoring from streaming data  
- Full observability (logs + DAG monitoring)

---

# 🧠 Architecture

![Data Lake](assets/airflow_s3_lakehouse.png)

```
Kafka (Project 3)
      ↓
S3 (raw)
      ↓
Airflow DAG (Project 4)
      ↓
ETL Processing
      ↓
S3 (silver / gold)
      ↓
Redshift (analytics-ready)
```

---

# 📊 Pipeline Flow

## 1️⃣ Extract
- Reads staging data from local / S3  
- Handles encoding issues & schema normalization  
- Outputs clean intermediate dataset  

## 2️⃣ Transform
- Validates timestamps  
- Handles null / invalid values  
- Aggregates business metrics (sales, profit, category insights)  

## 3️⃣ Load
- Writes transformed data to S3 (silver / gold)  
- Loads analytics-ready data into Redshift  

![Redshift](assets/airflow_redshift_pipeline.png)

---

# 🧩 DAG Structure

```
extract_staging_sales
        ↓
transform_staging_sales
        ↓
load_staging_sales_summary
```

---

# ⚙️ Airflow Execution

## ✅ Successful DAG Run
![DAG Success](assets/airflow_dag_success.png)

## 📜 Logs & Debugging
![Logs](assets/airflow_dag_logs.png)

---

# 🚨 Production Alerts & Observability

## ❌ Failure Detection (Airflow)

![Failure Alert](assets/airflow_alert_failure.png)

## ✅ Success Notification

![Success Alert](assets/airflow_alert_success.png)

---

# 📊 Business Event Alerts (Streaming)

The system also detects **real-time business anomalies** from streaming data:

- 🚨 High-value transactions  
- ⚠️ Risky discount-profit scenarios  
- 📉 Negative profit events  

![Streaming Alerts](assets/streaming_alert_events.png)

### 🔗 Integration Flow

```
Kafka → Python Consumer → Rule Detection → Telegram Alerts
```

---

# 🔁 Reliability & Fault Tolerance

- Retry mechanism for transient failures  
- Task-level failure isolation  
- Alerting on both success & failure  
- End-to-end observability via logs + monitoring  

---

# ☁️ Data Lake (AWS S3)

```
s3://sales-analytics-lakehouse-thana/

├── raw/
├── silver/
└── gold/
```

---

# 🐳 Running the Project

```bash
docker compose up -d
```

Airflow UI: http://localhost:8080

---

# 🧠 Key Concepts Demonstrated

- Airflow DAG orchestration  
- Medallion architecture  
- ETL modular design  
- Streaming + batch integration  
- Observability & alerting  
- Production-ready pipeline design  

---

# 🏁 Portfolio Context

| Project | Description |
|--------|------------|
| Project 1 | Batch ETL |
| Project 2 | FastAPI Analytics |
| Project 3 | Kafka Streaming |
| Project 4 | Airflow Orchestration |

---

# 💡 Key Takeaway

This project demonstrates how to build a **reliable, observable, and production-ready data pipeline**, combining orchestration, storage, streaming, and alerting into a unified system.
