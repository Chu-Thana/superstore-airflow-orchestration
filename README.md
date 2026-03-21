# 🛠 Airflow ETL Orchestration Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow-orange)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Pandas](https://img.shields.io/badge/Data-Pandas-yellow)
![Alerting](https://img.shields.io/badge/Alerts-Slack-red)

A production-style batch data pipeline orchestrated using Apache Airflow, featuring modular ETL processing, fault tolerance, and real-time alerting via Slack Webhooks.

---

# 🧠 Design Goals

This project simulates a **production-grade batch data pipeline** used in modern data platforms.

Key objectives:

* Orchestrate ETL workflows using Apache Airflow
* Design modular and reusable data processing components
* Handle failures with retry and alert mechanisms
* Implement observability through logging and alerts
* Structure pipeline for containerized deployment
* Integrate with upstream streaming concepts (Kafka)

---

# 🚀 Tech Stack

* Python 3.12
* Apache Airflow
* Pandas
* Docker & Docker Compose
* PostgreSQL (Airflow metadata DB)
* Slack Webhook (Alerting)
* Git

---

# Key Concepts Demonstrated

This project demonstrates core data engineering concepts:

* Workflow orchestration
* Batch ETL processing
* Task dependency management
* Retry & failure handling
* Observability & alerting
* Containerized pipelines
* Modular pipeline design

---

# 📂 Project Structure

```text
project4_airflow_orchestration/
│
├── dags/
│   ├── sales_etl_pipeline.py
│   └── project1_etl_runner.py
│
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── notify.py
│
├── data/
├── logs/
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .gitignore
```

---

# 🏗 Architecture Overview

This pipeline orchestrates batch processing while conceptually integrating with upstream streaming systems.

```text
(Project 3 - Streaming Layer)
Kafka → Consumer → Raw Data (Parquet / DB)

                ↓

(Project 4 - Orchestration Layer)
Airflow DAG

                ↓

(Project 1 - Processing Logic)
Extract → Transform → Load → Summary Output
```

---

# 🔄 Pipeline Flow

## 1️⃣ Extract

* Reads raw data from CSV
* Converts to Parquet format
* Handles encoding fallback

---

## 2️⃣ Transform

* Parses and validates date columns
* Handles invalid values using coercion
* Aggregates:

  * Total sales by category
  * Order count by category

---

## 3️⃣ Load

* Writes aggregated results to CSV
* Outputs final dataset for downstream usage

---

# 📊 DAG Structure

```text
extract_sales_data
        ↓
transform_sales_data
        ↓
load_sales_summary
```

Airflow manages task dependencies and execution order.

---

# 🔁 Fault Tolerance

This pipeline includes:

* Retry mechanism for failed tasks
* Graceful error handling
* Idempotent processing design

Example:

* Temporary failure → automatic retry
* Persistent failure → alert triggered

---

# 🚨 Alerting System

Integrated Slack alerting using Airflow callbacks.

### On Failure

```text
❌ FAILURE: Task transform_sales_data in DAG sales_etl_pipeline failed
```

### On Success

```text
✅ SUCCESS: DAG sales_etl_pipeline completed
```

This enables **real-time monitoring of pipeline health**.

---

# 📊 Observability

The system provides visibility through:

* Airflow logs
* Task-level execution history
* Slack alerts
* Structured print/log messages

This simulates production monitoring without external tools.

---

# 🐳 Running the Pipeline

Start services:

```bash
docker compose up -d
```

Access Airflow UI:

```
http://localhost:8080
```

Trigger DAG manually via UI.

---

# 📐 Key Design Decisions

* Modular ETL scripts for separation of concerns
* Airflow used as orchestration layer only
* Slack used for lightweight alerting
* Parquet used as intermediate storage format
* Docker ensures environment consistency

---

# 🔮 Future Improvements

* Replace local storage with data warehouse (BigQuery / Snowflake)
* Add data validation framework (Great Expectations)
* Integrate Prometheus & Grafana
* Add CI/CD pipeline
* Deploy to cloud (AWS / GCP)

---

# 🏁 Portfolio Context

Part of a **Data Engineering learning portfolio**:

```text
Project 1 → Batch ETL
Project 2 → Analytics API
Project 3 → Streaming Pipeline (Kafka) :contentReference[oaicite:0]{index=0}
Project 4 → Orchestration (Airflow)
Project 5 → Cloud Deployment (future)
```

This project demonstrates **workflow orchestration and production-style pipeline design**.
