# 🚀 Real-Time Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.4-orange?logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.4-blue)
![Azure Synapse](https://img.shields.io/badge/Azure%20Synapse-Analytics-0078D4?logo=microsoftazure)
![SQL](https://img.shields.io/badge/SQL-Advanced-lightgrey?logo=postgresql)
![License](https://img.shields.io/badge/License-MIT-green)

A production-ready **end-to-end data engineering pipeline** that ingests raw e-commerce event data, transforms it using **PySpark**, stores it in **Delta Lake** (Bronze → Silver → Gold architecture), and serves aggregated analytics via **Azure Synapse Analytics**.

---

## 📐 Architecture

```
Raw Events (JSON/CSV)
        │
        ▼
  ┌─────────────┐
  │  Ingestion  │  ← Python + PySpark Structured Streaming
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │   Bronze    │  ← Raw Delta Lake Table (append-only)
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │   Silver    │  ← Cleaned, deduplicated, schema-enforced
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │    Gold     │  ← Aggregated business metrics
  └──────┬──────┘
         │
         ▼
  Azure Synapse Analytics (SQL Queries / BI Dashboards)
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, PySpark Structured Streaming |
| Storage | Delta Lake (Bronze / Silver / Gold) |
| Processing | Apache Spark 3.4, PySpark |
| Cloud Analytics | Azure Synapse Analytics |
| Orchestration | Python scripts (extendable to ADF/Airflow) |
| Language | Python 3.10, SQL |
| Version Control | Git / GitHub |

---

## 📁 Project Structure

```
realtime-data-pipeline-spark-delta/
├── data/
│   └── sample_events.json          # Sample raw e-commerce events
├── src/
│   ├── ingestion/
│   │   └── ingest_raw_data.py       # Reads raw JSON → Bronze Delta table
│   ├── transformation/
│   │   ├── bronze_to_silver.py      # Cleans & deduplicates data
│   │   └── silver_to_gold.py        # Aggregates business KPIs
│   ├── synapse/
│   │   └── synapse_queries.sql      # Azure Synapse analytical queries
│   └── utils/
│       └── spark_session.py         # Reusable SparkSession factory
├── tests/
│   └── test_transformations.py      # Unit tests with pytest
├── notebooks/
│   └── pipeline_demo.ipynb          # Jupyter walkthrough of the pipeline
├── requirements.txt
└── README.md
```

---

## ⚡ Quickstart

```bash
# 1. Clone the repo
git clone https://github.com/archana24v-web/realtime-data-pipeline-spark-delta.git
cd realtime-data-pipeline-spark-delta

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run ingestion (Bronze layer)
python src/ingestion/ingest_raw_data.py

# 4. Transform Bronze → Silver
python src/transformation/bronze_to_silver.py

# 5. Aggregate Silver → Gold
python src/transformation/silver_to_gold.py
```

---

## 📊 Key Features

- ✅ **Medallion Architecture** (Bronze → Silver → Gold) with Delta Lake
- ✅ **Schema enforcement** and data quality checks
- ✅ **Deduplication** using Delta Lake MERGE operations
- ✅ **PySpark Structured Streaming** for real-time ingestion simulation
- ✅ **Azure Synapse** SQL queries for downstream analytics
- ✅ **Reusable SparkSession** factory pattern
- ✅ **Unit tested** with pytest

---

## 👩‍💻 Author

**Ashok** — MS Computer Science @ Auburn University at Montgomery  
Data Engineer | Python | PySpark | Delta Lake | Azure Synapse | SQL  
📍 Montgomery, Alabama  
🔗 [GitHub](https://github.com/archana24v-web)

---

## 📄 License

MIT License — feel free to use and adapt.
