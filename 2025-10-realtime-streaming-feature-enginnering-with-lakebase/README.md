# Real-Time Streaming Feature Engineering with Lakebase PostgreSQL

> **📌 PROJECT CONVENTION:** "Lakebase" in this project ALWAYS refers to **Databricks Lakebase PostgreSQL** (OLTP database, port 5432), NOT Delta Lake.

## Overview

This project implements a real-time streaming feature engineering pipeline that writes features to Databricks Lakebase PostgreSQL for ultra-low-latency serving (<10ms).

## Architecture

```
Transaction Stream → Feature Engineering → Lakebase PostgreSQL → Real-time ML Serving
                                           (port 5432, <10ms)
```

## Key Technologies

- **PySpark Structured Streaming** - Stream processing
- **Lakebase PostgreSQL** - Feature storage (OLTP, <10ms queries)
- **psycopg2** - PostgreSQL connectivity
- **Databricks Runtime** - Execution environment

## Quick Start

### 1. Provision Lakebase
```
Databricks Workspace:
  → Compute → OLTP Database → Create instance
  → Name: your-instance-name
  → Size: Small (start)
```

### 2. Configure Connection
```python
# Update in 00_setup.ipynb
LAKEBASE_CONFIG = {
    "instance_name": "your-instance-name",
    "database": "databricks_postgres"
}
```

### 3. Run Setup
```bash
# Run notebook
00_setup.ipynb
```

### 4. Run Demo
```bash
# Run streaming feature engineering demo
streaming_fraud_detection_pipeline.ipynb
```

## File Structure

```
project/
├── README.md                                      ← This file
├── 00_setup.ipynb                                 ← Initial setup and table creation
├── 01_streaming_fraud_detection_pipeline.ipynb    ← End-to-end streaming pipeline
└── utils/
    ├── lakebase_client.py                         ← Lakebase PostgreSQL client
    ├── data_generator.py                          ← Streaming data generator
    └── feature_engineering.py                     ← Feature engineering (stateless + stateful)
```

## Documentation

1. **[00_setup.ipynb](00_setup.ipynb)** - Setup and configuration guide
2. **[01_streaming_fraud_detection_pipeline.ipynb](01_streaming_fraud_detection_pipeline.ipynb)** - End-to-end pipeline demo

## Key Features

- Real-time streaming feature engineering (PySpark Structured Streaming)
- <10ms query latency (Lakebase PostgreSQL OLTP)
- ACID transactions for data consistency
- Standard SQL interface for feature serving
- Stateless + stateful features (transformWithStateInPandas)
- Production-ready error handling and retry logic
- Unified table schema (~70+ columns)

## Usage Example

```python
from utils.lakebase_client import LakebaseClient
from utils.data_generator import TransactionDataGenerator
from utils.feature_engineering import AdvancedFeatureEngineering

# Connect to Lakebase PostgreSQL
lakebase = LakebaseClient(
    instance_name="your-instance-name",
    database="databricks_postgres"
)

# Generate streaming data
generator = TransactionDataGenerator()
streaming_df = generator.generate_transaction_data(
    num_users=20,
    num_merchants=50,
    rows_per_second=10
)

# Apply stateless features
feature_engineer = AdvancedFeatureEngineering()
df_with_features = feature_engineer.apply_all_features(streaming_df)

# Write to Lakebase PostgreSQL using foreachBatch
query = df_with_features.writeStream \
    .foreachBatch(lakebase.write_streaming_batch) \
    .start()

# Query features (real-time serving)
features = lakebase.read_features("""
    SELECT * FROM transaction_features
    WHERE user_id = 'user_000001'
    AND timestamp > NOW() - INTERVAL '1 hour'
    ORDER BY timestamp DESC
    LIMIT 100
""")
```

## Performance

| Metric | Value |
|--------|-------|
| Write Latency | 50-100ms |
| Query Latency | <10ms |
| Concurrency | High |
| Use Case | Real-time ML serving |

## Prerequisites

- Databricks Runtime 13.0+ with ML
- Lakebase PostgreSQL instance (provisioned)
- Python 3.9+
- psycopg2-binary

## Installation

```bash
# In Databricks notebook
%pip install psycopg2-binary
```

## What This Project Does NOT Use

- Delta Lake for feature storage (uses Lakebase PostgreSQL instead)
- Databricks Feature Store API (uses direct PostgreSQL connections)
- File-based storage paths (uses OLTP database)
- Batch-only processing (100% streaming-compatible code)

## What This Project DOES Use

- Lakebase PostgreSQL (OLTP database, port 5432)
- PySpark Structured Streaming (100% streaming)
- transformWithStateInPandas for stateful processing (Spark 4.0+)
- Real-time queries with <10ms latency
- SQL-based feature serving
- Unified table schema (transaction_features)

## Support

- [Databricks Lakebase Docs](https://docs.databricks.com/en/lakehouse-architecture/lakebase/index.html)
- [PostgreSQL Docs](https://www.postgresql.org/docs/)
- [PySpark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

## License

MIT License

---

**Remember:** In this project, "Lakebase" = Lakebase PostgreSQL = OLTP database at port 5432
