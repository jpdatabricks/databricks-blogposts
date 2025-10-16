# Real-Time Streaming Feature Engineering with Lakebase PostgreSQL

> **📌 PROJECT CONVENTION:** "Lakebase" in this project ALWAYS refers to **Databricks Lakebase PostgreSQL** (OLTP database, port 5432), NOT Delta Lake. See [`PROJECT_CONVENTIONS.md`](PROJECT_CONVENTIONS.md) for details.

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

### 1. Read Conventions
```bash
cat PROJECT_CONVENTIONS.md  # Understand terminology
```

### 2. Provision Lakebase
```
Databricks Workspace:
  → Compute → OLTP Database → Create instance
  → Name: feature_store
  → Size: Small (start)
```

### 3. Configure Connection
```python
# Update in 00_setup_and_configuration.ipynb
host = "your-workspace.cloud.databricks.com"
```

### 4. Run Setup
```bash
# Run notebook
00_setup_and_configuration.ipynb
```

### 5. Run Demo
```bash
# Run streaming demo
01_streaming_lakebase_demo.ipynb
```

## File Structure

```
project/
├── PROJECT_CONVENTIONS.md          ← START HERE (terminology)
├── LAKEBASE_POSTGRESQL_SETUP.md    ← Setup guide
├── lakebase_client.py              ← PostgreSQL client
├── data_generator.py               ← Streaming data
├── feature_engineering.py          ← Feature logic
├── 00_setup_and_configuration.ipynb ← Initial setup
└── 01_streaming_lakebase_demo.ipynb ← End-to-end demo
```

## Documentation

1. **[PROJECT_CONVENTIONS.md](PROJECT_CONVENTIONS.md)** - Terminology (read first!)
2. **[LAKEBASE_POSTGRESQL_SETUP.md](LAKEBASE_POSTGRESQL_SETUP.md)** - Complete setup guide
3. **[lakebase_connection_guide.md](lakebase_connection_guide.md)** - Connection patterns
4. **[MIGRATION_SUMMARY.md](MIGRATION_SUMMARY.md)** - Architecture decisions

## Key Features

- ✅ Real-time streaming feature engineering
- ✅ <10ms query latency (Lakebase PostgreSQL)
- ✅ ACID transactions for consistency
- ✅ Standard SQL interface
- ✅ Production-ready error handling
- ✅ Connection pooling support

## Usage Example

```python
from lakebase_client import LakebaseClient
from data_generator import TransactionDataGenerator
from feature_engineering import AdvancedFeatureEngineering

# Connect to Lakebase PostgreSQL
lakebase = LakebaseClient(
    host="workspace.cloud.databricks.com",
    port=5432,
    database="feature_store"
)

# Generate streaming data
generator = TransactionDataGenerator()
streaming_df = generator.generate_transaction_data(rows_per_second=10)

# Apply features
feature_engineer = AdvancedFeatureEngineering()
df_with_features = feature_engineer.apply_all_features(streaming_df)

# Write to Lakebase PostgreSQL
query = feature_engineer.write_features_to_lakebase(
    df=df_with_features,
    lakebase_client=lakebase
)

# Query features (real-time serving)
features = lakebase.read_features("""
    SELECT * FROM transaction_features
    WHERE user_id = 'user_123'
    AND timestamp > NOW() - INTERVAL '1 hour'
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

❌ Delta Lake for feature storage
❌ Databricks Feature Store API
❌ File-based storage paths
❌ Batch-only processing

## What This Project DOES Use

✅ Lakebase PostgreSQL (OLTP)
✅ Streaming feature engineering
✅ Real-time queries (<10ms)
✅ SQL-based feature serving

## Support

- [Databricks Lakebase Docs](https://docs.databricks.com/lakebase/)
- [PostgreSQL Docs](https://www.postgresql.org/docs/)
- [Project Conventions](PROJECT_CONVENTIONS.md)

## License

MIT License

---

**Remember:** In this project, "Lakebase" = Lakebase PostgreSQL = OLTP database at port 5432
