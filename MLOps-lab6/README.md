# Lab 6 — PySpark Data Sources (ClickHouse)

> Academic project · ITMO University · MLOps course

Extends Lab 5 by replacing CSV file I/O with ClickHouse. Input data is read from a ClickHouse table via Spark JDBC; clustering results are written back to another table on completion.

## What Was Done

- Implemented `ClickHouseConnector`: wraps Spark JDBC reads and writes using `com.clickhouse:clickhouse-jdbc:0.4.6`
- Pipeline now takes `--input_table` and `--output_table` instead of file paths
- ClickHouse connection parameters injected via environment variables (`CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`)

## Running

```bash
python scripts/clustering.py \
  --input_table food_data \
  --output_table clustering_results \
  --output /path/to/model_output
```

**Tech:** PySpark MLlib, ClickHouse JDBC
