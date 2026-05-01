# Lab 7 — Spark Data Mart (Scala)

> Academic project · ITMO University · MLOps course

Extends Lab 6 by introducing a Scala data mart as an intermediary layer. The Python pipeline no longer accesses the database directly — it reads a preprocessed Spark SQL view exposed by the data mart.

## What Was Done

- Implemented `com.foodfacts.datamart.DataMart` in Scala: fetches raw data from MySQL, applies column filtering and imputation, and registers the result as a Spark SQL view (`{table}_processed`)
- `AutoClusteringPipeline` calls `data_mart.readProcessedData(input_table)` and queries the view — all preprocessing is now handled on the Scala side
- MySQL JDBC connector (`mysql-connector-java-8.0.33.jar`) added to the Spark classpath

## Running

```bash
python scripts/clustering.py \
  --input_table food_data \
  --output_table clustering_results \
  --output /path/to/model_output
```

Requires the compiled data mart JAR on the Spark classpath.

**Tech:** PySpark MLlib, Scala, MySQL JDBC
