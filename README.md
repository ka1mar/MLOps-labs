# Lab 5 — PySpark Clustering

> Academic project · ITMO University · MLOps course

K-Means clustering on the [Open Food Facts](https://world.openfoodfacts.org/data) dataset using PySpark MLlib.

## What Was Done

Implemented `AutoClusteringPipeline` (`scripts/clustering.py`) with the following steps: read TSV with schema inference → drop numeric columns with >30% missing values or <30% unique-value ratio → impute remaining nulls with column mean → assemble and scale features (`VectorAssembler` + `StandardScaler`) → train `KMeans(k=11)` → write cluster assignments to CSV and save the model.

Spark resource parameters (`executor cores/memory`, `driver memory`, `parallelism`, `shuffle partitions`) are configured via environment variables.

## Running

```bash
python scripts/clustering.py \
  --input /path/to/openfoodfacts.csv \
  --output /path/to/output
```

**Tech:** PySpark MLlib (KMeans, StandardScaler, VectorAssembler, Imputer)
