# Lab 8 — Kubernetes Migration

> Academic project · ITMO University · MLOps course

Migrates the three-service stack from Labs 5–7 (PySpark clustering model, ClickHouse data source, Scala data mart) from Docker Compose to Kubernetes.

## What Was Done

- Configured Spark to run in cluster mode on k8s with executor replication
- Deployed services incrementally: model (Lab 5 version) → added ClickHouse (Lab 6) → added Scala data mart (Lab 7); verified end-to-end execution after each step
- Tuned CPU and memory requests/limits per service
- Credentials managed as Kubernetes Secrets

**Tech:** Kubernetes, Apache Spark on k8s, ClickHouse, MySQL, Scala, PySpark
