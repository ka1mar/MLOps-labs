# Lab 2 — Database Integration

> Academic project · ITMO University · MLOps course

Extends Lab 1 by integrating Greenplum as the data source and prediction sink. Connection parameters are read from environment variables — no credentials in source or config files.

## What Was Done

- Implemented `DatabaseOperator` (`src/db_operations.py`): creates tables, loads train/test data, and stores prediction results in Greenplum
- Three tables: `train_data`, `test_data`, `model_predictions`
- Added `test_db.py` unit tests for database operations
- Extended `docker-compose.yml` with a Greenplum container and an init container that creates the database and user before the model runs

## Running Locally

```bash
docker-compose up
```

## Links

- DockerHub: https://hub.docker.com/repository/docker/ka1mar/mlops-lab2
