# MLOps Lab Works

> Academic project · ITMO University · MLOps course

Eight labs covering the full MLOps stack: from a baseline CI/CD pipeline to a Kubernetes-orchestrated multi-service system with secrets management, a message queue, and distributed data processing.

## Labs

| Lab | Topic | Key additions |
|---|---|---|
| [Lab 1](MLOps-lab1/) | CI/CD Pipeline | CatBoost model, Flask API, Docker, DVC, GitHub Actions CI/CD |
| [Lab 2](MLOps-lab2/) | Database Integration | Greenplum as data source and prediction sink |
| [Lab 3](MLOps-lab3/) | Secrets Management | HashiCorp Vault KV store; DB credentials fetched at runtime |
| [Lab 4](MLOps-lab4/) | Apache Kafka | Kafka Producer/Consumer for prediction metadata |
| [Lab 5](MLOps-lab5/) | PySpark Clustering | K-Means on Open Food Facts; column filtering, imputation, scaling |
| [Lab 6](MLOps-lab6/) | PySpark Data Sources | ClickHouse as I/O via JDBC; replaces CSV file access |
| [Lab 7](MLOps-lab7/) | Spark Data Mart | Scala data mart intermediary; model decoupled from direct DB access |
| [Lab 8](MLOps-lab8/) | Kubernetes Migration | Full Spark service stack deployed to k8s with replication |

---

## Labs 1–4: ML Service with CI/CD

**Dataset:** Wheat Seeds (UCI) — 210 samples, 7 geometric features, 3 classes.  
**Model:** CatBoost classifier. Hyperparameters stored in `config.ini`.  
**API:** Flask `POST /predict` endpoint.

Each lab is a fork of the previous one, incrementally adding infrastructure:

- **Lab 1** establishes the foundation: DVC-tracked data splits, unit and smoke tests, Dockerised service, and a GitHub Actions pipeline that builds and pushes the image to DockerHub on every PR to `main`.
- **Lab 2** integrates Greenplum: training data is loaded from the database and prediction results are written back. Credentials are injected via environment variables only.
- **Lab 3** replaces environment-variable injection with HashiCorp Vault: a KV secrets engine is seeded at container startup; the model service retrieves credentials via the Vault HTTP API.
- **Lab 4** adds Apache Kafka: a `KafkaProducer` publishes prediction metadata after each run; a dedicated `prediction-consumer` container subscribes to the topic. The full stack runs seven services in Docker Compose: model, API, Greenplum, Vault, Kafka, Zookeeper, and consumer.

---

## Labs 5–8: Distributed ML with PySpark

**Dataset:** [Open Food Facts](https://world.openfoodfacts.org/data) — large-scale food product database.  
**Model:** PySpark MLlib K-Means (k=11).

- **Lab 5** implements `AutoClusteringPipeline`: reads a TSV file, drops low-quality columns, imputes missing values, scales features with `StandardScaler`, trains K-Means, and writes results to CSV.
- **Lab 6** replaces file I/O with ClickHouse: a `ClickHouseConnector` wraps Spark JDBC reads and writes; input and output tables are passed as CLI arguments.
- **Lab 7** introduces a Scala data mart (`com.foodfacts.datamart.DataMart`) that handles data fetching and preprocessing from MySQL. The Python pipeline reads a Spark SQL view exposed by the data mart and no longer accesses the database directly.
- **Lab 8** migrates the entire three-tier stack to Kubernetes: Spark runs in cluster mode, each service is deployed and validated incrementally, and resource requests/limits are tuned.

---

**Tech:** CatBoost, Flask, Docker, DVC, GitHub Actions, Greenplum, HashiCorp Vault, Apache Kafka, PySpark MLlib, ClickHouse, Scala, Kubernetes
