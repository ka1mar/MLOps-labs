# Lab 4 — Apache Kafka Integration

> Academic project · ITMO University · MLOps course

Extends Lab 3 by adding Apache Kafka as a message bus. After each prediction run, the model service publishes a metadata message to a Kafka topic; a separate consumer service receives and logs it.

## What Was Done

- Implemented `KafkaProducer` (`src/kafka_producer.py`): publishes prediction metadata (timestamp, count, sample results) after each run
- Implemented `KafkaConsumer` (`src/kafka_consumer.py`): a standalone container that subscribes to the topic
- Added Kafka and Zookeeper containers to `docker-compose.yml`; Vault and Greenplum integration retained from Lab 3

The full Docker Compose stack runs seven services: `train_model`, `web_app`, `greenplum`, `greenplum-setup`, `vault`, `kafka`, `zookeeper`, and `prediction-consumer`.

## Running Locally

```bash
docker-compose up
```

## Links

- GitHub: https://github.com/ka1mar/MLOps-lab4
- DockerHub: https://hub.docker.com/repository/docker/ka1mar/mlops-lab4
