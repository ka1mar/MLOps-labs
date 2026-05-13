# Lab 1 — CI/CD Pipeline for an ML Model

> Academic project · ITMO University · MLOps course

A CatBoost classifier for Wheat Seeds (UCI, 210 samples, 7 features, 3 classes) wrapped in a Flask REST API, containerised with Docker, and shipped via a GitHub Actions CI/CD pipeline.

## What Was Done

- Trained a CatBoost classifier; hyperparameters in `config.ini`, data splits tracked with DVC
- Exposed predictions via a Flask `POST /predict` endpoint
- Wrote unit tests (`unittest` + `coverage`) and smoke tests
- Configured CI (GitHub Actions): builds a multi-arch Docker image and pushes it to DockerHub on every PR to `main`
- Configured CD: pulls the image, starts the container via `docker-compose`, runs smoke tests

## Running Locally

```bash
docker-compose up train_model   # preprocess → train → predict → unit tests
docker-compose up web_app       # Flask API on :5000
```

## Links

- DockerHub: https://hub.docker.com/repository/docker/ka1mar/mlops-lab1
