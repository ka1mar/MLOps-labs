# Lab 3 — Secrets Management with HashiCorp Vault

> Academic project · ITMO University · MLOps course

Extends Lab 2 by replacing environment-variable credential injection with HashiCorp Vault. The model service fetches database credentials from Vault at startup — no credentials exist in config files, environment, or source code.

## What Was Done

- Added a Vault container (dev mode) to `docker-compose.yml`
- `vault_setup.sh` enables the KV secrets engine at `db/credentials` and writes connection details on first startup
- `db_operations.py` fetches credentials via the Vault HTTP API (`GET /v1/db/credentials`) using a root token passed as `VAULT_DEV_ROOT_TOKEN_ID`
- Removed all local config files that previously contained credentials

## Running Locally

```bash
docker-compose up
```

## Links

- GitHub: https://github.com/ka1mar/MLOps-lab3
- DockerHub: https://hub.docker.com/repository/docker/ka1mar/mlops-lab3
