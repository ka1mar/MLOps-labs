#!/bin/bash

set -e

kubectl delete -f k8s-job.yml --ignore-not-found
kubectl delete -f k8s-setup.yml --ignore-not-found
kubectl delete -f k8s-config.yml --ignore-not-found

sleep 10

echo "Building Docker images..."
echo "Building data-init image..."
docker build -f Dockerfile.data-init -t data-init:latest .

echo "Building clustering-app image..."
docker build -f Dockerfile -t clustering-app:latest .

echo "Docker images built successfully!"

kubectl apply -f k8s-config.yml
kubectl apply -f k8s-setup.yml

kubectl wait --for=condition=ready pod -l app=mysql -n foodfacts --timeout=300s
kubectl wait --for=condition=ready pod -l app=spark-master -n foodfacts --timeout=300s

SPARK_MASTER_POD=$(kubectl get pods -n foodfacts -l app=spark-master -o jsonpath='{.items[0].metadata.name}')
SPARK_WORKER_POD=$(kubectl get pods -n foodfacts -l app=spark-worker -o jsonpath='{.items[0].metadata.name}')

kubectl port-forward pod/$SPARK_MASTER_POD 8090:8090 -n foodfacts > /dev/null 2>&1 &
kubectl port-forward pod/$SPARK_WORKER_POD 8081:8081 -n foodfacts > /dev/null 2>&1 &

echo "Spark Master Web UI: http://localhost:8090"
echo "Spark Worker Web UI: http://localhost:8081"

while ! kubectl exec deployment/mysql -n foodfacts -- mysqladmin ping -u root -proot --silent; do
  sleep 5
done

kubectl wait --for=condition=complete job/data-init -n foodfacts --timeout=300s

./scripts/db_init.sh

kubectl apply -f k8s-job.yml

sleep 5
CLUSTERING_POD=$(kubectl get pods -n foodfacts -l job-name=clustering-job -o jsonpath='{.items[0].metadata.name}')
if [ ! -z "$CLUSTERING_POD" ]; then
  kubectl port-forward pod/$CLUSTERING_POD 4040:4040 -n foodfacts > /dev/null 2>&1 &
  echo "Clustering Job Web UI: http://localhost:4040"
fi

kubectl wait --for=condition=complete job/clustering-job -n foodfacts --timeout=600s

kubectl exec deployment/mysql -n foodfacts -- mysql -u root -proot -D foodfacts -e "
SELECT COUNT(*) as 'Products' FROM products;
SELECT COUNT(*) as 'Predictions' FROM predicts;
SELECT prediction, COUNT(*) as count FROM predicts GROUP BY prediction ORDER BY prediction;
"