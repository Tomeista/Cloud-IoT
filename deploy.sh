#!/bin/bash
# IoT Sensor Monitoring - Kubernetes Deployment Script
# Deploys all components to a k3s cluster

set -e

NAMESPACE="iot-monitoring"
REGISTRY="${REGISTRY:-localhost:5000}"

echo "=== IoT Sensor Monitoring - Deployment ==="
echo "Registry: $REGISTRY"
echo "Namespace: $NAMESPACE"
echo ""

# Build and push images
echo "--- Building container images ---"

echo "Building backend..."
docker build -t "$REGISTRY/backend:latest" ./backend

echo "Building frontend..."
docker build -t "$REGISTRY/frontend:latest" ./frontend

echo "Building flink-job..."
docker build -t "$REGISTRY/flink-job:latest" ./flink-job

echo ""
echo "--- Pushing images ---"
docker push "$REGISTRY/backend:latest"
docker push "$REGISTRY/frontend:latest"
docker push "$REGISTRY/flink-job:latest"

echo ""
echo "--- Deploying to Kubernetes ---"

# Option 1: Direct manifests
if [ "$1" == "--manifests" ]; then
    echo "Using raw manifests..."
    kubectl apply -f k8s/namespace.yaml
    kubectl apply -f k8s/kafka.yaml
    kubectl apply -f k8s/seaweedfs.yaml
    kubectl apply -f k8s/backend.yaml
    kubectl apply -f k8s/frontend.yaml
    kubectl apply -f k8s/flink.yaml
else
    # Option 2: Helm (default)
    echo "Using Helm chart..."
    helm upgrade --install iot-monitoring ./helm \
        --namespace "$NAMESPACE" \
        --create-namespace \
        --set imageRegistry="$REGISTRY"
fi

echo ""
echo "--- Waiting for pods ---"
kubectl -n "$NAMESPACE" rollout status deployment/backend --timeout=120s
kubectl -n "$NAMESPACE" rollout status deployment/frontend --timeout=120s

echo ""
echo "=== Deployment complete ==="
echo "Frontend: http://<node-ip>:30080"
echo "Backend API: http://<node-ip>:30080/api/health"
echo "Flink UI: kubectl -n $NAMESPACE port-forward svc/flink-jobmanager 8081:8081"
echo "SeaweedFS Filer UI: kubectl -n $NAMESPACE port-forward svc/seaweedfs 8888:8888"
