#!/bin/bash
# IoT Sensor Monitoring - Kubernetes Deployment Script
# Deploys all components to a k3s cluster
#
#   ./deploy.sh                  # Helm, images imported into the node's containerd
#   ./deploy.sh --manifests      # raw manifests from k8s/
#   REGISTRY=host:5000 PUSH=1 ./deploy.sh    # build against a real registry
#
# The default registry prefix matches what helm/values.yaml and k8s/*.yaml
# reference, so images built here are the images the cluster looks for.

set -euo pipefail

NAMESPACE="iot-monitoring"
REGISTRY="${REGISTRY:-iot-monitoring}"
PUSH="${PUSH:-0}"

echo "=== IoT Sensor Monitoring - Deployment ==="
echo "Registry: $REGISTRY"
echo "Namespace: $NAMESPACE"
echo ""

if [ "${1:-}" == "--manifests" ] && [ "$REGISTRY" != "iot-monitoring" ]; then
    echo "ERROR: k8s/*.yaml hardcode the image prefix 'iot-monitoring/'."
    echo "       Use the Helm path (which takes --set imageRegistry) for '$REGISTRY'."
    exit 1
fi

# --- Build images ---
echo "--- Building container images ---"

# The backend image pulls in sensor_simulator.py and flink-job/metadata.json,
# so it needs the repository root as its build context, not ./backend.
echo "Building backend..."
docker build -f backend/Dockerfile -t "$REGISTRY/backend:latest" .

echo "Building frontend..."
docker build -t "$REGISTRY/frontend:latest" ./frontend

echo "Building flink-job..."
docker build -t "$REGISTRY/flink-job:latest" ./flink-job

if [ "$PUSH" == "1" ]; then
    echo ""
    echo "--- Pushing images ---"
    docker push "$REGISTRY/backend:latest"
    docker push "$REGISTRY/frontend:latest"
    docker push "$REGISTRY/flink-job:latest"
else
    echo ""
    echo "Skipping push (set PUSH=1 to push to a real registry)."
    echo "Import the images on every node instead, e.g.:"
    echo "  docker save $REGISTRY/backend:latest | sudo k3s ctr images import -"
fi

echo ""
echo "--- Deploying to Kubernetes ---"

if [ "${1:-}" == "--manifests" ]; then
    echo "Using raw manifests..."
    kubectl apply -f k8s/namespace.yaml
    kubectl apply -f k8s/kafka.yaml
    kubectl apply -f k8s/seaweedfs.yaml
    kubectl apply -f k8s/backend.yaml
    kubectl apply -f k8s/archiver.yaml
    kubectl apply -f k8s/frontend.yaml
    kubectl apply -f k8s/flink.yaml
    kubectl apply -f k8s/simulator.yaml
else
    echo "Using Helm chart..."
    helm upgrade --install iot-monitoring ./helm \
        --namespace "$NAMESPACE" \
        --create-namespace \
        --set imageRegistry="$REGISTRY"
fi

echo ""
echo "--- Waiting for infrastructure ---"
kubectl -n "$NAMESPACE" rollout status statefulset/zookeeper --timeout=180s
kubectl -n "$NAMESPACE" rollout status statefulset/kafka --timeout=180s
kubectl -n "$NAMESPACE" rollout status statefulset/seaweedfs --timeout=180s

echo ""
echo "--- Waiting for applications ---"
kubectl -n "$NAMESPACE" rollout status deployment/flink-jobmanager --timeout=180s
kubectl -n "$NAMESPACE" rollout status deployment/flink-taskmanager --timeout=180s
kubectl -n "$NAMESPACE" rollout status deployment/backend --timeout=120s
kubectl -n "$NAMESPACE" rollout status deployment/archiver --timeout=120s
kubectl -n "$NAMESPACE" rollout status deployment/frontend --timeout=120s

# The submit Job waits for the JobManager itself and exits once the pipeline is
# running; failing here means the stream job did not start.
echo ""
echo "--- Waiting for the Flink job submission ---"
kubectl -n "$NAMESPACE" wait --for=condition=complete job/flink-job-submit --timeout=300s

echo ""
echo "=== Deployment complete ==="
kubectl -n "$NAMESPACE" get pods
echo ""
echo "Frontend: http://<node-ip>:30080"
echo "Backend API: http://<node-ip>:30080/api/health"
echo "Flink UI: kubectl -n $NAMESPACE port-forward svc/flink-jobmanager 8081:8081"
echo "SeaweedFS Filer UI: kubectl -n $NAMESPACE port-forward svc/seaweedfs 8888:8888"
