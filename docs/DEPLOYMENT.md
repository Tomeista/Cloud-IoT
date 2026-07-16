# Deployment Guide — IoT Sensor Monitoring on OpenStack

This guide walks through deploying the full stack to three Ubuntu VMs on OpenStack with k3s.

---

## Prerequisites

- 3 Ubuntu VMs (22.04+) on OpenStack with IPv6 connectivity
- SSH access to all three machines
- Minimum resources per VM: 2 vCPUs, 4 GB RAM, 20 GB disk
- One machine designated as **server** (control plane), two as **agents** (workers)

Notation used below:

```
SERVER_IP=<ipv6-address-of-server-node>
AGENT1_IP=<ipv6-address-of-agent-1>
AGENT2_IP=<ipv6-address-of-agent-2>
```

---

## Step 1: Install k3s on the Server Node

SSH into the server VM:

```bash
ssh ubuntu@$SERVER_IP
```

Install k3s with IPv6 dual-stack support:

```bash
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server \
  --cluster-cidr fd00:cafe::/56,10.42.0.0/16 \
  --service-cidr fd01:cafe::/112,10.43.0.0/16 \
  --tls-san $SERVER_IP \
  --write-kubeconfig-mode 644" sh -
```

Wait for it to be ready:

```bash
sudo kubectl get nodes
```

Retrieve the join token (needed for agents):

```bash
sudo cat /var/lib/rancher/k3s/server/node-token
```

Save this token as `K3S_TOKEN`.

---

## Step 2: Join Agent Nodes

SSH into each agent VM and run:

```bash
curl -sfL https://get.k3s.io | K3S_URL="https://[$SERVER_IP]:6443" \
  K3S_TOKEN="<paste-token-here>" sh -
```

Verify on the server:

```bash
sudo kubectl get nodes
# Should show 3 nodes in Ready state
```

---

## Step 3: Set Up a Container Registry

k3s needs to pull your custom images. Simplest option: deploy a private registry on the cluster or use the k3s built-in containerd import.

### Option A: Import images directly (simplest for prototype)

Build images on a machine with Docker, save as tar, copy to all nodes:

```bash
# On your local machine (or a build VM)
docker build -f backend/Dockerfile -t iot-monitoring/backend:latest .
docker build -t iot-monitoring/frontend:latest ./frontend
docker build -t iot-monitoring/flink-job:latest ./flink-job

docker save iot-monitoring/backend:latest -o backend.tar
docker save iot-monitoring/frontend:latest -o frontend.tar
docker save iot-monitoring/flink-job:latest -o flink-job.tar
```

Copy to each node and import:

```bash
# On each node
sudo k3s ctr images import backend.tar
sudo k3s ctr images import frontend.tar
sudo k3s ctr images import flink-job.tar
```

### Option B: In-cluster registry (better for iteration)

```bash
sudo kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: registry
  namespace: kube-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: registry
  template:
    metadata:
      labels:
        app: registry
    spec:
      containers:
        - name: registry
          image: registry:2
          ports:
            - containerPort: 5000
---
apiVersion: v1
kind: Service
metadata:
  name: registry
  namespace: kube-system
spec:
  type: NodePort
  ports:
    - port: 5000
      nodePort: 30500
  selector:
    app: registry
EOF
```

Then configure k3s to trust it. Create `/etc/rancher/k3s/registries.yaml` on all nodes:

```yaml
mirrors:
  "registry:5000":
    endpoint:
      - "http://[$SERVER_IP]:30500"
```

Restart k3s on each node after this change.

---

## Step 4: Build and Push Images

If using Option B (in-cluster registry):

```bash
export REGISTRY="[$SERVER_IP]:30500"

docker build -f backend/Dockerfile -t $REGISTRY/backend:latest .
docker build -t $REGISTRY/frontend:latest ./frontend
docker build -t $REGISTRY/flink-job:latest ./flink-job

docker push $REGISTRY/backend:latest
docker push $REGISTRY/frontend:latest
docker push $REGISTRY/flink-job:latest
```

---

## Step 5: Install Helm (on server node)

```bash
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

Copy the kubeconfig for Helm to use:

```bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
```

---

## Step 6: Deploy the Stack

### Using Helm (recommended):

```bash
helm upgrade --install iot-monitoring ./helm \
  --namespace iot-monitoring \
  --create-namespace \
  --set imageRegistry="iot-monitoring" \
  --set imagePullPolicy="IfNotPresent"
```

If using in-cluster registry:

```bash
helm upgrade --install iot-monitoring ./helm \
  --namespace iot-monitoring \
  --create-namespace \
  --set imageRegistry="registry:5000"
```

### Using raw manifests (alternative):

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/seaweedfs.yaml
kubectl apply -f k8s/backend.yaml
kubectl apply -f k8s/frontend.yaml
kubectl apply -f k8s/flink.yaml
```

---

## Step 7: Wait for Infrastructure to Start

```bash
kubectl -n iot-monitoring get pods -w
```

Wait until Kafka, Zookeeper, and SeaweedFS are Running before proceeding. This may take 1–3 minutes on first pull.

---

## Step 8: Submit the Flink Job

Once the Flink JobManager is running:

```bash
# Port-forward to Flink REST API
kubectl -n iot-monitoring port-forward svc/flink-jobmanager 8081:8081 &

# Submit the PyFlink job
kubectl -n iot-monitoring exec -it deploy/flink-jobmanager -- \
  flink run -py /opt/flink/job/job.py
```

Verify the job is running:

```bash
curl http://localhost:8081/jobs
```

---

## Step 9: Verify End-to-End Flow

### Check all pods are running:

```bash
kubectl -n iot-monitoring get pods
```

### Access the Web UI:

```
http://[$SERVER_IP]:30080
```

### Test the API directly:

```bash
# Health check
curl http://[$SERVER_IP]:30080/api/health

# Send a test event
curl -X POST http://[$SERVER_IP]:30080/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "sensor_id": "sensor-temp-0001",
    "sensor_type": "temperature",
    "value": 42.5,
    "unit": "°C",
    "location": "Hall-A1"
  }'

# Start the simulator
curl -X POST http://[$SERVER_IP]:30080/api/simulator/start

# Check aggregates (wait ~60s for first window)
curl http://[$SERVER_IP]:30080/api/aggregates

# Check alerts
curl http://[$SERVER_IP]:30080/api/alerts
```

---

## Step 10: Demonstrate Horizontal Scaling

```bash
# Scale frontend
kubectl -n iot-monitoring scale deployment/frontend --replicas=3

# Scale backend
kubectl -n iot-monitoring scale deployment/backend --replicas=3

# Scale Flink TaskManagers
kubectl -n iot-monitoring scale deployment/flink-taskmanager --replicas=4

# Verify
kubectl -n iot-monitoring get pods
```

Take a screenshot of this for the submission.

---

## Troubleshooting

### Pods stuck in Pending

```bash
kubectl -n iot-monitoring describe pod <pod-name>
# Usually means insufficient resources or PVC not bound
```

### Kafka connection refused

```bash
# Check Kafka is ready
kubectl -n iot-monitoring logs statefulset/kafka
# Ensure Zookeeper started first
kubectl -n iot-monitoring logs statefulset/zookeeper
```

### Flink job fails

```bash
kubectl -n iot-monitoring logs deploy/flink-jobmanager
# Check TaskManager connectivity
kubectl -n iot-monitoring logs deploy/flink-taskmanager
```

### Frontend shows no data

1. Verify backend is running: `curl http://[$SERVER_IP]:30080/api/health`
2. Check Kafka topics have data: `kubectl -n iot-monitoring exec -it kafka-0 -- kafka-topics --list --bootstrap-server localhost:9092`
3. Ensure Flink job is running: check Flink UI on port 8081

### IPv6 connectivity issues

- Ensure OpenStack security groups allow traffic on ports 30080, 6443, 9092
- Verify nodes can reach each other: `ping6 $AGENT1_IP`
- Check k3s uses IPv6 for pod networking: `kubectl get pods -o wide`

---

## Quick Reference

| Service         | Access                                                |
| --------------- | ----------------------------------------------------- |
| Web UI          | `http://[$SERVER_IP]:30080`                           |
| Backend API     | `http://[$SERVER_IP]:30080/api/`                      |
| Flink Dashboard | `kubectl port-forward svc/flink-jobmanager 8081:8081` |
| SeaweedFS Filer UI | `kubectl port-forward svc/seaweedfs 8888:8888`     |
| Kubernetes      | `kubectl -n iot-monitoring get all`                   |

### Verify archived objects

The backend archives raw sensor events as JSON Lines objects into the
`iot-lakehouse` bucket on SeaweedFS. To check that archiving works:

```bash
# Archiver stats (objects_written should grow while the simulator runs)
curl http://localhost:8000/api/archive/status

# Browse the bucket in the SeaweedFS Filer UI (after port-forwarding 8888)
# http://localhost:8888/buckets/iot-lakehouse/raw/
```
