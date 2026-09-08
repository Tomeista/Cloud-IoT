FROM python:3.11-slim

WORKDIR /app

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/app/ ./app/
COPY sensor_simulator.py ./sensor_simulator.py

# The simulator builds its fleet from the same catalogue the Flink job enriches
# against, so every emitted sensor_id resolves to a real group/description.
COPY flink-job/metadata.json ./metadata.json

EXPOSE 8000

# Bind on IPv4: the k3s cluster runs single-stack IPv4 internally (see
# terraform/cloud-init), so a pod has no IPv6 address and a socket bound to ::
# never accepts a connection to the pod IP -- the kubelet probes then fail with
# ECONNREFUSED and restart the container forever. External IPv6 reaches the
# stack through the socat bridge on the node, not through this socket.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
