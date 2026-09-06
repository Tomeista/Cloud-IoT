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

CMD ["uvicorn", "app.main:app", "--host", "::", "--port", "8000"]
