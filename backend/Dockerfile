FROM python:3.11-slim

WORKDIR /app

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/app/ ./app/
COPY sensor_simulator.py ./sensor_simulator.py

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "::", "--port", "8000"]
