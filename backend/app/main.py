import logging
import subprocess
import sys
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .kafka_utils import (
    aggregates_store,
    alerts_store,
    produce_event,
    start_consumer_thread,
)
from .models import SensorEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Simulator process management
_simulator_process: subprocess.Popen | None = None
_simulator_lock = threading.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_consumer_thread()
    logger.info("Backend started")
    yield
    global _simulator_process
    if _simulator_process:
        _simulator_process.terminate()
        _simulator_process = None
    logger.info("Backend shutdown")


app = FastAPI(
    title="IoT Sensor Monitoring API",
    description="Ingestion and serving API for IoT sensor data pipeline",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/api/events")
def ingest_event(event: SensorEvent):
    event_dict = event.model_dump()
    event_dict["event_time"] = event.event_time.isoformat()

    produced = produce_event(
        topic=settings.kafka_events_topic,
        key=event.sensor_id,
        value=event_dict,
    )
    return {
        "status": "accepted" if produced else "accepted_local",
        "kafka": produced,
        "event": event_dict,
    }


@app.get("/api/aggregates")
def get_aggregates(limit: int = 100):
    data = list(aggregates_store)[-limit:]
    return data


@app.get("/api/alerts")
def get_alerts(limit: int = 50):
    data = list(alerts_store)[-limit:]
    return data


@app.post("/api/simulator/start")
def start_simulator():
    global _simulator_process
    with _simulator_lock:
        if _simulator_process and _simulator_process.poll() is None:
            raise HTTPException(status_code=409, detail="Simulator already running")

        _simulator_process = subprocess.Popen(
            [
                sys.executable,
                "/app/sensor_simulator.py",
                "--output",
                "kafka",
                "--kafka-bootstrap",
                settings.kafka_bootstrap_servers,
                "--kafka-topic",
                settings.kafka_events_topic,
                "--num-sensors",
                str(settings.simulator_num_sensors),
                "--interval",
                str(settings.simulator_interval),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
    return {"status": "started", "pid": _simulator_process.pid}


@app.post("/api/simulator/stop")
def stop_simulator():
    global _simulator_process
    with _simulator_lock:
        if not _simulator_process or _simulator_process.poll() is not None:
            raise HTTPException(status_code=404, detail="No simulator running")
        _simulator_process.terminate()
        _simulator_process = None
    return {"status": "stopped"}


@app.get("/api/simulator/status")
def simulator_status():
    if _simulator_process and _simulator_process.poll() is None:
        return {"running": True, "pid": _simulator_process.pid}
    return {"running": False}
