import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .archiver import archive_stats, start_archiver_thread
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_consumer_thread()
    if settings.archiver_enabled:
        start_archiver_thread()
    else:
        logger.info("Archiver disabled on this replica (serving only)")
    logger.info("Backend started")
    yield
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


@app.get("/api/archive/status")
def archive_status():
    return archive_stats


@app.get("/api/history")
def history(
    dataset: str,
    dt: str | None = None,
    sensor_id: str | None = None,
    limit: int = 100,
):
    """Query one Delta table directly, over its full history.

    The other read endpoints answer from the serving layer's in-memory window
    and therefore only know the recent past; this one reads the lakehouse.

    Served only by the archiver pod: reading Delta needs pyarrow, which the
    serving replicas deliberately do not load (see `delta_writer`). nginx
    routes this path to the archiver Service.
    """
    if not settings.archiver_enabled:
        raise HTTPException(
            status_code=503,
            detail=(
                "This replica serves the stream only. /api/history is answered "
                "by the archiver; reach it through the frontend proxy."
            ),
        )

    # Imported lazily for the same reason the archiver imports its writer late.
    from .delta_reader import TableMissing, datasets, query

    if dataset not in datasets():
        raise HTTPException(
            status_code=400,
            detail=f"unknown dataset {dataset!r}; expected one of {datasets()}",
        )

    try:
        return query(dataset, dt=dt, sensor_id=sensor_id, limit=limit)
    except TableMissing:
        raise HTTPException(
            status_code=404,
            detail=f"dataset {dataset!r} has no committed data yet",
        ) from None
