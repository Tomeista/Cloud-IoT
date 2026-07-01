"""FastAPI Backend for IoT Sensor Monitoring.

Provides REST API for event ingestion, serving aggregates/alerts,
and WebSocket for real-time updates. Connects to Kafka as producer
and consumer.
"""

import asyncio
import json
import logging
import os
import sys
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENTS_TOPIC = os.getenv("KAFKA_EVENTS_TOPIC", "sensor-events")
AGGREGATES_TOPIC = os.getenv("KAFKA_AGGREGATES_TOPIC", "sensor-aggregates")
ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "sensor-alerts")

logger = logging.getLogger("iot-backend")
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
MAX_BUFFER = 500
recent_events: deque = deque(maxlen=MAX_BUFFER)
recent_alerts: deque = deque(maxlen=MAX_BUFFER)
recent_aggregates: deque = deque(maxlen=MAX_BUFFER)
ws_clients: set[WebSocket] = set()

producer = None
consumer_task = None
simulator_task = None

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class SensorEvent(BaseModel):
    sensor_id: str
    timestamp: str | None = None
    type: str
    value: float
    unit: str
    location: str


class SimulatorConfig(BaseModel):
    num_sensors: int = 20
    interval: float = 1.0
    batch_size: int = 5


# ---------------------------------------------------------------------------
# WebSocket broadcast helper
# ---------------------------------------------------------------------------

async def broadcast_ws(message: dict):
    dead: set[WebSocket] = set()
    for ws in ws_clients:
        try:
            await ws.send_json(message)
        except Exception:
            dead.add(ws)
    ws_clients.difference_update(dead)


# ---------------------------------------------------------------------------
# Kafka consumer background task
# ---------------------------------------------------------------------------

async def consume_results():
    """Continuously consume aggregates and alerts from Kafka."""
    from aiokafka import AIOKafkaConsumer

    await asyncio.sleep(5)  # give Kafka time to become ready
    retries = 0
    while retries < 30:
        try:
            consumer = AIOKafkaConsumer(
                AGGREGATES_TOPIC,
                ALERTS_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="api-consumer",
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode()),
            )
            await consumer.start()
            logger.info("Kafka consumer started")
            async for msg in consumer:
                data = msg.value
                if msg.topic == ALERTS_TOPIC:
                    recent_alerts.appendleft(data)
                    await broadcast_ws({"type": "alert", "data": data})
                elif msg.topic == AGGREGATES_TOPIC:
                    recent_aggregates.appendleft(data)
                    await broadcast_ws({"type": "aggregate", "data": data})
        except asyncio.CancelledError:
            break
        except Exception as exc:
            retries += 1
            logger.warning("Kafka consumer error (retry %d): %s", retries, exc)
            await asyncio.sleep(min(retries * 2, 30))
    logger.info("Kafka consumer stopped")


# ---------------------------------------------------------------------------
# Simulator background task
# ---------------------------------------------------------------------------

async def run_simulator(config: SimulatorConfig):
    # Import the sensor simulator from project root
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from sensor_simulator import SensorFleet

    fleet = SensorFleet(config.num_sensors)
    logger.info(
        "Simulator started: %d sensors, batch=%d, interval=%.1fs",
        config.num_sensors,
        config.batch_size,
        config.interval,
    )
    try:
        while True:
            events = fleet.read_random(config.batch_size)
            for ev in events:
                ev_dict = asdict(ev)
                recent_events.appendleft(ev_dict)

                if producer:
                    await producer.send(EVENTS_TOPIC, key=ev.sensor_id, value=ev_dict)

                await broadcast_ws({"type": "event", "data": ev_dict})

            await asyncio.sleep(config.interval)
    except asyncio.CancelledError:
        logger.info("Simulator stopped")


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(_app: FastAPI):
    global producer, consumer_task

    try:
        from aiokafka import AIOKafkaProducer

        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode(),
            key_serializer=lambda k: k.encode() if k else None,
        )
        await producer.start()
        consumer_task = asyncio.create_task(consume_results())
        logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP)
    except Exception as exc:
        logger.warning("Kafka not available – running without Kafka: %s", exc)
        producer = None

    yield

    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    if producer:
        await producer.stop()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="IoT Sensor Monitoring API", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# REST routes
# ---------------------------------------------------------------------------

@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "kafka_connected": producer is not None,
        "simulator_running": simulator_task is not None and not simulator_task.done(),
    }


@app.post("/api/events")
async def create_event(event: SensorEvent):
    if not event.timestamp:
        event.timestamp = datetime.now(timezone.utc).isoformat()

    ev_dict = event.model_dump()
    recent_events.appendleft(ev_dict)

    if producer:
        await producer.send(EVENTS_TOPIC, key=event.sensor_id, value=ev_dict)

    await broadcast_ws({"type": "event", "data": ev_dict})
    return {"status": "accepted", "event": ev_dict}


@app.post("/api/events/batch")
async def create_events_batch(events: list[SensorEvent]):
    results = []
    for event in events:
        if not event.timestamp:
            event.timestamp = datetime.now(timezone.utc).isoformat()
        ev_dict = event.model_dump()
        recent_events.appendleft(ev_dict)
        if producer:
            await producer.send(EVENTS_TOPIC, key=event.sensor_id, value=ev_dict)
        await broadcast_ws({"type": "event", "data": ev_dict})
        results.append(ev_dict)
    return {"status": "accepted", "count": len(results)}


@app.post("/api/simulator/start")
async def start_simulator(config: SimulatorConfig = SimulatorConfig()):
    global simulator_task
    if simulator_task and not simulator_task.done():
        raise HTTPException(status_code=400, detail="Simulator already running")
    simulator_task = asyncio.create_task(run_simulator(config))
    return {"status": "started", "config": config.model_dump()}


@app.post("/api/simulator/stop")
async def stop_simulator():
    global simulator_task
    if not simulator_task or simulator_task.done():
        raise HTTPException(status_code=400, detail="Simulator not running")
    simulator_task.cancel()
    try:
        await simulator_task
    except asyncio.CancelledError:
        pass
    simulator_task = None
    return {"status": "stopped"}


@app.get("/api/simulator/status")
async def simulator_status():
    running = simulator_task is not None and not simulator_task.done()
    return {"running": running}


@app.get("/api/events/recent")
async def get_recent_events(limit: int = 50):
    return list(recent_events)[:limit]


@app.get("/api/alerts")
async def get_alerts(limit: int = 100):
    return list(recent_alerts)[:limit]


@app.get("/api/aggregates")
async def get_aggregates(limit: int = 100):
    return list(recent_aggregates)[:limit]


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------

@app.websocket("/ws/live")
async def websocket_live(ws: WebSocket):
    await ws.accept()
    ws_clients.add(ws)
    logger.info("WebSocket client connected (%d total)", len(ws_clients))
    try:
        while True:
            await ws.receive_text()  # keep-alive
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(ws)
        logger.info("WebSocket client disconnected (%d total)", len(ws_clients))


# ---------------------------------------------------------------------------
# Static frontend files (dev mode – in Docker, nginx serves these instead)
# ---------------------------------------------------------------------------
_frontend_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "frontend", "public",
)
if os.path.isdir(_frontend_dir):
    app.mount("/", StaticFiles(directory=_frontend_dir, html=True), name="frontend")
    logger.info("Serving frontend from %s", _frontend_dir)
