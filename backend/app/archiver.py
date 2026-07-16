import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from .config import settings

logger = logging.getLogger(__name__)

# Bound the in-memory buffer so a long S3 outage cannot exhaust memory
_MAX_BUFFERED_EVENTS = 10_000

# Stats exposed via GET /api/archive/status
archive_stats = {
    "objects_written": 0,
    "events_archived": 0,
    "last_object_key": None,
}


def _s3_client():
    # SeaweedFS requires path-style addressing (no virtual-host DNS)
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            connect_timeout=5,
            retries={"max_attempts": 2},
        ),
    )


def _ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=settings.s3_bucket)
    except ClientError:
        s3.create_bucket(Bucket=settings.s3_bucket)
        logger.info("Created bucket %s", settings.s3_bucket)


def _flush(s3, buffer: list[dict]) -> None:
    now = datetime.now(timezone.utc)
    key = (
        f"raw/{now:%Y/%m/%d/%H}/"
        f"{int(now.timestamp() * 1000)}-{uuid.uuid4().hex[:8]}.jsonl"
    )
    body = "\n".join(json.dumps(event, default=str) for event in buffer) + "\n"
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    archive_stats["objects_written"] += 1
    archive_stats["events_archived"] += len(buffer)
    archive_stats["last_object_key"] = key
    logger.info(
        "Archived %d events to s3://%s/%s", len(buffer), settings.s3_bucket, key
    )


def _archive_loop():
    """Background thread: consume raw sensor events and archive them to S3."""
    buffer: list[dict] = []
    last_flush = time.monotonic()
    s3 = None
    while True:
        try:
            consumer = KafkaConsumer(
                settings.kafka_events_topic,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="backend-archiver",
                auto_offset_reset="latest",
            )
            logger.info("Kafka consumer connected for archiver")
            while True:
                records = consumer.poll(timeout_ms=1000)
                for _partition, messages in records.items():
                    buffer.extend(msg.value for msg in messages)

                flush_due = len(buffer) >= settings.s3_archive_max_batch or (
                    buffer
                    and time.monotonic() - last_flush
                    >= settings.s3_archive_flush_seconds
                )
                if flush_due:
                    try:
                        if s3 is None:
                            s3 = _s3_client()
                            _ensure_bucket(s3)
                        _flush(s3, buffer)
                        buffer.clear()
                    except (ClientError, BotoCoreError) as e:
                        logger.warning(
                            "S3 flush failed (%s), keeping %d events buffered",
                            e,
                            len(buffer),
                        )
                        s3 = None
                        del buffer[:-_MAX_BUFFERED_EVENTS]
                    last_flush = time.monotonic()
        except NoBrokersAvailable:
            logger.warning("Kafka not available for archiver, retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Archiver error: {e}, retrying in 5s...")
            time.sleep(5)


def start_archiver_thread():
    thread = threading.Thread(target=_archive_loop, daemon=True)
    thread.start()
    logger.info("Started background S3 archiver thread")
