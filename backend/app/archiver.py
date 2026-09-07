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
    "datasets": {},
}

# Field carrying each dataset's own event time. Partitioning on this instead of
# on arrival time keeps a record in the same partition when it is replayed or
# arrives late, so a partition is complete once its watermark has passed.
_TIME_FIELD = {
    "raw": "event_time",
    "aggregates": "window_start_ts",
    "alerts": "timestamp",
    "late": "event_time",
}


def _dataset_topics() -> dict[str, str]:
    """Kafka topic -> dataset prefix in the lake.

    `raw` is the immutable landing zone (every event as ingested); `aggregates`
    and `alerts` are the stream job's results. Archiving all of them means the
    lake holds both the input and the output of the pipeline, so results
    survive a restart instead of living only in the serving layer's memory.
    `late` holds events the stream job dropped for arriving past their window's
    allowed lateness -- keeping them makes the loss auditable rather than
    invisible.
    """
    return {
        settings.kafka_events_topic: "raw",
        settings.kafka_aggregates_topic: "aggregates",
        settings.kafka_alerts_topic: "alerts",
        settings.kafka_late_topic: "late",
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


def _partition_time(dataset: str, record: dict) -> datetime:
    """Event time of a record, falling back to ingest time when unavailable."""
    value = record.get(_TIME_FIELD.get(dataset, ""))
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _flush(s3, dataset: str, buffer: list[dict]) -> None:
    """Write the buffer as JSON Lines, one object per hourly partition.

    Hive-style `dt=`/`hour=` partitions are what query engines (Spark, DuckDB,
    Trino) expect for partition pruning, so a later batch query can read a
    single day without scanning the bucket.
    """
    partitions: dict[str, list[dict]] = {}
    for record in buffer:
        ts = _partition_time(dataset, record)
        partitions.setdefault(f"dt={ts:%Y-%m-%d}/hour={ts:%H}", []).append(record)

    for partition, records in partitions.items():
        now = datetime.now(timezone.utc)
        key = (
            f"{dataset}/{partition}/"
            f"{int(now.timestamp() * 1000)}-{uuid.uuid4().hex[:8]}.jsonl"
        )
        body = "\n".join(json.dumps(record, default=str) for record in records) + "\n"
        s3.put_object(
            Bucket=settings.s3_bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )

        stats = archive_stats["datasets"].setdefault(
            dataset,
            {"objects_written": 0, "records_archived": 0, "last_object_key": None},
        )
        stats["objects_written"] += 1
        stats["records_archived"] += len(records)
        stats["last_object_key"] = key
        archive_stats["objects_written"] += 1
        archive_stats["events_archived"] += len(records)
        archive_stats["last_object_key"] = key
        logger.info(
            "Archived %d %s records to s3://%s/%s",
            len(records),
            dataset,
            settings.s3_bucket,
            key,
        )


def _archive_loop():
    """Background thread: archive raw events and pipeline results to S3."""
    topics = _dataset_topics()
    buffers: dict[str, list[dict]] = {dataset: [] for dataset in topics.values()}
    last_flush = time.monotonic()
    s3 = None
    while True:
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="backend-archiver",
                auto_offset_reset="latest",
                # Committed manually once a batch is durably in S3; see below.
                enable_auto_commit=False,
            )
            logger.info("Kafka consumer connected for archiver: %s", ", ".join(topics))
            while True:
                records = consumer.poll(timeout_ms=1000)
                for partition, messages in records.items():
                    dataset = topics.get(partition.topic)
                    if dataset is not None:
                        buffers[dataset].extend(msg.value for msg in messages)

                # Flush a dataset once it reaches the batch size, and every
                # dataset holding anything when the flush interval elapses --
                # alerts are rare, so they must not wait for a full batch.
                now = time.monotonic()
                timer_due = now - last_flush >= settings.s3_archive_flush_seconds
                due = [
                    dataset
                    for dataset, buffer in buffers.items()
                    if buffer
                    and (timer_due or len(buffer) >= settings.s3_archive_max_batch)
                ]
                if timer_due:
                    last_flush = now

                for dataset in due:
                    try:
                        if s3 is None:
                            s3 = _s3_client()
                            _ensure_bucket(s3)
                        _flush(s3, dataset, buffers[dataset])
                        buffers[dataset].clear()
                    except (ClientError, BotoCoreError) as e:
                        logger.warning(
                            "S3 flush failed for %s (%s), keeping %d records buffered",
                            dataset,
                            e,
                            len(buffers[dataset]),
                        )
                        s3 = None
                        del buffers[dataset][:-_MAX_BUFFERED_EVENTS]

                # Acknowledge the consumed records only once every buffer has
                # reached S3. Auto-commit would advance the offsets while
                # records were still held in memory, so a pod restart would
                # drop them without a trace. Committing late instead means a
                # crash mid-flush replays a batch: duplicates in the lake, but
                # no silent loss.
                if due and not any(buffers.values()):
                    consumer.commit()
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
