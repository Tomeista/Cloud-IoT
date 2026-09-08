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
    "commits_written": 0,
    "objects_written": 0,
    "events_archived": 0,
    "records_rejected": 0,
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
    """Kafka topic -> Delta table in the lakehouse.

    `raw` is the immutable landing zone (every event as ingested); `aggregates`
    and `alerts` are the stream job's results. Archiving all of them means the
    lakehouse holds both the input and the output of the pipeline, so results
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
    # SeaweedFS requires path-style addressing (no virtual-host DNS).
    # Delta writes go through delta-rs; this client is still needed to create
    # the bucket and to park records the schema rejected.
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
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


def _quarantine(s3, dataset: str, records: list[dict]) -> None:
    """Park schema-rejected records as JSONL beside the tables.

    Same reasoning as the `late` dataset: a record the pipeline could not take
    is written down rather than dropped, so the loss is auditable. JSONL is the
    right format here precisely because it needs no schema -- these are the
    records that did not have one.
    """
    now = datetime.now(timezone.utc)
    key = (
        f"_rejected/{dataset}/dt={now:%Y-%m-%d}/hour={now:%H}/"
        f"{int(now.timestamp() * 1000)}-{uuid.uuid4().hex[:8]}.jsonl"
    )
    body = "\n".join(json.dumps(record, default=str) for record in records) + "\n"
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    archive_stats["records_rejected"] += len(records)
    logger.error(
        "Quarantined %d rejected %s records to s3://%s/%s",
        len(records),
        dataset,
        settings.s3_bucket,
        key,
    )


def _flush(s3, dataset: str, buffer: list[dict]) -> None:
    """Append the buffer to the dataset's Delta table as a single commit.

    Unlike the JSONL path this does not group records by partition by hand:
    `dt` and `hour` are attached as columns and delta-rs lays the files out
    under `dt=`/`hour=` itself, recording in the transaction log which files
    belong to the table.
    """
    # Imported here rather than at module scope: the serving replicas import
    # this module only for archive_stats, and pyarrow costs ~100 MB of RSS that
    # a pod which never archives should not pay for.
    from . import delta_writer

    rows = []
    for record in buffer:
        ts = _partition_time(dataset, record)
        rows.append({**record, "dt": f"{ts:%Y-%m-%d}", "hour": f"{ts:%H}"})

    table, rejected = delta_writer.build_table(dataset, rows)

    if table is not None:
        delta_writer.append(dataset, table)

        version = delta_writer.current_version(dataset)
        location = delta_writer.table_uri(dataset)
        last_key = location if version is None else f"{location} @ v{version}"

        stats = archive_stats["datasets"].setdefault(
            dataset,
            {
                "commits_written": 0,
                "objects_written": 0,
                "records_archived": 0,
                "records_rejected": 0,
                "version": None,
                "last_object_key": None,
            },
        )
        stats["commits_written"] += 1
        # One commit writes at least one Parquet object; the dashboard's object
        # counter keeps its meaning at commit granularity.
        stats["objects_written"] += 1
        stats["records_archived"] += table.num_rows
        stats["version"] = version
        stats["last_object_key"] = last_key
        archive_stats["commits_written"] += 1
        archive_stats["objects_written"] += 1
        archive_stats["events_archived"] += table.num_rows
        archive_stats["last_object_key"] = last_key
        logger.info(
            "Committed %d %s records to %s (version %s)",
            table.num_rows,
            dataset,
            location,
            version,
        )

    if rejected:
        _quarantine(s3, dataset, rejected)
        archive_stats["datasets"].setdefault(dataset, {}).setdefault(
            "records_rejected", 0
        )
        archive_stats["datasets"][dataset]["records_rejected"] += len(rejected)


def _archive_loop():
    """Background thread: archive raw events and pipeline results to the lakehouse.

    Runs in exactly one pod. A Delta table tolerates only one writer here:
    SeaweedFS accepts a conditional put without enforcing it, so two writers
    would both claim the same log version and one would silently overwrite the
    other. The archiver therefore has its own Deployment at replicas: 1 with
    strategy: Recreate, while the serving replicas run with the archiver off.
    """
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
                    except Exception as e:  # noqa: BLE001
                        # delta-rs raises its own exception types, and importing
                        # them here would pull pyarrow into the serving pods.
                        # Records that do not fit the schema never reach this
                        # point -- build_table quarantines them -- so what lands
                        # here is a failed commit: keep the batch and retry.
                        logger.warning(
                            "Delta commit failed for %s (%s), keeping %d records "
                            "buffered",
                            dataset,
                            e,
                            len(buffers[dataset]),
                        )
                        del buffers[dataset][:-_MAX_BUFFERED_EVENTS]

                # Acknowledge the consumed records only once every buffer has
                # reached S3. Auto-commit would advance the offsets while
                # records were still held in memory, so a pod restart would
                # drop them without a trace. Committing late instead means a
                # crash mid-flush replays a batch: duplicates in the lakehouse,
                # but no silent loss.
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
    logger.info("Started background Delta archiver thread")
