"""Delta Lake write path for the four pipeline datasets.

Kept apart from `archiver.py` on purpose: importing pyarrow costs roughly
100 MB of resident memory, and the serving replicas import the archiver module
only to read `archive_stats`. They never write, so they never import this.

Each dataset is its own Delta table under the same bucket:

    s3://iot-lakehouse/{raw|aggregates|alerts|late}/
        _delta_log/                       <- the transaction log
        dt=YYYY-MM-DD/hour=HH/*.parquet   <- the data

The transaction log is what separates the lakehouse from the previous JSONL
lake: readers see the table at a consistent version instead of whichever
objects happen to come back from a bucket listing.
"""

import logging

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from .config import settings

logger = logging.getLogger(__name__)

# Partition columns. Under JSONL these lived only in the object key; Delta
# needs them as real columns, so the archiver adds them before writing.
_PARTITION_COLUMNS = ["dt", "hour"]


def _schema(*fields: pa.Field) -> pa.Schema:
    return pa.schema(
        [*fields, pa.field("dt", pa.string()), pa.field("hour", pa.string())]
    )


# Timestamps stay strings rather than pa.timestamp(). They are ISO-8601 in UTC,
# which sorts chronologically, so Parquet's per-file min/max statistics still
# support predicate pushdown -- and no record can be lost to a parse failure on
# the write path.
SCHEMAS: dict[str, pa.Schema] = {
    # As ingested from `sensor-events`, before the Flink job enriches it.
    "raw": _schema(
        pa.field("sensor_id", pa.string()),
        pa.field("event_time", pa.string()),
        pa.field("sensor_type", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("unit", pa.string()),
        pa.field("location", pa.string()),
    ),
    # Window results from WindowAggregateFunction in flink-job/job.py.
    "aggregates": _schema(
        pa.field("window_start", pa.string()),
        pa.field("window_end", pa.string()),
        pa.field("window_start_ts", pa.string()),
        pa.field("window_end_ts", pa.string()),
        pa.field("sensor_id", pa.string()),
        pa.field("sensor_type", pa.string()),
        pa.field("location", pa.string()),
        pa.field("group", pa.string()),
        pa.field("description", pa.string()),
        pa.field("avg_value", pa.float64()),
        pa.field("min_value", pa.float64()),
        pa.field("max_value", pa.float64()),
        pa.field("event_count", pa.int64()),
    ),
    # Emitted by AlertingFunction once a breach is sustained.
    "alerts": _schema(
        pa.field("sensor_id", pa.string()),
        pa.field("sensor_type", pa.string()),
        pa.field("location", pa.string()),
        pa.field("group", pa.string()),
        pa.field("description", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("threshold", pa.float64()),
        pa.field("timestamp", pa.string()),
        pa.field("severity", pa.string()),
        pa.field("consecutive_breaches", pa.int64()),
    ),
    # Side output of the window: enriched events, so group/description are set.
    "late": _schema(
        pa.field("sensor_id", pa.string()),
        pa.field("event_time", pa.string()),
        pa.field("sensor_type", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("unit", pa.string()),
        pa.field("location", pa.string()),
        pa.field("group", pa.string()),
        pa.field("description", pa.string()),
    ),
}

# Arrow raises these when a record does not fit its table's schema.
_SCHEMA_ERRORS = (
    pa.ArrowInvalid,
    pa.ArrowTypeError,
    pa.ArrowNotImplementedError,
)

# One warning per (dataset, field), not one per record.
_warned_unknown: set[tuple[str, str]] = set()


def table_uri(dataset: str) -> str:
    return f"s3://{settings.s3_bucket}/{dataset}"


def storage_options() -> dict[str, str]:
    """S3 settings for delta-rs.

    Note what is absent: AWS_S3_ALLOW_UNSAFE_RENAME. delta-rs offers it as an
    escape hatch for stores that cannot commit safely, at the price of the
    guarantee that two writers never claim the same log version. It is not
    needed here, and not wanted either: a single writer is guaranteed by the
    deployment instead (see k8s/archiver.yaml).
    """
    return {
        "AWS_ENDPOINT_URL": settings.s3_endpoint,
        "AWS_ACCESS_KEY_ID": settings.s3_access_key,
        "AWS_SECRET_ACCESS_KEY": settings.s3_secret_key,
        "AWS_REGION": settings.s3_region,
        # SeaweedFS is reached over plain HTTP inside the cluster network.
        "AWS_ALLOW_HTTP": "true",
    }


def _project(dataset: str, record: dict, schema: pa.Schema) -> dict:
    """Reduce a record to exactly the fields its table declares.

    A field the schema does not know is not archived. That is the trade the
    lakehouse makes: under JSONL a new field reached the lake by itself, here
    it has to be added to SCHEMAS deliberately. It is logged rather than
    dropped in silence, so the omission is visible in the archiver's log.
    """
    for field in record.keys() - set(schema.names):
        marker = (dataset, field)
        if marker not in _warned_unknown:
            _warned_unknown.add(marker)
            logger.warning(
                "Field %r on dataset %r is not in the table schema and will "
                "not be archived; add it to SCHEMAS in delta_writer.py",
                field,
                dataset,
            )
    return {name: record.get(name) for name in schema.names}


def build_table(
    dataset: str, records: list[dict]
) -> tuple[pa.Table | None, list[dict]]:
    """Convert records to an Arrow table, isolating any that do not fit.

    Returns `(table, rejected)`. Under JSONL any record could be written, so a
    malformed one was harmless. With a schema it is not: left unhandled, one
    bad record would fail its whole batch every time, and because the archiver
    commits Kafka offsets only after a successful flush, the same batch would
    be replayed on every retry -- a crash loop that stops archiving entirely.
    Rejecting the single record keeps the rest of the batch moving.
    """
    schema = SCHEMAS[dataset]
    rows = [_project(dataset, record, schema) for record in records]

    try:
        return pa.Table.from_pylist(rows, schema=schema), []
    except _SCHEMA_ERRORS:
        pass

    good: list[dict] = []
    rejected: list[dict] = []
    for row, record in zip(rows, records):
        try:
            pa.Table.from_pylist([row], schema=schema)
            good.append(row)
        except _SCHEMA_ERRORS as e:
            logger.error("Rejected a %s record: %s", dataset, e)
            rejected.append(record)

    table = pa.Table.from_pylist(good, schema=schema) if good else None
    return table, rejected


def append(dataset: str, table: pa.Table) -> None:
    """Append one Arrow table to its Delta table as a single commit."""
    write_deltalake(
        table_uri(dataset),
        table,
        mode="append",
        partition_by=_PARTITION_COLUMNS,
        # Additive schema changes (a new field added to SCHEMAS) extend the
        # table instead of failing the write.
        schema_mode="merge",
        storage_options=storage_options(),
    )


def current_version(dataset: str) -> int | None:
    """Latest committed version, reported by the archive status endpoint."""
    try:
        return DeltaTable(
            table_uri(dataset), storage_options=storage_options()
        ).version()
    except Exception:  # noqa: BLE001 - status detail only, never worth failing on
        return None
