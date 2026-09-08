"""Read path over the Delta tables.

The counterpart to `delta_writer`, and the reason the lakehouse is worth having:
the serving endpoints answer from the stream and therefore only know the recent
past, while these queries reach the full history in the lake.

Like the writer, this imports pyarrow and is therefore only ever loaded in the
archiver pod. nginx routes `/api/history` there alongside `/api/archive/`
([`frontend/nginx.conf`](frontend/nginx.conf)); the serving replicas stay lean.
"""

import logging

import pyarrow.compute as pc
from deltalake import DeltaTable

from .archiver import _TIME_FIELD as TIME_FIELD
from .delta_writer import SCHEMAS, storage_options, table_uri

logger = logging.getLogger(__name__)

MAX_LIMIT = 1000


class TableMissing(Exception):
    """Raised when a dataset has not been committed to yet."""


def datasets() -> list[str]:
    return list(SCHEMAS)


def query(
    dataset: str,
    dt: str | None = None,
    sensor_id: str | None = None,
    limit: int = 100,
) -> dict:
    """Read rows back out of one Delta table, newest first.

    `dt` is pushed down as a partition filter rather than applied afterwards,
    so a query for one day reads only that day's Parquet files instead of the
    whole table -- the payoff of partitioning by event time.
    """
    limit = max(1, min(limit, MAX_LIMIT))

    try:
        table = DeltaTable(table_uri(dataset), storage_options=storage_options())
    except Exception as e:  # noqa: BLE001 - table absent until the first commit
        raise TableMissing(str(e)) from e

    partitions = [("dt", "=", dt)] if dt else None
    arrow = table.to_pyarrow_table(partitions=partitions)

    if sensor_id and arrow.num_rows:
        arrow = arrow.filter(pc.equal(arrow["sensor_id"], sensor_id))

    # Newest first. The time fields are ISO-8601 in UTC, so a plain string sort
    # is chronological -- the same property that lets Parquet's min/max
    # statistics prune on them.
    time_field = TIME_FIELD.get(dataset)
    if time_field and time_field in arrow.column_names and arrow.num_rows:
        arrow = arrow.sort_by([(time_field, "descending")])

    total = arrow.num_rows
    rows = arrow.slice(0, limit).to_pylist()

    return {
        "dataset": dataset,
        "version": table.version(),
        "partition": dt,
        "matched": total,
        "returned": len(rows),
        "rows": rows,
    }
