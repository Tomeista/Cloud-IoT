"""Tests for backend.app.archiver._partition_time.

Partitioning archived records by their own event time (rather than by arrival
time) is what lets a partition be considered complete once its watermark has
passed -- a replayed or late record still lands in the partition it belongs
to. _partition_time is the single point where that mapping happens, so its
per-dataset field pick and its fallback on missing/malformed values are worth
pinning down.
"""

from datetime import datetime, timezone

from backend.app.archiver import _partition_time


def test_raw_dataset_uses_event_time_with_z_suffix():
    """`raw` archives use each record's own event_time. The Z form is what
    datetime.isoformat() emits with a UTC tzinfo when normalised, and what
    the simulator writes, so it must parse."""
    result = _partition_time("raw", {"event_time": "2026-09-12T14:23:45Z"})

    assert result == datetime(2026, 9, 12, 14, 23, 45, tzinfo=timezone.utc)


def test_raw_dataset_uses_event_time_with_explicit_offset():
    """The parser also has to accept the +00:00 form -- Python's own
    datetime.isoformat() emits it and records may arrive either way."""
    result = _partition_time("raw", {"event_time": "2026-09-12T14:23:45+00:00"})

    assert result == datetime(2026, 9, 12, 14, 23, 45, tzinfo=timezone.utc)


def test_aggregates_dataset_uses_window_start_ts():
    """Aggregates key off window_start_ts, not event_time -- an aggregate row
    has no event_time and picking the wrong field would silently fall back to
    ingest time."""
    record = {
        "window_start_ts": "2026-09-12T14:20:00Z",
        "event_time": "2026-09-12T14:23:45Z",
    }

    result = _partition_time("aggregates", record)

    assert result == datetime(2026, 9, 12, 14, 20, 0, tzinfo=timezone.utc)


def test_alerts_dataset_uses_timestamp():
    """Alerts use `timestamp` as their own time field."""
    record = {"timestamp": "2026-09-12T14:25:00Z"}

    result = _partition_time("alerts", record)

    assert result == datetime(2026, 9, 12, 14, 25, 0, tzinfo=timezone.utc)


def test_late_dataset_uses_event_time():
    """The `late` dataset carries the original events that missed their
    window, so it partitions by the same event_time as `raw`."""
    record = {"event_time": "2026-09-12T14:23:45Z"}

    result = _partition_time("late", record)

    assert result == datetime(2026, 9, 12, 14, 23, 45, tzinfo=timezone.utc)


def test_missing_time_field_falls_back_to_now():
    """A record without its dataset's time field must not raise -- the
    archiver keeps ingesting, it just uses ingest time as an approximation
    of event time for that record."""
    before = datetime.now(timezone.utc)

    result = _partition_time("raw", {"sensor_id": "sensor-temp-0000"})

    after = datetime.now(timezone.utc)
    assert before <= result <= after
    assert result.tzinfo is timezone.utc


def test_malformed_time_value_falls_back_to_now():
    """A malformed timestamp string is treated the same as a missing one:
    fall back to now(), do not crash the flush."""
    before = datetime.now(timezone.utc)

    result = _partition_time("raw", {"event_time": "not-a-timestamp"})

    after = datetime.now(timezone.utc)
    assert before <= result <= after
    assert result.tzinfo is timezone.utc


def test_non_string_time_value_falls_back_to_now():
    """An integer (or any non-string) timestamp bypasses the isinstance
    guard and falls straight through to the now() fallback."""
    before = datetime.now(timezone.utc)

    result = _partition_time("raw", {"event_time": 1757683425})

    after = datetime.now(timezone.utc)
    assert before <= result <= after


def test_empty_string_time_value_falls_back_to_now():
    """An empty event_time string is falsy and skips the parse branch."""
    before = datetime.now(timezone.utc)

    result = _partition_time("raw", {"event_time": ""})

    after = datetime.now(timezone.utc)
    assert before <= result <= after


def test_unknown_dataset_falls_back_to_now():
    """An unknown dataset name maps to no field via _TIME_FIELD.get(...,'');
    record.get('') is None, so the function falls back to now() rather than
    raising a KeyError."""
    before = datetime.now(timezone.utc)

    result = _partition_time("does-not-exist", {"event_time": "2026-09-12T14:23:45Z"})

    after = datetime.now(timezone.utc)
    assert before <= result <= after
