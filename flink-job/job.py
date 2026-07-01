"""
Apache Flink Streaming Job – IoT Sensor Processing

Reads sensor events from Kafka, performs:
  1. Tumbling-window aggregation (1 minute)
  2. Threshold-based alerting
  3. Enrichment with static sensor metadata

Writes results to Kafka output topics:
  - sensor-aggregates: windowed statistics per sensor
  - sensor-alerts:     threshold violations
"""

import json
import os
import logging

from pyflink.table import EnvironmentSettings, TableEnvironment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flink-job")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
EVENTS_TOPIC = os.getenv("KAFKA_EVENTS_TOPIC", "sensor-events")
AGGREGATES_TOPIC = os.getenv("KAFKA_AGGREGATES_TOPIC", "sensor-aggregates")
ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "sensor-alerts")
CHECKPOINT_INTERVAL = os.getenv("CHECKPOINT_INTERVAL_MS", "60000")
METADATA_PATH = os.getenv("SENSOR_METADATA_PATH", "/opt/flink/sensor_metadata.json")

# Alert thresholds (loaded from metadata or defaults)
THRESHOLDS = {
    "temperature": {"high": 60.0, "low": -5.0},
    "pressure": {"high": 1050.0, "low": 950.0},
    "humidity": {"high": 85.0, "low": 15.0},
    "vibration": {"high": 35.0, "low": 0.0},
}


def load_metadata():
    """Load sensor metadata for enrichment."""
    try:
        with open(METADATA_PATH, encoding="utf-8") as f:
            meta = json.load(f)
        # Update thresholds from metadata
        for stype, info in meta.get("sensor_types", {}).items():
            if stype in THRESHOLDS:
                THRESHOLDS[stype]["high"] = info.get(
                    "alert_threshold_high", THRESHOLDS[stype]["high"]
                )
                THRESHOLDS[stype]["low"] = info.get(
                    "alert_threshold_low", THRESHOLDS[stype]["low"]
                )
        logger.info("Loaded sensor metadata from %s", METADATA_PATH)
        return meta
    except FileNotFoundError:
        logger.warning("Metadata file not found at %s, using defaults", METADATA_PATH)
        return {}


def main():
    metadata = load_metadata()

    # Build location-group mapping for enrichment SQL
    loc_map = metadata.get("locations", {})
    location_cases = "\n".join(
        f"        WHEN location = '{loc}' THEN '{info.get('group', 'Unknown')}'"
        for loc, info in loc_map.items()
    )
    if not location_cases:
        location_cases = "        WHEN 1=0 THEN 'none'"

    # Build threshold cases for alerting SQL
    threshold_high_cases = "\n".join(
        f"        WHEN sensor_type = '{st}' THEN {th['high']}"
        for st, th in THRESHOLDS.items()
    )

    # ── Flink Table Environment ──────────────────────────────────────────
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # Checkpoint configuration
    t_env.get_config().set("execution.checkpointing.interval", CHECKPOINT_INTERVAL)
    t_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")
    t_env.get_config().set(
        "execution.checkpointing.min-pause", "10000"
    )
    t_env.get_config().set("parallelism.default", "1")

    # ── Kafka source: sensor-events ──────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE sensor_events (
            sensor_id     STRING,
            `timestamp`   STRING,
            `type`        STRING,
            `value`       DOUBLE,
            unit          STRING,
            location      STRING,
            event_time AS TO_TIMESTAMP(
                REPLACE(SUBSTRING(`timestamp`, 1, 19), 'T', ' ')
            ),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector'                        = 'kafka',
            'topic'                            = '{EVENTS_TOPIC}',
            'properties.bootstrap.servers'     = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'              = 'flink-processor',
            'scan.startup.mode'                = 'latest-offset',
            'format'                           = 'json',
            'json.fail-on-missing-field'       = 'false',
            'json.ignore-parse-errors'         = 'true'
        )
    """)

    # ── Kafka sink: sensor-aggregates ────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE sensor_aggregates (
            sensor_id     STRING,
            sensor_type   STRING,
            location      STRING,
            location_group STRING,
            window_start  TIMESTAMP(3),
            window_end    TIMESTAMP(3),
            avg_value     DOUBLE,
            min_value     DOUBLE,
            max_value     DOUBLE,
            event_count   BIGINT
        ) WITH (
            'connector'                        = 'kafka',
            'topic'                            = '{AGGREGATES_TOPIC}',
            'properties.bootstrap.servers'     = '{KAFKA_BOOTSTRAP}',
            'format'                           = 'json',
            'json.timestamp-format.standard'   = 'ISO-8601'
        )
    """)

    # ── Kafka sink: sensor-alerts ────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE sensor_alerts (
            sensor_id     STRING,
            sensor_type   STRING,
            location      STRING,
            location_group STRING,
            window_start  TIMESTAMP(3),
            window_end    TIMESTAMP(3),
            max_value     DOUBLE,
            threshold     DOUBLE,
            severity      STRING
        ) WITH (
            'connector'                        = 'kafka',
            'topic'                            = '{ALERTS_TOPIC}',
            'properties.bootstrap.servers'     = '{KAFKA_BOOTSTRAP}',
            'format'                           = 'json',
            'json.timestamp-format.standard'   = 'ISO-8601'
        )
    """)

    # ── Create aggregation view (enriched) ───────────────────────────────
    t_env.execute_sql(f"""
        CREATE VIEW enriched_aggregates AS
        SELECT
            sensor_id,
            `type`     AS sensor_type,
            location,
            CASE
{location_cases}
                ELSE 'Unknown'
            END        AS location_group,
            window_start,
            window_end,
            AVG(`value`)   AS avg_value,
            MIN(`value`)   AS min_value,
            MAX(`value`)   AS max_value,
            COUNT(*)       AS event_count
        FROM TABLE(
            TUMBLE(TABLE sensor_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        GROUP BY
            sensor_id, `type`, location, window_start, window_end
    """)

    # ── Multi-statement insert: aggregates + alerts ──────────────────────
    stmt_set = t_env.create_statement_set()

    # 1) Write aggregated results
    stmt_set.add_insert_sql("""
        INSERT INTO sensor_aggregates
        SELECT * FROM enriched_aggregates
    """)

    # 2) Write alerts (threshold violations)
    stmt_set.add_insert_sql(f"""
        INSERT INTO sensor_alerts
        SELECT
            sensor_id,
            sensor_type,
            location,
            location_group,
            window_start,
            window_end,
            max_value,
            CASE
{threshold_high_cases}
                ELSE 999.0
            END AS threshold,
            CASE
                WHEN max_value > CASE
{threshold_high_cases}
                    ELSE 999.0
                END * 1.2 THEN 'CRITICAL'
                ELSE 'WARNING'
            END AS severity
        FROM enriched_aggregates
        WHERE max_value > CASE
{threshold_high_cases}
            ELSE 999.0
        END
    """)

    logger.info("Submitting Flink job...")
    stmt_set.execute().wait()


if __name__ == "__main__":
    main()
