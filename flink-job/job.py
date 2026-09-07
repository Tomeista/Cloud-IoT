"""
IoT Sensor Monitoring - Flink Stream Processing Job

Reads sensor events from Kafka, performs:
1. Tumbling window aggregation (1 minute) per sensor
2. Stateful threshold alerting (sustained breaches)
3. Enrichment with static sensor metadata

Writes results to Kafka output topics.
"""

import json
import logging
import os
import time

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.environ.get("KAFKA_EVENTS_TOPIC", "sensor-events")
AGGREGATES_TOPIC = os.environ.get("KAFKA_AGGREGATES_TOPIC", "sensor-aggregates")
ALERTS_TOPIC = os.environ.get("KAFKA_ALERTS_TOPIC", "sensor-alerts")
LATE_TOPIC = os.environ.get("KAFKA_LATE_TOPIC", "sensor-late-events")
METADATA_PATH = os.environ.get("METADATA_PATH", "/opt/flink/job/metadata.json")

# How long a window stays open for stragglers after its watermark has passed.
# Events later than this are dropped from the window and diverted to the late
# side output instead, so "how much did we miss" stays measurable.
ALLOWED_LATENESS_MS = int(os.environ.get("ALLOWED_LATENESS_SECONDS", "30")) * 1000

# Events carry event_time, so a partition that momentarily has no traffic must
# not hold the watermark back for everyone else.
SOURCE_IDLE_TIMEOUT_S = int(os.environ.get("SOURCE_IDLE_TIMEOUT_SECONDS", "10"))

CHECKPOINT_INTERVAL_MS = int(os.environ.get("CHECKPOINT_INTERVAL_SECONDS", "30")) * 1000

# A parallelism set on the environment overrides `parallelism.default` from
# flink-conf.yaml, so hardcoding it here would silently ignore the Helm value.
# Raising it past the input topic's partition count adds idle subtasks rather
# than throughput -- see kafka.numPartitions.
PARALLELISM = int(os.environ.get("FLINK_PARALLELISM", "2"))

# Side channel for events that arrive after their window has already been
# emitted and closed.
LATE_EVENTS_TAG = OutputTag("late-events", Types.STRING())


# Load sensor metadata for enrichment
def load_metadata():
    try:
        with open(METADATA_PATH, "r") as f:
            data = json.load(f)
            return data.get("sensors", {}), data.get("default_thresholds", {})
    except FileNotFoundError:
        logger.warning(f"Metadata file not found at {METADATA_PATH}, using defaults")
        return {}, {
            "temperature": {"warning": 60.0, "critical": 70.0},
            "pressure": {"warning": 1050.0, "critical": 1080.0},
            "humidity": {"warning": 80.0, "critical": 90.0},
            "vibration": {"warning": 30.0, "critical": 40.0},
        }


SENSOR_METADATA, DEFAULT_THRESHOLDS = load_metadata()


class EventTimestampAssigner(TimestampAssigner):
    """Extracts event_time from sensor events for event-time processing."""

    def extract_timestamp(self, value, record_timestamp):
        try:
            from datetime import datetime

            event = json.loads(value)
            dt = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            return record_timestamp


class AlertingFunction(KeyedProcessFunction):
    """
    Stateful alerting: triggers alerts only when threshold is breached
    for a sustained number of consecutive readings (not just a single spike).
    """

    CONSECUTIVE_THRESHOLD = 3  # Number of consecutive breaches before alerting

    def open(self, runtime_context: RuntimeContext):
        self.breach_count = runtime_context.get_state(
            ValueStateDescriptor("breach_count", Types.INT())
        )
        self.last_alert_time = runtime_context.get_state(
            ValueStateDescriptor("last_alert_time", Types.LONG())
        )

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        event = json.loads(value)
        sensor_id = event.get("sensor_id", "unknown")
        sensor_type = event.get("sensor_type", "unknown")
        val = event.get("value", 0)

        # Get thresholds from metadata or defaults
        if sensor_id in SENSOR_METADATA:
            warning_thresh = SENSOR_METADATA[sensor_id].get(
                "warning_threshold",
                DEFAULT_THRESHOLDS.get(sensor_type, {}).get("warning", float("inf")),
            )
            critical_thresh = SENSOR_METADATA[sensor_id].get(
                "critical_threshold",
                DEFAULT_THRESHOLDS.get(sensor_type, {}).get("critical", float("inf")),
            )
        else:
            warning_thresh = DEFAULT_THRESHOLDS.get(sensor_type, {}).get(
                "warning", float("inf")
            )
            critical_thresh = DEFAULT_THRESHOLDS.get(sensor_type, {}).get(
                "critical", float("inf")
            )

        current_count = self.breach_count.value() or 0

        if val >= warning_thresh:
            current_count += 1
            self.breach_count.update(current_count)

            # Only alert after sustained breaches
            if current_count >= self.CONSECUTIVE_THRESHOLD:
                last_time = self.last_alert_time.value() or 0
                now_ms = ctx.timestamp() or int(time.time() * 1000)

                # Don't re-alert within 30 seconds
                if now_ms - last_time > 30000:
                    severity = "critical" if val >= critical_thresh else "warning"
                    alert = {
                        "sensor_id": sensor_id,
                        "sensor_type": sensor_type,
                        "location": event.get("location", "unknown"),
                        # Carried over from the enrichment step so the alert is
                        # readable without a second lookup in the catalogue.
                        "group": event.get("group", "Unknown"),
                        "description": event.get("description", ""),
                        "value": val,
                        "threshold": warning_thresh,
                        "timestamp": event.get("event_time", ""),
                        "severity": severity,
                        "consecutive_breaches": current_count,
                    }
                    self.last_alert_time.update(now_ms)
                    yield json.dumps(alert)
        else:
            # Reset counter when value returns to normal
            self.breach_count.update(0)


def enrich_event(value: str) -> str:
    """Enriches an event with static sensor metadata."""
    event = json.loads(value)
    sensor_id = event.get("sensor_id", "")

    if sensor_id in SENSOR_METADATA:
        meta = SENSOR_METADATA[sensor_id]
        event["group"] = meta.get("group", "Unknown")
        event["description"] = meta.get("description", "")
    else:
        event["group"] = "Unknown"
        event["description"] = ""

    return json.dumps(event)


class WindowAggregateFunction(ProcessWindowFunction):
    """Computes aggregate statistics over a tumbling window per sensor."""

    def process(self, key, context: ProcessWindowFunction.Context, elements):
        values = []
        sensor_type = "unknown"
        location = "unknown"
        group = "Unknown"
        description = ""

        for elem in elements:
            event = json.loads(elem)
            values.append(event.get("value", 0))
            sensor_type = event.get("sensor_type", sensor_type)
            location = event.get("location", location)
            # Added by enrich_event upstream; kept on the aggregate so the
            # enrichment survives into the serving layer and the lake.
            group = event.get("group", group)
            description = event.get("description", description)

        if not values:
            return

        window = context.window()
        from datetime import datetime, timezone

        start_dt = datetime.fromtimestamp(window.start / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(window.end / 1000, tz=timezone.utc)

        aggregate = {
            # Short labels the dashboard renders on chart axes and tables.
            "window_start": start_dt.strftime("%H:%M"),
            "window_end": end_dt.strftime("%H:%M"),
            # Full timestamps: the archived records need an unambiguous date to
            # be partitioned by event time and to stay readable in the lake.
            "window_start_ts": start_dt.isoformat(),
            "window_end_ts": end_dt.isoformat(),
            "sensor_id": key,
            "sensor_type": sensor_type,
            "location": location,
            "group": group,
            "description": description,
            "avg_value": round(sum(values) / len(values), 2),
            "min_value": round(min(values), 2),
            "max_value": round(max(values), 2),
            "event_count": len(values),
        }

        yield json.dumps(aggregate)


def build_kafka_sink(topic: str) -> KafkaSink:
    """A JSON-string Kafka sink for one of the job's result topics."""
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def main():
    logger.info("Starting IoT Sensor Monitoring Flink Job")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    logger.info(f"Input topic: {INPUT_TOPIC}")
    logger.info(f"Output topics: {AGGREGATES_TOPIC}, {ALERTS_TOPIC}, {LATE_TOPIC}")

    # Set up execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)

    # Without checkpoints the alerting state (breach counters) and the Kafka
    # offsets are lost whenever a TaskManager restarts, and the source resumes
    # at the latest offset -- so a restart would silently drop events and reset
    # every counter mid-breach.
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)

    # Add Kafka connector JAR
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")

    # Kafka source
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(INPUT_TOPIC)
        .set_group_id("flink-iot-processor")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Event-time watermarks tolerating 15s of out-of-orderness. The idleness
    # timeout matters as soon as parallelism exceeds the partition count: a
    # source subtask without partitions never emits a watermark, and because
    # the downstream watermark is the minimum across all subtasks, the windows
    # would never fire at all.
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(15))
        .with_timestamp_assigner(EventTimestampAssigner())
        .with_idleness(Duration.of_seconds(SOURCE_IDLE_TIMEOUT_S))
    )

    # Create stream from Kafka source
    events_stream = env.from_source(source, watermark_strategy, "Kafka Sensor Events")

    # Enrich events with metadata
    enriched_stream = events_stream.map(enrich_event, output_type=Types.STRING())

    # Branch 1: Windowed aggregation (1-minute tumbling windows)
    keyed_stream = enriched_stream.key_by(
        lambda x: json.loads(x).get("sensor_id", "unknown")
    )

    # Windows stay open for ALLOWED_LATENESS_MS past the watermark so moderately
    # late events still update their window; anything later is diverted to the
    # side output rather than silently discarded.
    aggregates_stream = (
        keyed_stream.window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .allowed_lateness(ALLOWED_LATENESS_MS)
        .side_output_late_data(LATE_EVENTS_TAG)
        .process(WindowAggregateFunction(), output_type=Types.STRING())
    )

    late_stream = aggregates_stream.get_side_output(LATE_EVENTS_TAG)

    # Branch 2: Stateful alerting
    alerts_stream = enriched_stream.key_by(
        lambda x: json.loads(x).get("sensor_id", "unknown")
    ).process(AlertingFunction(), output_type=Types.STRING())

    # Kafka sinks
    aggregates_stream.sink_to(build_kafka_sink(AGGREGATES_TOPIC))
    alerts_stream.sink_to(build_kafka_sink(ALERTS_TOPIC))
    late_stream.sink_to(build_kafka_sink(LATE_TOPIC))

    env.execute("IoT Sensor Monitoring Pipeline")


if __name__ == "__main__":
    main()
