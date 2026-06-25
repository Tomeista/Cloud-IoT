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

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (
    StreamExecutionEnvironment,
    TimeCharacteristic,
)
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
METADATA_PATH = os.environ.get("METADATA_PATH", "/opt/flink/job/metadata.json")


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

        for elem in elements:
            event = json.loads(elem)
            values.append(event.get("value", 0))
            sensor_type = event.get("sensor_type", sensor_type)
            location = event.get("location", location)

        if not values:
            return

        window = context.window()
        from datetime import datetime, timezone

        aggregate = {
            "window_start": datetime.fromtimestamp(
                window.start / 1000, tz=timezone.utc
            ).strftime("%H:%M"),
            "window_end": datetime.fromtimestamp(
                window.end / 1000, tz=timezone.utc
            ).strftime("%H:%M"),
            "sensor_id": key,
            "sensor_type": sensor_type,
            "location": location,
            "avg_value": round(sum(values) / len(values), 2),
            "min_value": round(min(values), 2),
            "max_value": round(max(values), 2),
            "event_count": len(values),
        }

        yield json.dumps(aggregate)


def main():
    logger.info("Starting IoT Sensor Monitoring Flink Job")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
    logger.info(f"Input topic: {INPUT_TOPIC}")
    logger.info(f"Output topics: {AGGREGATES_TOPIC}, {ALERTS_TOPIC}")

    # Set up execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

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

    # Watermark strategy with 15 seconds allowed lateness
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(15)
    ).with_timestamp_assigner(EventTimestampAssigner())

    # Create stream from Kafka source
    events_stream = env.from_source(source, watermark_strategy, "Kafka Sensor Events")

    # Enrich events with metadata
    enriched_stream = events_stream.map(enrich_event, output_type=Types.STRING())

    # Branch 1: Windowed aggregation (1-minute tumbling windows)
    keyed_stream = enriched_stream.key_by(
        lambda x: json.loads(x).get("sensor_id", "unknown")
    )

    aggregates_stream = keyed_stream.window(
        TumblingEventTimeWindows.of(Duration.of_minutes(1))
    ).process(WindowAggregateFunction(), output_type=Types.STRING())

    # Branch 2: Stateful alerting
    alerts_stream = enriched_stream.key_by(
        lambda x: json.loads(x).get("sensor_id", "unknown")
    ).process(AlertingFunction(), output_type=Types.STRING())

    # Kafka sinks
    aggregates_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(AGGREGATES_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    alerts_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(ALERTS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    aggregates_stream.sink_to(aggregates_sink)
    alerts_stream.sink_to(alerts_sink)

    env.execute("IoT Sensor Monitoring Pipeline")


if __name__ == "__main__":
    main()
