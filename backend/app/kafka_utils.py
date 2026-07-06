import json
import logging
import threading
import time
from collections import deque

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from .config import settings

logger = logging.getLogger(__name__)

_producer: KafkaProducer | None = None
_producer_lock = threading.Lock()


def get_producer() -> KafkaProducer | None:
    global _producer
    if _producer is not None:
        return _producer
    with _producer_lock:
        if _producer is not None:
            return _producer
        try:
            _producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=3,
            )
            logger.info("Kafka producer connected")
        except NoBrokersAvailable:
            logger.warning("Kafka not available, running without message broker")
            _producer = None
    return _producer


def produce_event(topic: str, key: str, value: dict) -> bool:
    producer = get_producer()
    if producer is None:
        return False
    try:
        producer.send(topic, key=key, value=value)
        producer.flush()
        return True
    except Exception as e:
        logger.error(f"Failed to produce message: {e}")
        return False


# In-memory stores for consumed results (populated by background consumer)
aggregates_store: deque = deque(maxlen=1000)
alerts_store: deque = deque(maxlen=500)


def _consume_results():
    """Background thread that consumes processed results from Kafka."""
    while True:
        try:
            consumer = KafkaConsumer(
                settings.kafka_aggregates_topic,
                settings.kafka_alerts_topic,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="backend-serving",
                auto_offset_reset="latest",
                consumer_timeout_ms=1000,
            )
            logger.info("Kafka consumer connected for results")
            while True:
                records = consumer.poll(timeout_ms=1000)
                for topic_partition, messages in records.items():
                    for msg in messages:
                        if topic_partition.topic == settings.kafka_aggregates_topic:
                            aggregates_store.append(msg.value)
                        elif topic_partition.topic == settings.kafka_alerts_topic:
                            alerts_store.append(msg.value)
        except NoBrokersAvailable:
            logger.warning("Kafka not available for consumer, retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Consumer error: {e}, retrying in 5s...")
            time.sleep(5)


def start_consumer_thread():
    thread = threading.Thread(target=_consume_results, daemon=True)
    thread.start()
    logger.info("Started background Kafka consumer thread")
