"""
IoT Sensor Data Simulator

Generates synthetic sensor data (Temperature, Pressure, Humidity, Vibration)
and publishes to Kafka or outputs to stdout/file.

Schema: sensor_id, timestamp, type, value, unit, location
"""

import argparse
import json
import random
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

SENSOR_TYPES = {
    "temperature": {"min": -10.0, "max": 80.0, "unit": "°C", "drift": 0.5},
    "pressure": {"min": 900.0, "max": 1100.0, "unit": "hPa", "drift": 2.0},
    "humidity": {"min": 0.0, "max": 100.0, "unit": "%", "drift": 1.0},
    "vibration": {"min": 0.0, "max": 50.0, "unit": "mm/s", "drift": 0.8},
}

LOCATIONS = [
    "Hall-A1",
    "Hall-A2",
    "Hall-B1",
    "Hall-B2",
    "Outdoor-Storage-1",
    "Outdoor-Storage-2",
    "Office-GF",
    "Office-1F",
    "Server-Room",
    "Production-Line-1",
    "Production-Line-2",
    "Cold-Storage",
]


@dataclass
class SensorEvent:
    sensor_id: str
    timestamp: str
    type: str
    value: float
    unit: str
    location: str


class Sensor:
    """Simulates a single sensor with realistic value drift."""

    def __init__(self, sensor_id: str, type: str, location: str):
        self.sensor_id = sensor_id
        self.type = type
        self.location = location
        config = SENSOR_TYPES[type]
        self.min_val = config["min"]
        self.max_val = config["max"]
        self.unit = config["unit"]
        self.drift = config["drift"]
        # Start at a random value within range
        self.current_value = random.uniform(self.min_val, self.max_val)

    def read(self) -> SensorEvent:
        """Generate next reading with realistic drift."""
        # Random walk with mean reversion
        mid = (self.min_val + self.max_val) / 2
        reversion = (mid - self.current_value) * 0.01
        change = random.gauss(0, self.drift) + reversion
        self.current_value = max(
            self.min_val, min(self.max_val, self.current_value + change)
        )

        # Occasional spike (anomaly) with 1% probability
        if random.random() < 0.01:
            spike = random.choice([-1, 1]) * self.drift * 5
            self.current_value = max(
                self.min_val, min(self.max_val, self.current_value + spike)
            )

        return SensorEvent(
            sensor_id=self.sensor_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            type=self.type,
            value=round(self.current_value, 2),
            unit=self.unit,
            location=self.location,
        )


class SensorFleet:
    """Manages a fleet of sensors."""

    def __init__(self, num_sensors: int):
        self.sensors: list[Sensor] = []
        for i in range(num_sensors):
            sensor_type = random.choice(list(SENSOR_TYPES.keys()))
            location = random.choice(LOCATIONS)
            sensor_id = f"sensor-{sensor_type[:4]}-{i:04d}"
            self.sensors.append(Sensor(sensor_id, sensor_type, location))

    def read_all(self) -> list[SensorEvent]:
        return [s.read() for s in self.sensors]

    def read_random(self, count: int = 1) -> list[SensorEvent]:
        selected = random.sample(self.sensors, min(count, len(self.sensors)))
        return [s.read() for s in selected]


def output_stdout(events: list[SensorEvent]):
    for event in events:
        print(json.dumps(asdict(event), ensure_ascii=False))


def output_file(events: list[SensorEvent], filepath: str):
    with open(filepath, "a", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(asdict(event), ensure_ascii=False) + "\n")


def output_kafka(events: list[SensorEvent], bootstrap_servers: str, topic: str):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    for event in events:
        producer.send(topic, key=event.sensor_id, value=asdict(event))
    producer.flush()


def run_simulator(args):
    fleet = SensorFleet(args.num_sensors)
    events_per_tick = (
        max(1, args.num_sensors // 10) if args.batch_size == 0 else args.batch_size
    )

    print(
        f"Starting simulator: {args.num_sensors} sensors, "
        f"{events_per_tick} events every {args.interval}s, "
        f"output={args.output}",
        file=sys.stderr,
    )

    total_events = 0
    try:
        while True:
            events = fleet.read_random(events_per_tick)
            total_events += len(events)

            if args.output == "stdout":
                output_stdout(events)
            elif args.output == "file":
                output_file(events, args.file)
            elif args.output == "kafka":
                output_kafka(events, args.kafka_bootstrap, args.kafka_topic)

            if args.max_events > 0 and total_events >= args.max_events:
                print(
                    f"Reached max events ({args.max_events}). Stopping.",
                    file=sys.stderr,
                )
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print(f"\nStopped. Total events generated: {total_events}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="IoT Sensor Data Simulator")
    parser.add_argument(
        "--num-sensors",
        type=int,
        default=100,
        help="Number of sensors in the fleet (default: 100)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between event batches (default: 1.0)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Events per tick (default: num_sensors/10)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after N events (0 = unlimited)",
    )
    parser.add_argument(
        "--output",
        choices=["stdout", "file", "kafka"],
        default="stdout",
        help="Output destination (default: stdout)",
    )
    parser.add_argument(
        "--file",
        type=str,
        default="sensor_data.jsonl",
        help="Output file path (when --output=file)",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--kafka-topic",
        type=str,
        default="sensor-events",
        help="Kafka topic name",
    )

    args = parser.parse_args()
    run_simulator(args)


if __name__ == "__main__":
    main()
