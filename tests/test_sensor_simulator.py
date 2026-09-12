"""Tests for sensor_simulator.load_catalogue.

The simulator shares its sensor catalogue with the Flink job so that every
emitted sensor_id resolves to a real group/description at enrichment time.
load_catalogue is the single point where that catalogue is read from disk, so
its fallback behaviour (missing file, malformed JSON) and its filter (drop
entries the simulator cannot instantiate a Sensor from) are worth pinning
down.
"""

import json

import sensor_simulator


def test_missing_file_returns_empty_dict(tmp_path):
    """A path that does not exist must not raise -- the simulator falls back
    to a randomly generated fleet in that case."""
    missing = tmp_path / "does-not-exist.json"

    result = sensor_simulator.load_catalogue(str(missing))

    assert result == {}


def test_malformed_json_returns_empty_dict(tmp_path):
    """Malformed JSON is treated the same as a missing file: log and fall back,
    never crash the simulator on startup."""
    path = tmp_path / "broken.json"
    path.write_text("{not valid json", encoding="utf-8")

    result = sensor_simulator.load_catalogue(str(path))

    assert result == {}


def test_valid_catalogue_is_returned(tmp_path):
    """A well-formed catalogue comes back as {sensor_id: meta}."""
    path = tmp_path / "metadata.json"
    path.write_text(
        json.dumps(
            {
                "sensors": {
                    "sensor-temp-0000": {
                        "sensor_type": "temperature",
                        "location": "Hall-A1",
                    },
                    "sensor-humi-0001": {
                        "sensor_type": "humidity",
                        "location": "Cold-Storage",
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    result = sensor_simulator.load_catalogue(str(path))

    assert set(result.keys()) == {"sensor-temp-0000", "sensor-humi-0001"}
    assert result["sensor-temp-0000"]["sensor_type"] == "temperature"
    assert result["sensor-temp-0000"]["location"] == "Hall-A1"


def test_entries_with_unknown_sensor_type_are_dropped(tmp_path):
    """SensorFleet cannot build a Sensor for a type absent from SENSOR_TYPES,
    so load_catalogue filters those entries out before they reach the fleet."""
    path = tmp_path / "metadata.json"
    path.write_text(
        json.dumps(
            {
                "sensors": {
                    "sensor-temp-0000": {
                        "sensor_type": "temperature",
                        "location": "Hall-A1",
                    },
                    "sensor-mystery-0001": {
                        "sensor_type": "radioactivity",
                        "location": "Hall-A1",
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    result = sensor_simulator.load_catalogue(str(path))

    assert list(result.keys()) == ["sensor-temp-0000"]


def test_entries_with_missing_location_are_dropped(tmp_path):
    """Sensor requires a location string; entries without one are filtered."""
    path = tmp_path / "metadata.json"
    path.write_text(
        json.dumps(
            {
                "sensors": {
                    "sensor-temp-0000": {
                        "sensor_type": "temperature",
                        "location": "Hall-A1",
                    },
                    "sensor-temp-0001": {
                        "sensor_type": "temperature",
                        "location": "",
                    },
                    "sensor-temp-0002": {
                        "sensor_type": "temperature",
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    result = sensor_simulator.load_catalogue(str(path))

    assert list(result.keys()) == ["sensor-temp-0000"]


def test_missing_sensors_key_returns_empty_dict(tmp_path):
    """A JSON file without a top-level `sensors` key is valid JSON but has no
    catalogue to load -- the .get('sensors', {}) fallback should kick in."""
    path = tmp_path / "no-sensors-key.json"
    path.write_text(json.dumps({"default_thresholds": {}}), encoding="utf-8")

    result = sensor_simulator.load_catalogue(str(path))

    assert result == {}
