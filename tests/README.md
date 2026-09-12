# Tests

Unit tests for the Cloud-IoT project.

## Running

From the repo root:

```
pip install -r requirements-dev.txt
pytest
```

pytest discovers `tests/test_*.py` automatically. Individual files or tests:

```
pytest tests/test_sensor_simulator.py
pytest tests/test_sensor_simulator.py::test_malformed_json_returns_empty_dict
```

## Layout

- `tests/conftest.py` — shared pytest setup: puts the repo root on `sys.path`
  and stubs `kafka` when it is not importable, so tests that touch
  `backend.app.archiver` do not need a working kafka-python client.
- `tests/test_sensor_simulator.py` — `sensor_simulator.load_catalogue`: file
  fallbacks and the sensor-type / location filter.
- `tests/test_archiver_partition_time.py` — `backend.app.archiver._partition_time`:
  per-dataset time-field pick and fallbacks on missing/malformed values.

Tests import project modules directly; the shared `conftest.py` handles the
`sys.path` entry so no package installation step is needed.
