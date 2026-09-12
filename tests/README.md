# Tests

Unit tests for the Cloud-IoT project. Currently covers the sensor simulator's
catalogue loader; more modules to follow.

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

- `tests/test_sensor_simulator.py` — `sensor_simulator.load_catalogue`.

Tests at the repo root import project modules directly (each file prepends the
repo root to `sys.path`). No package installation step is needed to run them.
