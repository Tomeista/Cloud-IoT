"""Shared pytest configuration.

Two responsibilities:

1. Put the repo root on sys.path so tests can import top-level modules
   (`sensor_simulator`) and the `backend` package directly.

2. Stub out `kafka` before any test imports `backend.app.archiver` (which
   imports `kafka` at module scope). The functions we test in `archiver`
   are pure -- they never touch the consumer -- so pulling in the real
   kafka-python client is both unnecessary and, on Python >= 3.12, broken
   (kafka-python 2.0.2 uses a vendored six that no longer imports).
"""

import sys
import types
from pathlib import Path

# 1. sys.path setup.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


# 2. kafka stub. Only registered if the real module is not importable, so a
# machine that has a working kafka-python installed keeps using it.
def _stub_kafka() -> None:
    try:
        import kafka  # noqa: F401
        import kafka.errors  # noqa: F401
        return
    except ImportError:
        pass

    kafka_mod = types.ModuleType("kafka")
    kafka_mod.KafkaConsumer = object
    kafka_mod.KafkaProducer = object
    sys.modules["kafka"] = kafka_mod

    kafka_errors = types.ModuleType("kafka.errors")
    kafka_errors.NoBrokersAvailable = type("NoBrokersAvailable", (Exception,), {})
    sys.modules["kafka.errors"] = kafka_errors


_stub_kafka()
