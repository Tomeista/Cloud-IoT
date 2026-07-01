# Konfiguration

## Umgebungsvariablen

### Backend (FastAPI)

| Variable | Default | Beschreibung |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka-Broker Adresse(n) |
| `KAFKA_EVENTS_TOPIC` | `sensor-events` | Topic für Roh-Events |
| `KAFKA_AGGREGATES_TOPIC` | `sensor-aggregates` | Topic für Flink-Aggregate |
| `KAFKA_ALERTS_TOPIC` | `sensor-alerts` | Topic für Flink-Alerts |
| `MINIO_ENDPOINT` | `minio:9000` | MinIO S3-Endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO Zugangsdaten |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO Passwort |

### Flink Job

| Variable | Default | Beschreibung |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka-Broker für Flink |
| `KAFKA_EVENTS_TOPIC` | `sensor-events` | Input-Topic |
| `KAFKA_AGGREGATES_TOPIC` | `sensor-aggregates` | Output-Topic Aggregate |
| `KAFKA_ALERTS_TOPIC` | `sensor-alerts` | Output-Topic Alerts |
| `CHECKPOINT_INTERVAL_MS` | `60000` | Flink Checkpoint-Intervall (ms) |
| `SENSOR_METADATA_PATH` | `/opt/flink/sensor_metadata.json` | Pfad zu Stammdaten |

### Sensor Simulator (CLI)

| Argument | Default | Beschreibung |
|----------|---------|-------------|
| `--num-sensors` | `100` | Anzahl Sensoren in der Fleet |
| `--interval` | `1.0` | Sekunden zwischen Batches |
| `--batch-size` | `0` | Events pro Tick (0 = num_sensors/10) |
| `--max-events` | `0` | Stop nach N Events (0 = unbegrenzt) |
| `--output` | `stdout` | Ziel: stdout, file, kafka |
| `--file` | `sensor_data.jsonl` | Ausgabedatei |
| `--kafka-bootstrap` | `localhost:9092` | Kafka-Server |
| `--kafka-topic` | `sensor-events` | Kafka-Topic |

---

## Kafka Topics

| Topic | Producer | Consumer | Format |
|-------|----------|----------|--------|
| `sensor-events` | Backend / Simulator | Flink | JSON |
| `sensor-aggregates` | Flink | Backend | JSON (ISO-8601 Timestamps) |
| `sensor-alerts` | Flink | Backend | JSON (ISO-8601 Timestamps) |

Alle Topics werden bei Bedarf automatisch erstellt (`auto.create.topics.enable=true`).

---

## Alert-Schwellwerte

Definiert in `flink-job/sensor_metadata.json`:

| Sensortyp | High | Low | Normal Range |
|-----------|------|-----|-------------|
| temperature | 60.0 °C | -5.0 °C | 15.0 – 35.0 |
| pressure | 1050.0 hPa | 950.0 hPa | 980.0 – 1020.0 |
| humidity | 85.0 % | 15.0 % | 30.0 – 60.0 |
| vibration | 35.0 mm/s | 0.0 mm/s | 0.0 – 15.0 |

**Severity-Berechnung:**
- `WARNING`: max_value > threshold_high
- `CRITICAL`: max_value > threshold_high × 1.2

---

## Kubernetes-Konfiguration (Helm values.yaml)

### Skalierung

| Komponente | Default Replicas | Skalierbar |
|------------|-----------------|-----------|
| Kafka | 1 | StatefulSet (manuell) |
| MinIO | 1 | StatefulSet (manuell) |
| Flink JobManager | 1 | Nein (singleton) |
| Flink TaskManager | 1 | Ja (`kubectl scale`) |
| Backend | 1 | Ja (horizontal) |
| Frontend | 1 | Ja (horizontal) |

### Ressourcen-Limits (Default)

| Komponente | Memory Request | Memory Limit | CPU Request | CPU Limit |
|------------|---------------|-------------|-------------|-----------|
| Kafka | 512Mi | 1Gi | 250m | 500m |
| MinIO | 256Mi | 512Mi | 100m | 250m |
| Flink JM | 512Mi | 1Gi | 250m | 500m |
| Flink TM | 512Mi | 1Gi | 250m | 500m |
| Backend | 128Mi | 256Mi | 100m | 250m |
| Frontend | 64Mi | 128Mi | 50m | 100m |

**Gesamt-Minimum**: ~2 GiB RAM, 1 vCPU

### Persistenz

| Komponente | PVC Size | Pfad |
|------------|----------|------|
| Kafka | 5Gi | `/bitnami/kafka` |
| MinIO | 5Gi | `/data` |

---

## Docker Compose Ports

| Service | Interner Port | Externer Port | Beschreibung |
|---------|--------------|---------------|-------------|
| Frontend | 80 | 3000 | Web-UI |
| Backend | 8000 | 8000 | API (direkt) |
| Kafka | 9092/9094 | 9094 | Kafka (externer Listener) |
| Flink UI | 8081 | 8081 | Flink Dashboard |
| MinIO API | 9000 | 9000 | S3-kompatible API |
| MinIO Console | 9001 | 9001 | MinIO Web-UI |
