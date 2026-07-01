# Dateien – Projektstruktur

## Übersicht

| Pfad | Typ | Beschreibung |
|------|-----|--------------|
| `sensor_simulator.py` | Python | Standalone Sensor-Simulator mit Kafka/File/Stdout-Output |
| `docker-compose.yml` | YAML | Lokale Entwicklungsumgebung (alle Services) |
| `.env.example` | Env | Template für Umgebungsvariablen |

## Backend (`backend/`)

| Pfad | Typ | Beschreibung |
|------|-----|--------------|
| `backend/Dockerfile` | Docker | Python 3.12 slim Image mit uvicorn |
| `backend/requirements.txt` | Pip | FastAPI, uvicorn, aiokafka, pydantic |
| `backend/app/__init__.py` | Python | Package-Marker (leer) |
| `backend/app/main.py` | Python | Haupt-API: Routes, Kafka-Integration, WebSocket, Simulator |

## Frontend (`frontend/`)

| Pfad | Typ | Beschreibung |
|------|-----|--------------|
| `frontend/Dockerfile` | Docker | nginx 1.27 alpine Image |
| `frontend/nginx.conf` | nginx | Reverse Proxy Config (API + WebSocket Proxy) |
| `frontend/public/index.html` | HTML | SPA mit Producer- und Dashboard-Tabs |
| `frontend/public/app.js` | JavaScript | Frontend-Logik: WebSocket, Chart, API-Calls |
| `frontend/public/styles.css` | CSS | Custom Styles (Buttons, Cards, Tabs, Log-Entries) |

## Flink Job (`flink-job/`)

| Pfad | Typ | Beschreibung |
|------|-----|--------------|
| `flink-job/Dockerfile` | Docker | Flink 1.18 + Python + Kafka-Connector JAR |
| `flink-job/job.py` | Python | PyFlink SQL: Aggregation, Alerting, Enrichment |
| `flink-job/sensor_metadata.json` | JSON | Stammdaten: Schwellwerte, Location-Gruppen |

## Kubernetes (`k8s/helm/iot-monitoring/`)

| Pfad | Beschreibung |
|------|--------------|
| `Chart.yaml` | Helm Chart Metadaten (v0.1.0) |
| `values.yaml` | Konfigurierbare Werte (Replicas, Ressourcen, Images) |
| `templates/_helpers.tpl` | Template-Hilfsfunktionen (Labels, Namespace) |
| `templates/configmap.yaml` | ConfigMap + Secret (Kafka-URLs, MinIO-Credentials) |
| `templates/kafka.yaml` | Kafka StatefulSet + Headless Service (KRaft) |
| `templates/minio.yaml` | MinIO StatefulSet + Bucket-Init Job |
| `templates/flink-jobmanager.yaml` | Flink JobManager Deployment + Service |
| `templates/flink-taskmanager.yaml` | Flink TaskManager Deployment + Job-Submit Job |
| `templates/backend.yaml` | Backend Deployment + Service (mit Health-Probes) |
| `templates/frontend.yaml` | Frontend Deployment + NodePort Service |

## Dokumentation (`docs/`)

| Pfad | Beschreibung |
|------|--------------|
| `docs/README.md` | Übersicht und Index |
| `docs/FILES.md` | Diese Datei – Dateistruktur |
| `docs/FUNCTIONS.md` | Funktionen und Klassen |
| `docs/IMPORTS.md` | Import-Abhängigkeiten |
| `docs/API_DOCUMENTATION.md` | API-Endpunkte |
| `docs/CONFIGURATION.md` | Konfiguration |
| `docs/QUICKSTART.md` | Schnellstart |
| `docs/K8S.md` | Kubernetes Ressourcen-Übersicht |

## Assets (`assets/`)

| Pfad | Beschreibung |
|------|--------------|
| `assets/vorschlag-iot-sensormonitoring.md` | Originaler Projektvorschlag |
