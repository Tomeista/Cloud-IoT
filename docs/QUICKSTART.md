# Schnellstart

## Voraussetzungen

- **Docker** + **Docker Compose** (v2) installiert
- ~4 GB freier RAM (für alle Services)
- Ports frei: 3000, 8000, 8081, 9000, 9001, 9094

## 1. Repository klonen

```bash
git clone <repo-url>
cd Cloud-IoT
```

## 2. Services starten

```bash
docker compose up -d --build
```

Erster Start dauert einige Minuten (Images bauen, Kafka starten).

## 3. Status prüfen

```bash
docker compose ps
```

Alle Services sollten `Up` zeigen. Kafka braucht ~15-20s zum Starten.

## 4. Web-UI öffnen

**http://localhost:3000**

### Producer-Tab:
1. Simulator starten (Button "▶ Starten")
2. Events erscheinen im Live Log

### Dashboard-Tab:
1. Warten bis Flink-Job läuft (~30s nach Start)
2. Aggregierte Werte und Alerts erscheinen automatisch

## 5. Weitere UIs

| URL | Service |
|-----|---------|
| http://localhost:3000 | Web-UI (Frontend) |
| http://localhost:8000/docs | FastAPI Swagger UI |
| http://localhost:8081 | Flink Web Dashboard |
| http://localhost:9001 | MinIO Console (minioadmin/minioadmin) |

## 6. Sensor-Simulator direkt nutzen

```bash
# Standalone auf stdout
python sensor_simulator.py --num-sensors 10 --interval 0.5

# Direkt nach Kafka (Docker muss laufen)
python sensor_simulator.py --output kafka --kafka-bootstrap localhost:9094
```

## 7. Stoppen

```bash
docker compose down        # Services stoppen
docker compose down -v     # + Volumes löschen (Clean State)
```

## Troubleshooting

| Problem | Lösung |
|---------|--------|
| Frontend zeigt "API offline" | Backend noch nicht bereit → 10s warten |
| Keine Aggregates/Alerts | Flink Job braucht ~30s nach Start |
| Kafka healthcheck failed | Mehr RAM zuweisen, Container Restart abwarten |
| Port bereits belegt | Ports in `docker-compose.yml` anpassen |
| Frontend-Änderung nicht sichtbar | Ctrl+Shift+R (Hard Refresh) |
