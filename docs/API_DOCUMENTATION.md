# API-Dokumentation

## Base URL

- **Lokal (Docker Compose)**: `http://localhost:3000/api/` (via nginx Proxy)
- **Direkt**: `http://localhost:8000/api/`
- **Kubernetes**: `http://<node-ip>:30080/api/`

---

## REST-Endpunkte

### Health

#### `GET /api/health`

Prüft den Status der API und der Kafka-Verbindung.

**Response:**
```json
{
  "status": "ok",
  "kafka_connected": true,
  "simulator_running": false
}
```

---

### Events

#### `POST /api/events`

Speist ein einzelnes Sensor-Event ein und leitet es an Kafka weiter.

**Request Body:**
```json
{
  "sensor_id": "sensor-temp-0001",
  "type": "temperature",
  "value": 42.5,
  "unit": "°C",
  "location": "Hall-A1",
  "timestamp": "2026-06-25T14:30:00+00:00"  // optional, wird auto-generiert
}
```

**Response:**
```json
{
  "status": "accepted",
  "event": { ... }
}
```

#### `POST /api/events/batch`

Speist mehrere Events gleichzeitig ein.

**Request Body:** Array von Events (wie oben)

**Response:**
```json
{
  "status": "accepted",
  "count": 5
}
```

#### `GET /api/events/recent?limit=50`

Gibt die letzten Events aus dem In-Memory-Buffer zurück.

**Query Parameter:**
- `limit` (int, default: 50) – Maximale Anzahl

**Response:** Array von Event-Objekten

---

### Simulator

#### `POST /api/simulator/start`

Startet den integrierten Sensor-Simulator als Background-Task.

**Request Body (optional):**
```json
{
  "num_sensors": 20,
  "interval": 1.0,
  "batch_size": 5
}
```

**Response:**
```json
{
  "status": "started",
  "config": { "num_sensors": 20, "interval": 1.0, "batch_size": 5 }
}
```

**Fehler:** `400` wenn Simulator bereits läuft

#### `POST /api/simulator/stop`

Stoppt den laufenden Simulator.

**Response:**
```json
{
  "status": "stopped"
}
```

**Fehler:** `400` wenn kein Simulator läuft

#### `GET /api/simulator/status`

Gibt den aktuellen Status des Simulators zurück.

**Response:**
```json
{
  "running": true
}
```

---

### Alerts und Aggregate

#### `GET /api/alerts?limit=100`

Gibt die letzten Alerts zurück (von Flink via Kafka konsumiert).

**Response:**
```json
[
  {
    "sensor_id": "sensor-temp-0012",
    "sensor_type": "temperature",
    "location": "Server-Room",
    "location_group": "Infrastructure",
    "window_start": "2026-06-25T14:30:00",
    "window_end": "2026-06-25T14:31:00",
    "max_value": 65.3,
    "threshold": 60.0,
    "severity": "WARNING"
  }
]
```

#### `GET /api/aggregates?limit=100`

Gibt die letzten Aggregat-Ergebnisse zurück (1-Minuten-Fenster).

**Response:**
```json
[
  {
    "sensor_id": "sensor-temp-0001",
    "sensor_type": "temperature",
    "location": "Hall-A1",
    "location_group": "Production",
    "window_start": "2026-06-25T14:30:00",
    "window_end": "2026-06-25T14:31:00",
    "avg_value": 34.2,
    "min_value": 31.8,
    "max_value": 37.1,
    "event_count": 12
  }
]
```

---

## WebSocket

### `WS /ws/live`

Echtzeit-Stream für Events, Alerts und Aggregates.

**Verbindung:**
```javascript
const ws = new WebSocket('ws://localhost:3000/ws/live');
```

**Empfangene Nachrichten:**

```json
// Sensor-Event
{ "type": "event", "data": { "sensor_id": "...", "value": 42.5, ... } }

// Alert
{ "type": "alert", "data": { "sensor_id": "...", "severity": "WARNING", ... } }

// Aggregat
{ "type": "aggregate", "data": { "sensor_id": "...", "avg_value": 34.2, ... } }
```

**Keep-Alive:** Client kann beliebige Textnachrichten senden (werden ignoriert).

---

## Fehlerbehandlung

Alle Endpunkte geben bei Fehlern ein JSON-Objekt zurück:

```json
{
  "detail": "Simulator already running"
}
```

HTTP-Statuscodes:
- `200` – Erfolg
- `400` – Ungültige Anfrage / Zustandsfehler
- `422` – Validierungsfehler (Pydantic)
- `500` – Interner Serverfehler
