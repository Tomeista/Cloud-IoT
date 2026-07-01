# Funktionen und Klassen

## sensor_simulator.py

### Klassen

| Klasse | Beschreibung |
|--------|-------------|
| `SensorEvent` | Dataclass – repräsentiert eine einzelne Sensormessung (sensor_id, timestamp, type, value, unit, location) |
| `Sensor` | Simuliert einen Sensor mit realistischem Value-Drift und Mean-Reversion |
| `SensorFleet` | Verwaltet eine Flotte von Sensoren, bietet `read_all()` und `read_random()` |

### Funktionen

| Funktion | Parameter | Beschreibung |
|----------|-----------|-------------|
| `output_stdout(events)` | `list[SensorEvent]` | Gibt Events als JSON-Lines auf stdout aus |
| `output_file(events, filepath)` | `list[SensorEvent], str` | Hängt Events an eine Datei an (JSONL) |
| `output_kafka(events, bootstrap_servers, topic)` | `list[SensorEvent], str, str` | Sendet Events an Kafka-Topic |
| `run_simulator(args)` | `argparse.Namespace` | Haupt-Simulationsloop: erzeugt und dispatcht Events |
| `main()` | – | Argument-Parser und Entry-Point |

### Methoden

| Klasse | Methode | Beschreibung |
|--------|---------|-------------|
| `Sensor` | `__init__(sensor_id, type, location)` | Initialisiert Sensor mit Zufallsstartwert |
| `Sensor` | `read() → SensorEvent` | Generiert nächsten Messwert mit Drift + Spike-Chance (1%) |
| `SensorFleet` | `__init__(num_sensors)` | Erstellt Fleet mit zufälligen Typen/Standorten |
| `SensorFleet` | `read_all() → list[SensorEvent]` | Liest alle Sensoren |
| `SensorFleet` | `read_random(count) → list[SensorEvent]` | Liest zufällige Auswahl von Sensoren |

---

## backend/app/main.py

### Klassen

| Klasse | Beschreibung |
|--------|-------------|
| `SensorEvent` | Pydantic-Model für API-Validierung (timestamp optional, wird auto-generiert) |
| `SimulatorConfig` | Pydantic-Model für Simulator-Konfiguration (num_sensors=20, interval=1.0, batch_size=5) |

### Funktionen

| Funktion | Typ | Beschreibung |
|----------|-----|-------------|
| `broadcast_ws(message)` | async | Sendet JSON-Nachricht an alle verbundenen WebSocket-Clients |
| `consume_results()` | async | Background-Task: konsumiert Aggregates/Alerts aus Kafka |
| `run_simulator(config)` | async | Background-Task: betreibt SensorFleet, sendet Events an Kafka |
| `lifespan(_app)` | async contextmanager | Initialisiert Kafka-Producer/Consumer bei App-Start |

### API-Endpunkte (Funktionen)

| Funktion | Route | Methode | Beschreibung |
|----------|-------|---------|-------------|
| `health()` | `/api/health` | GET | Health-Check: API- und Kafka-Status |
| `create_event(event)` | `/api/events` | POST | Einzelnes Sensor-Event einspeisen |
| `create_events_batch(events)` | `/api/events/batch` | POST | Batch von Events einspeisen |
| `start_simulator(config)` | `/api/simulator/start` | POST | Simulator als Background-Task starten |
| `stop_simulator()` | `/api/simulator/stop` | POST | Laufenden Simulator stoppen |
| `simulator_status()` | `/api/simulator/status` | GET | Prüfen ob Simulator läuft |
| `get_recent_events(limit)` | `/api/events/recent` | GET | Letzte Events aus Buffer (max 500) |
| `get_alerts(limit)` | `/api/alerts` | GET | Letzte Alerts aus Buffer |
| `get_aggregates(limit)` | `/api/aggregates` | GET | Letzte Aggregate aus Buffer |
| `websocket_live(ws)` | `/ws/live` | WebSocket | Echtzeit-Stream: Events, Alerts, Aggregates |

---

## flink-job/job.py

### Funktionen

| Funktion | Beschreibung |
|----------|-------------|
| `load_metadata()` | Lädt `sensor_metadata.json`, aktualisiert Schwellwert-Dict `THRESHOLDS` |
| `main()` | Erstellt Flink TableEnvironment, definiert Kafka-Source/Sinks, startet Windowed-Aggregation + Alerting |

### SQL-Tabellen (zur Laufzeit erstellt)

| Tabelle | Typ | Beschreibung |
|---------|-----|-------------|
| `sensor_events` | Source (Kafka) | Roh-Events mit Event-Time + Watermark (10s) |
| `sensor_aggregates` | Sink (Kafka) | Aggregierte Werte pro 1-Min-Fenster |
| `sensor_alerts` | Sink (Kafka) | Alert-Events bei Schwellwertüberschreitung |
| `enriched_aggregates` | View | Zwischenergebnis mit Location-Group Enrichment |

---

## frontend/public/app.js

### Funktionen

| Funktion | Beschreibung |
|----------|-------------|
| `showTab(tab)` | Wechselt zwischen Producer- und Dashboard-Ansicht |
| `apiFetch(path, options)` | Fetch-Wrapper für API-Aufrufe mit JSON-Header |
| `checkHealth()` | Pollt `/api/health`, aktualisiert Status-Anzeigen |
| `connectWebSocket()` | WebSocket-Verbindung zu `/ws/live` mit Auto-Reconnect (3s) |
| `handleEvent(data)` | Verarbeitet Event: Chart-Update, Stat-Cards, Log-Entry |
| `handleAlert(data)` | Verarbeitet Alert: Alert-Liste, Card-Highlight |
| `handleAggregate(data)` | Verarbeitet Aggregat: Tabellen-Eintrag, Stat-Card-Update |
| `addLogEntry(data)` | Fügt formatierte Event-Zeile in Log ein (max 200) |
| `clearEventLog()` | Leert Event-Log-Anzeige |
| `initChart()` | Initialisiert Chart.js Line-Chart (4 Datasets) |
| `updateChart()` | Aktualisiert Chart mit neuesten Datenpunkten |
| `startSimulator()` | POST an `/api/simulator/start` mit Form-Konfiguration |
| `stopSimulator()` | POST an `/api/simulator/stop` |
| `setSimulatorUI(running)` | Aktualisiert Button-States und Status-Indikator |

---

## Statistik

| Modul | Klassen | Funktionen | Gesamt |
|-------|---------|------------|--------|
| sensor_simulator.py | 3 | 5 (+5 Methoden) | 13 |
| backend/app/main.py | 2 | 14 | 16 |
| flink-job/job.py | 0 | 2 | 2 |
| frontend/public/app.js | 0 | 14 | 14 |
| **Gesamt** | **5** | **35** | **45** |
