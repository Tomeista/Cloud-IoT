# Dokumentation – IoT Sensor Monitoring

## Übersicht

Diese Dokumentation hält den aktuellen Entwicklungsstand des Projekts fest und dient als Referenz für alle Teammitglieder und den KI-Agenten.

## Inhaltsverzeichnis

| Datei | Inhalt |
|-------|--------|
| [FILES.md](FILES.md) | Alle Dateien im Projekt mit Beschreibung |
| [FUNCTIONS.md](FUNCTIONS.md) | Alle Funktionen und Klassen pro Modul |
| [IMPORTS.md](IMPORTS.md) | Import-Abhängigkeiten pro Datei |
| [API_DOCUMENTATION.md](API_DOCUMENTATION.md) | REST- und WebSocket-Endpunkte |
| [CONFIGURATION.md](CONFIGURATION.md) | Konfiguration und Umgebungsvariablen |
| [ROADMAP.md](ROADMAP.md) | Geplante Features und Zukunft |
| [QUICKSTART.md](QUICKSTART.md) | Schnellstart-Anleitung |
| [WORKFLOW.md](WORKFLOW.md) | Entwicklungs-Workflow |
| [HANDOFF.md](HANDOFF.md) | Übergabe-Kontext für Weiterarbeit |

## Aktueller Stand

**Datum**: 2026-06-25

**Status**: MVP-Prototyp vollständig angelegt

- [x] Sensor Simulator (standalone Python)
- [x] FastAPI Backend mit Kafka-Integration
- [x] Web-UI (Producer + Dashboard)
- [x] PyFlink Streaming Job (Aggregation + Alerting)
- [x] Docker Compose (lokale Entwicklung)
- [x] Helm Chart (Kubernetes Deployment)
- [ ] End-to-End-Test auf k3s
- [ ] Iceberg/Parquet-Schreibung in MinIO
- [ ] Erweiterte Dashboard-Visualisierung
