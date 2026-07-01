# Kubernetes – Ressourcen-Übersicht und Kosten

## Aktuelle Ressourcen (Stand: 2026-06-25)

### Deployments

| Name | Replicas | Memory Request | Memory Limit | CPU Request | CPU Limit |
|------|----------|---------------|-------------|-------------|-----------|
| backend | 1 | 128Mi | 256Mi | 100m | 250m |
| frontend | 1 | 64Mi | 128Mi | 50m | 100m |
| flink-jobmanager | 1 | 512Mi | 1Gi | 250m | 500m |
| flink-taskmanager | 1 | 512Mi | 1Gi | 250m | 500m |

### StatefulSets

| Name | Replicas | Memory Request | Memory Limit | CPU Request | CPU Limit | PVC |
|------|----------|---------------|-------------|-------------|-----------|-----|
| kafka | 1 | 512Mi | 1Gi | 250m | 500m | 5Gi |
| minio | 1 | 256Mi | 512Mi | 100m | 250m | 5Gi |

### Jobs (einmalig / post-install)

| Name | Trigger | Beschreibung |
|------|---------|-------------|
| minio-bucket-setup | post-install | Erstellt Buckets `warehouse` + `checkpoints` |
| flink-job-submit | post-install | Submitted PyFlink Job an JobManager |

### Services

| Name | Typ | Port(s) | Extern erreichbar? |
|------|-----|---------|-------------------|
| kafka | ClusterIP (Headless) | 9092, 9093 | Nein |
| minio | ClusterIP | 9000, 9001 | Nein |
| flink-jobmanager | ClusterIP | 6123, 6124, 8081 | Nein |
| backend | ClusterIP | 8000 | Nein |
| frontend | NodePort | 80 → 30080 | **Ja** |

---

## Gesamtverbrauch (alle Replicas = 1)

| Ressource | Minimum (Requests) | Maximum (Limits) |
|-----------|-------------------|-----------------|
| **Memory** | 1.984 GiB | 3.896 GiB |
| **CPU** | 1.000 Cores | 2.100 Cores |
| **Storage (PVC)** | 10 GiB | 10 GiB |
| **Pods gesamt** | 6 | 6 |

### Berechnung Memory Requests

```
backend:          128 Mi
frontend:          64 Mi
flink-jobmanager: 512 Mi
flink-taskmanager:512 Mi
kafka:            512 Mi
minio:            256 Mi
────────────────────────
SUMME:          1.984 Gi
```

### Berechnung CPU Requests

```
backend:          100m
frontend:          50m
flink-jobmanager: 250m
flink-taskmanager:250m
kafka:            250m
minio:            100m
────────────────────────
SUMME:          1.000 Cores (= 1 vCPU)
```

---

## Kosten-Schätzung (Cloud-Referenz)

> Für das Uni-Projekt nutzen wir k3s lokal. Diese Schätzung dient nur zur Einordnung.

| Anbieter | VM-Typ (passend) | Preis/Monat (ca.) | Preis/Wochenende |
|----------|-----------------|-------------------|-----------------|
| Hetzner (CX31) | 4 vCPU, 8GB RAM | ~12 € | ~0,80 € |
| AWS (t3.medium) | 2 vCPU, 4GB RAM | ~35 $ | ~2,40 $ |
| Azure (B2s) | 2 vCPU, 4GB RAM | ~38 $ | ~2,60 $ |

**Mit Default-Konfiguration (1 Replica je Service) besteht KEIN Kostenrisiko.**

---

## Wann steigen Kosten?

| Aktion | Auswirkung |
|--------|-----------|
| `kubectl scale deployment flink-taskmanager --replicas=5` | +2 GiB RAM, +1 CPU |
| `kubectl scale deployment backend --replicas=10` | +1.2 GiB RAM |
| Kafka Replicas erhöhen | +512 Mi RAM + 5Gi Storage pro Broker |
| MinIO Replicas erhöhen | +256 Mi RAM + 5Gi Storage pro Instanz |
| PVC-Größe erhöhen | Mehr Storage-Kosten (Cloud) |

### Sicherheitsregeln

1. **Nie mehr als 5 Replicas** eines Services ohne Absprache
2. **Nie PVC > 20 GiB** ohne Absprache
3. **Resource Limits sind Pflicht** – kein Pod ohne `resources.limits`
4. **Kein HPA (Horizontal Pod Autoscaler)** im Prototyp aktivieren – unkontrollierte Skalierung!

---

## Namespace

Alle Ressourcen laufen in: **`iot-monitoring`**

```bash
# Alle Ressourcen auflisten
kubectl get all -n iot-monitoring

# Ressourcenverbrauch prüfen
kubectl top pods -n iot-monitoring

# PVCs prüfen
kubectl get pvc -n iot-monitoring
```

---

## Änderungslog

| Datum | Änderung | Auswirkung |
|-------|----------|-----------|
| 2026-06-25 | Initiale Erstellung | 6 Pods, ~2 GiB RAM, 10 GiB Storage |
