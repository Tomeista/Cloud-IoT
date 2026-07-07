// Central mock data + grouping config for the frontend.
//
// When the backend is connected, aggregates/alerts come from /api/*.
// This data is the fallback so the UI stays presentable without a backend
// and demonstrates how it would look once wired up.

export const SENSOR_TYPES = {
  temperature: { label: 'Temperature', unit: '°C', color: '#d32f2f' },
  humidity: { label: 'Humidity', unit: '%', color: '#0288d1' },
  pressure: { label: 'Pressure', unit: 'hPa', color: '#7b1fa2' },
  vibration: { label: 'Vibration', unit: 'mm/s', color: '#ed6c02' },
};

// Static sensor catalogue (mirrors flink-job/metadata.json).
export const SENSORS = [
  { sensor_id: 'sensor-temp-0000', sensor_type: 'temperature', location: 'Hall-A1' },
  { sensor_id: 'sensor-temp-0001', sensor_type: 'temperature', location: 'Hall-A2' },
  { sensor_id: 'sensor-pres-0002', sensor_type: 'pressure', location: 'Hall-B1' },
  { sensor_id: 'sensor-pres-0003', sensor_type: 'pressure', location: 'Hall-B2' },
  { sensor_id: 'sensor-humi-0004', sensor_type: 'humidity', location: 'Cold-Storage' },
  { sensor_id: 'sensor-humi-0005', sensor_type: 'humidity', location: 'Server-Room' },
  { sensor_id: 'sensor-vibr-0006', sensor_type: 'vibration', location: 'Production-Line-1' },
  { sensor_id: 'sensor-vibr-0007', sensor_type: 'vibration', location: 'Production-Line-2' },
];

// Logical sensor groups (e.g. per machine/area). Frontend-defined for now;
// could later be derived from a "group" field in metadata.json.
export const SENSOR_GROUPS = [
  { name: 'Machine 1 – Hall A', sensors: ['sensor-temp-0000', 'sensor-temp-0001', 'sensor-vibr-0006'] },
  { name: 'Machine 2 – Hall B', sensors: ['sensor-pres-0002', 'sensor-pres-0003', 'sensor-vibr-0007'] },
  { name: 'Cold Storage', sensors: ['sensor-humi-0004'] },
  { name: 'Server Room', sensors: ['sensor-humi-0005'] },
];

// Baseline value + spread per type, for realistic-looking mock windows.
const BASELINE = { temperature: 24, humidity: 55, pressure: 1013, vibration: 12 };
const SPREAD = { temperature: 6, humidity: 15, pressure: 20, vibration: 8 };

const pad = (n) => String(n).padStart(2, '0');

// Generate `windows` one-minute aggregate windows per sensor, ending "now".
export function generateMockAggregates(windows = 8) {
  const rows = [];
  const now = new Date();
  for (const s of SENSORS) {
    const base = BASELINE[s.sensor_type];
    const spread = SPREAD[s.sensor_type];
    let avg = base + (Math.random() - 0.5) * spread;
    for (let i = windows - 1; i >= 0; i--) {
      const t = new Date(now.getTime() - i * 60000);
      const label = `${pad(t.getHours())}:${pad(t.getMinutes())}`;
      avg += (Math.random() - 0.5) * spread * 0.3;
      const min = avg - Math.random() * spread * 0.4;
      const max = avg + Math.random() * spread * 0.4;
      rows.push({
        window_start: label,
        window_end: label,
        sensor_id: s.sensor_id,
        sensor_type: s.sensor_type,
        location: s.location,
        avg_value: Math.round(avg * 10) / 10,
        min_value: Math.round(min * 10) / 10,
        max_value: Math.round(max * 10) / 10,
        event_count: 55 + Math.floor(Math.random() * 10),
      });
    }
  }
  return rows;
}

export const MOCK_AGGREGATES = generateMockAggregates();

export const MOCK_ALERTS = [
  { id: 1, sensor_id: 'sensor-temp-0000', sensor_type: 'temperature', location: 'Hall-A1', value: 72.4, threshold: 60, timestamp: new Date(Date.now() - 60000).toISOString(), severity: 'critical', consecutive_breaches: 5 },
  { id: 2, sensor_id: 'sensor-vibr-0006', sensor_type: 'vibration', location: 'Production-Line-1', value: 41.2, threshold: 30, timestamp: new Date(Date.now() - 180000).toISOString(), severity: 'warning', consecutive_breaches: 3 },
  { id: 3, sensor_id: 'sensor-pres-0002', sensor_type: 'pressure', location: 'Hall-B1', value: 1086, threshold: 1050, timestamp: new Date(Date.now() - 300000).toISOString(), severity: 'warning', consecutive_breaches: 3 },
];

// System / pipeline log feed — lower-severity events that are NOT alerts yet.
// No backend endpoint exists yet; this could later be fed by a /api/logs
// endpoint or a Kafka "system-events" topic. Kept here to demonstrate the feed.
export const MOCK_LOGS = [
  { id: 1, level: 'warning', source: 'sensor-temp-0000', message: 'Wert nähert sich Warnschwelle (58.7 °C / 60)', timestamp: new Date(Date.now() - 15000).toISOString() },
  { id: 2, level: 'info', source: 'flink', message: 'Checkpoint 128 abgeschlossen (2.1s)', timestamp: new Date(Date.now() - 45000).toISOString() },
  { id: 3, level: 'error', source: 'kafka', message: 'Consumer-Lag steigt: sensor-events Partition 2 (1.2k)', timestamp: new Date(Date.now() - 90000).toISOString() },
  { id: 4, level: 'info', source: 'sensor-vibr-0007', message: 'Verbindung nach kurzem Ausfall wiederhergestellt', timestamp: new Date(Date.now() - 120000).toISOString() },
  { id: 5, level: 'warning', source: 'backend', message: '/api/aggregates: Limit von 100 Zeilen erreicht', timestamp: new Date(Date.now() - 210000).toISOString() },
  { id: 6, level: 'info', source: 'minio', message: 'Bucket iot-lakehouse bereit', timestamp: new Date(Date.now() - 300000).toISOString() },
];
