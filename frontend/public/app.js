// ---------------------------------------------------------------------------
// IoT Sensor Monitoring – Frontend Application
// ---------------------------------------------------------------------------

const API_BASE = '/api';
let ws = null;
let eventCount = 0;
const sensorCharts = {};

// Chart data buffers (last 60 data points per type)
const MAX_CHART_POINTS = 60;
const chartData = {
  temperature: [],
  pressure: [],
  humidity: [],
  vibration: [],
};
const chartLabels = {
  temperature: [],
  pressure: [],
  humidity: [],
  vibration: [],
};

// Recent values per type for stat cards
const latestValues = {};

// ── Tab Navigation ──────────────────────────────────────────────────────────

function showTab(tab) {
  document.getElementById('view-producer').classList.toggle('hidden', tab !== 'producer');
  document.getElementById('view-dashboard').classList.toggle('hidden', tab !== 'dashboard');
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + tab).classList.add('active');
}

// ── API Helpers ─────────────────────────────────────────────────────────────

async function apiFetch(path, options = {}) {
  const res = await fetch(API_BASE + path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

// ── Health Check ────────────────────────────────────────────────────────────

async function checkHealth() {
  try {
    const data = await apiFetch('/health');
    const el = document.getElementById('kafka-status');
    if (data.kafka_connected) {
      el.innerHTML = '<span class="w-2 h-2 rounded-full bg-green-500"></span> Kafka';
    } else {
      el.innerHTML = '<span class="w-2 h-2 rounded-full bg-yellow-500"></span> Kafka (offline)';
    }
  } catch {
    const el = document.getElementById('kafka-status');
    el.innerHTML = '<span class="w-2 h-2 rounded-full bg-red-500"></span> API offline';
  }
}

// ── WebSocket ───────────────────────────────────────────────────────────────

function connectWebSocket() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = proto + '//' + location.host + '/ws/live';
  ws = new WebSocket(wsUrl);

  ws.onopen = () => {
    const el = document.getElementById('ws-status');
    el.innerHTML = '<span class="w-2 h-2 rounded-full bg-green-500"></span> WS';
  };

  ws.onmessage = (evt) => {
    const msg = JSON.parse(evt.data);
    if (msg.type === 'event') handleEvent(msg.data);
    else if (msg.type === 'alert') handleAlert(msg.data);
    else if (msg.type === 'aggregate') handleAggregate(msg.data);
  };

  ws.onclose = () => {
    const el = document.getElementById('ws-status');
    el.innerHTML = '<span class="w-2 h-2 rounded-full bg-red-500"></span> WS';
    setTimeout(connectWebSocket, 3000);
  };

  ws.onerror = () => ws.close();
}

// ── Event Handling ──────────────────────────────────────────────────────────

function handleEvent(data) {
  eventCount++;
  document.getElementById('sim-event-count').textContent = eventCount;

  // Update chart data
  const type = data.type;
  if (chartData[type] !== undefined) {
    chartData[type].push(data.value);
    if (chartData[type].length > MAX_CHART_POINTS) chartData[type].shift();

    const now = new Date().toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    chartLabels[type].push(now);
    if (chartLabels[type].length > MAX_CHART_POINTS) chartLabels[type].shift();
  }

  // Track latest values per type
  if (!latestValues[type]) latestValues[type] = [];
  latestValues[type].push(data.value);
  if (latestValues[type].length > 20) latestValues[type].shift();

  // Update stat card
  const avg = latestValues[type].reduce((a, b) => a + b, 0) / latestValues[type].length;
  const valEl = document.getElementById('val-' + type);
  if (valEl) valEl.textContent = avg.toFixed(1);

  updateChart(type);
  addLogEntry(data);
}

function addLogEntry(data) {
  const log = document.getElementById('event-log');
  // Remove placeholder
  const placeholder = log.querySelector('.italic');
  if (placeholder) placeholder.remove();

  const entry = document.createElement('div');
  entry.className = 'log-entry ' + data.type;
  entry.textContent = `[${new Date(data.timestamp).toLocaleTimeString('de-DE')}] ${data.sensor_id} | ${data.type} = ${data.value} ${data.unit} @ ${data.location}`;
  log.prepend(entry);

  // Keep max 200 entries
  while (log.children.length > 200) log.lastChild.remove();
}

function clearEventLog() {
  document.getElementById('event-log').innerHTML = '<p class="text-gray-600 italic">Noch keine Events…</p>';
}

// ── Alert Handling ──────────────────────────────────────────────────────────

let alertCount = 0;

function handleAlert(data) {
  alertCount++;
  document.getElementById('alert-count').textContent = alertCount;

  const list = document.getElementById('alert-list');
  const placeholder = list.querySelector('.italic');
  if (placeholder) placeholder.remove();

  const item = document.createElement('div');
  item.className = 'alert-item' + (data.severity === 'WARNING' ? ' warning' : '');
  const time = data.window_end ? new Date(data.window_end).toLocaleTimeString('de-DE') : new Date().toLocaleTimeString('de-DE');
  item.innerHTML = `
    <div class="flex justify-between items-start">
      <span class="font-medium">${data.sensor_type || data.type || 'Sensor'}</span>
      <span class="text-xs text-gray-500">${time}</span>
    </div>
    <div class="text-xs text-gray-400 mt-0.5">
      ${data.sensor_id || ''} @ ${data.location || ''} — Max: ${(data.max_value || data.value || 0).toFixed(1)} (Schwelle: ${(data.threshold || 0).toFixed(1)})
    </div>`;
  list.prepend(item);

  // Highlight stat card
  const card = document.getElementById('card-' + (data.sensor_type || data.type));
  if (card) {
    card.classList.add('alert');
    setTimeout(() => card.classList.remove('alert'), 5000);
  }
}

// ── Aggregate Handling ──────────────────────────────────────────────────────

function handleAggregate(data) {
  const tbody = document.getElementById('agg-table-body');
  const placeholder = tbody.querySelector('.italic');
  if (placeholder) placeholder.parentElement.remove();

  const row = document.createElement('tr');
  row.className = '';
  const winStart = data.window_start ? new Date(data.window_start).toLocaleTimeString('de-DE') : '-';
  const winEnd = data.window_end ? new Date(data.window_end).toLocaleTimeString('de-DE') : '-';
  row.innerHTML = `
    <td class="px-5 py-3 font-mono text-xs">${data.sensor_id || '-'}</td>
    <td class="px-5 py-3">${data.sensor_type || '-'}</td>
    <td class="px-5 py-3">${data.location || '-'}</td>
    <td class="px-5 py-3 font-semibold text-slate-100">${(data.avg_value || 0).toFixed(2)}</td>
    <td class="px-5 py-3">${(data.min_value || 0).toFixed(2)}</td>
    <td class="px-5 py-3">${(data.max_value || 0).toFixed(2)}</td>
    <td class="px-5 py-3 text-center">${data.event_count || 0}</td>
    <td class="px-5 py-3 text-xs text-slate-500">${winStart} – ${winEnd}</td>`;
  tbody.prepend(row);

  // Keep max 100 rows
  while (tbody.children.length > 100) tbody.lastChild.remove();

  // Update stat card with aggregate avg
  const type = data.sensor_type;
  const valEl = document.getElementById('val-' + type);
  if (valEl && data.avg_value) valEl.textContent = data.avg_value.toFixed(1);
}

// ── Charts (one per sensor type) ────────────────────────────────────────────

const COLORS = {
  temperature: { border: '#ef4444', bg: 'rgba(239,68,68,0.08)', gradient: ['rgba(239,68,68,0.15)', 'rgba(239,68,68,0)'] },
  pressure:    { border: '#3b82f6', bg: 'rgba(59,130,246,0.08)', gradient: ['rgba(59,130,246,0.15)', 'rgba(59,130,246,0)'] },
  humidity:    { border: '#10b981', bg: 'rgba(16,185,129,0.08)', gradient: ['rgba(16,185,129,0.15)', 'rgba(16,185,129,0)'] },
  vibration:   { border: '#f59e0b', bg: 'rgba(245,158,11,0.08)', gradient: ['rgba(245,158,11,0.15)', 'rgba(245,158,11,0)'] },
};

function initChart() {
  Object.entries(COLORS).forEach(([type, colors]) => {
    const canvas = document.getElementById('chart-' + type);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    sensorCharts[type] = new Chart(ctx, {
      type: 'line',
      data: {
        labels: chartLabels[type],
        datasets: [{
          data: chartData[type],
          borderColor: colors.border,
          backgroundColor: colors.bg,
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 4,
          pointHoverBackgroundColor: colors.border,
          tension: 0.4,
          fill: true,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 0 },
        interaction: { intersect: false, mode: 'index' },
        scales: {
          x: {
            display: true,
            ticks: { color: '#64748b', maxTicksLimit: 6, font: { size: 10 } },
            grid: { color: 'rgba(51,65,85,0.3)', drawBorder: false },
          },
          y: {
            display: true,
            ticks: { color: '#64748b', font: { size: 10 }, maxTicksLimit: 5 },
            grid: { color: 'rgba(51,65,85,0.3)', drawBorder: false },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: 'rgba(15,23,42,0.9)',
            borderColor: colors.border,
            borderWidth: 1,
            titleColor: '#e2e8f0',
            bodyColor: '#94a3b8',
            padding: 10,
            cornerRadius: 8,
          },
        },
      },
    });
  });
}

function updateChart(type) {
  const chart = sensorCharts[type];
  if (!chart) return;
  chart.data.labels = chartLabels[type];
  chart.data.datasets[0].data = chartData[type];
  chart.update();
}

// ── Manual Event Form ───────────────────────────────────────────────────────

document.getElementById('event-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const typeEl = document.getElementById('evt-type');
  const units = { temperature: '°C', pressure: 'hPa', humidity: '%', vibration: 'mm/s' };
  const sensorType = typeEl.value;

  const event = {
    sensor_id: document.getElementById('evt-sensor-id').value || 'manual-' + Date.now(),
    type: sensorType,
    value: parseFloat(document.getElementById('evt-value').value),
    unit: units[sensorType],
    location: document.getElementById('evt-location').value,
  };

  try {
    const result = await apiFetch('/events', { method: 'POST', body: JSON.stringify(event) });
    const fb = document.getElementById('evt-feedback');
    fb.textContent = '✓ Event gesendet';
    fb.classList.remove('hidden');
    setTimeout(() => fb.classList.add('hidden'), 2000);
  } catch (err) {
    alert('Fehler: ' + err.message);
  }
});

// ── Simulator Controls ──────────────────────────────────────────────────────

async function startSimulator() {
  const config = {
    num_sensors: parseInt(document.getElementById('sim-sensors').value),
    interval: parseFloat(document.getElementById('sim-interval').value),
    batch_size: parseInt(document.getElementById('sim-batch').value),
  };
  try {
    await apiFetch('/simulator/start', { method: 'POST', body: JSON.stringify(config) });
    setSimulatorUI(true);
  } catch (err) {
    alert('Fehler: ' + err.message);
  }
}

async function stopSimulator() {
  try {
    await apiFetch('/simulator/stop', { method: 'POST' });
    setSimulatorUI(false);
  } catch (err) {
    alert('Fehler: ' + err.message);
  }
}

function setSimulatorUI(running) {
  document.getElementById('btn-sim-start').disabled = running;
  document.getElementById('btn-sim-stop').disabled = !running;
  document.getElementById('sim-status-dot').className =
    'w-2.5 h-2.5 rounded-full ' + (running ? 'bg-green-500 animate-pulse' : 'bg-gray-600');
  document.getElementById('sim-status-text').textContent = running ? 'Aktiv' : 'Inaktiv';
  document.getElementById('sim-status-text').className = running ? 'text-green-400' : 'text-gray-400';
}

// ── Initialization ──────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  initChart();
  connectWebSocket();
  checkHealth();
  // Poll health every 10s
  setInterval(checkHealth, 10000);
  // Check simulator status
  apiFetch('/simulator/status').then(data => setSimulatorUI(data.running)).catch(() => {});
});
