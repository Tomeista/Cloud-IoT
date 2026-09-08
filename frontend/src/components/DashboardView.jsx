import {
  Box,
  Card,
  CardContent,
  Grid,
  Typography,
  Chip,
  Stack,
  Paper,
  Divider,
} from '@mui/material';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import ThermostatIcon from '@mui/icons-material/Thermostat';
import WaterDropIcon from '@mui/icons-material/WaterDrop';
import SpeedIcon from '@mui/icons-material/Speed';
import VibrationIcon from '@mui/icons-material/Vibration';
import SensorsIcon from '@mui/icons-material/Sensors';
import BoltIcon from '@mui/icons-material/Bolt';
import StorageIcon from '@mui/icons-material/Storage';
import { useLiveData } from '../LiveDataContext';

const SENSOR_ICONS = {
  temperature: <ThermostatIcon />,
  humidity: <WaterDropIcon />,
  pressure: <SpeedIcon />,
  vibration: <VibrationIcon />,
};

// The four Delta tables the archiver writes, in pipeline order:
// the raw landing zone, the stream job's two result streams, and the events it
// dropped for arriving too late. Listed statically so a table that has not
// been committed to yet still shows up as 0 rather than silently missing.
const DATASETS = [
  { key: 'raw', label: 'Rohdaten' },
  { key: 'aggregates', label: 'Aggregate' },
  { key: 'alerts', label: 'Alerts' },
  { key: 'late', label: 'Verspätet' },
];

const fmtNum = (n) => (n ?? 0).toLocaleString('de-DE');

const fmtTime = (value) => {
  try {
    return new Date(value).toLocaleTimeString();
  } catch {
    return String(value);
  }
};

function DashboardView() {
  const { aggregates, alerts, archive, status, lastUpdated } = useLiveData();
  const online = status === 'online';

  // KPIs derived from the live data (no hardcoded values).
  const activeSensors = new Set(aggregates.map((a) => a.sensor_id)).size;
  const latestWindow = aggregates.reduce(
    (m, a) => (a.window_start > m ? a.window_start : m),
    '',
  );
  const eventsPerMin = aggregates
    .filter((a) => a.window_start === latestWindow)
    .reduce((sum, a) => sum + (a.event_count || 0), 0);
  const criticalCount = alerts.filter((a) => a.severity === 'critical').length;

  // Without a backend these counts are unknown, not zero -- showing "0 Alerts"
  // while disconnected would assert something we cannot know.
  const stats = [
    {
      label: 'Active Alerts',
      value: online ? alerts.length : '—',
      sub: online ? `${criticalCount} critical` : 'keine Verbindung',
      color: !online ? 'text.disabled' : alerts.length ? 'error.main' : 'success.main',
      icon: <WarningAmberIcon />,
    },
    {
      label: 'Active Sensors',
      value: online ? activeSensors : '—',
      sub: online ? 'reporting' : 'keine Verbindung',
      color: online ? 'primary.main' : 'text.disabled',
      icon: <SensorsIcon />,
    },
    {
      label: 'Events / min',
      value: online ? eventsPerMin : '—',
      sub: online ? (latestWindow ? `window ${latestWindow}` : '—') : 'keine Verbindung',
      color: online ? 'success.main' : 'text.disabled',
      icon: <BoltIcon />,
    },
  ];

  return (
    <Box>
      <Stack
        direction="row"
        alignItems="flex-start"
        justifyContent="space-between"
        flexWrap="wrap"
        gap={1}
      >
        <Box>
          <Typography variant="h4" gutterBottom>
            Dashboard
          </Typography>
          <Typography variant="body1" color="text.secondary" sx={{ mb: 3 }}>
            Wichtigste Kennzahlen, aktive Alerts und Zustand des Data Lake.
          </Typography>
        </Box>
        <Chip
          size="small"
          variant="outlined"
          color={online ? 'success' : status === 'offline' ? 'error' : 'default'}
          label={
            online
              ? `Live · ${lastUpdated ? fmtTime(lastUpdated) : ''}`
              : status === 'offline'
                ? 'Backend nicht erreichbar'
                : 'Verbinde …'
          }
        />
      </Stack>

      {/* KPI cards */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        {stats.map((s) => (
          <Grid item xs={12} sm={4} key={s.label}>
            <Card>
              <CardContent>
                <Stack direction="row" alignItems="center" spacing={1.5}>
                  <Box sx={{ color: s.color, display: 'flex' }}>{s.icon}</Box>
                  <Box>
                    <Typography variant="body2" color="text.secondary">
                      {s.label}
                    </Typography>
                    <Typography variant="h4" sx={{ color: s.color, lineHeight: 1.1 }}>
                      {s.value}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {s.sub}
                    </Typography>
                  </Box>
                </Stack>
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>

      <Grid container spacing={3}>
        {/* Alerts */}
        <Grid item xs={12} md={7}>
          <Card sx={{ height: '100%' }}>
            <CardContent>
              <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                <WarningAmberIcon color="warning" />
                <Typography variant="h6">Active Alerts</Typography>
                <Chip label={alerts.length} size="small" />
              </Stack>
              <Divider sx={{ mb: 2 }} />
              <Stack spacing={1.5} sx={{ maxHeight: 440, overflow: 'auto', pr: 1 }}>
                {alerts.length === 0 && (
                  <Typography variant="body2" color="text.secondary">
                    {online
                      ? 'Keine aktiven Alerts.'
                      : status === 'offline'
                        ? 'Backend nicht erreichbar — keine Daten.'
                        : 'Lade …'}
                  </Typography>
                )}
                {alerts.map((alert) => (
                  <Paper
                    key={alert.id ?? `${alert.sensor_id}-${alert.timestamp}`}
                    variant="outlined"
                    sx={{
                      p: 1.5,
                      borderLeft: 4,
                      borderColor:
                        alert.severity === 'critical' ? 'error.main' : 'warning.main',
                    }}
                  >
                    <Stack direction="row" justifyContent="space-between" alignItems="center">
                      <Stack direction="row" alignItems="center" spacing={1}>
                        {SENSOR_ICONS[alert.sensor_type]}
                        <Box>
                          <Typography variant="subtitle2">{alert.sensor_id}</Typography>
                          <Typography variant="caption" color="text.secondary">
                            {alert.location} · {fmtTime(alert.timestamp)}
                          </Typography>
                        </Box>
                      </Stack>
                      <Chip
                        label={alert.severity}
                        size="small"
                        color={alert.severity === 'critical' ? 'error' : 'warning'}
                      />
                    </Stack>
                    <Typography variant="body2" sx={{ mt: 1 }}>
                      Value: <strong>{alert.value}</strong> (threshold: {alert.threshold})
                      {alert.consecutive_breaches != null && (
                        <> · {alert.consecutive_breaches}× in Folge</>
                      )}
                    </Typography>
                  </Paper>
                ))}
              </Stack>
            </CardContent>
          </Card>
        </Grid>

        {/* Data lake archive */}
        <Grid item xs={12} md={5}>
          <Card sx={{ height: '100%' }}>
            <CardContent>
              <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                <StorageIcon color="primary" />
                <Typography variant="h6">Lakehouse</Typography>
                <Typography variant="caption" color="text.secondary">
                  (Delta Lake · SeaweedFS S3)
                </Typography>
              </Stack>
              <Divider sx={{ mb: 2 }} />

              {!archive ? (
                <Typography variant="body2" color="text.secondary">
                  {status === 'offline'
                    ? 'Backend nicht erreichbar — keine Daten.'
                    : 'Lade …'}
                </Typography>
              ) : (
                <>
                  <Stack direction="row" spacing={4} sx={{ mb: 2 }}>
                    <Box>
                      <Typography variant="body2" color="text.secondary">
                        Commits
                      </Typography>
                      <Typography variant="h5">
                        {fmtNum(archive.objects_written)}
                      </Typography>
                    </Box>
                    <Box>
                      <Typography variant="body2" color="text.secondary">
                        Datensätze
                      </Typography>
                      <Typography variant="h5">
                        {fmtNum(archive.events_archived)}
                      </Typography>
                    </Box>
                  </Stack>

                  <Stack divider={<Divider flexItem />}>
                    {DATASETS.map(({ key, label }) => {
                      const ds = archive.datasets?.[key];
                      return (
                        <Stack
                          key={key}
                          direction="row"
                          justifyContent="space-between"
                          alignItems="center"
                          sx={{ py: 0.75 }}
                        >
                          <Typography variant="body2">{label}</Typography>
                          <Typography variant="caption" color="text.secondary">
                            {fmtNum(ds?.records_archived)} Datensätze
                            {ds?.version != null && ` · v${ds.version}`}
                          </Typography>
                        </Stack>
                      );
                    })}
                  </Stack>

                  {archive.last_object_key && (
                    <Box sx={{ mt: 2 }}>
                      <Typography variant="caption" color="text.secondary">
                        Zuletzt geschrieben
                      </Typography>
                      <Typography
                        variant="caption"
                        component="div"
                        sx={{ fontFamily: 'monospace', wordBreak: 'break-all' }}
                      >
                        {archive.last_object_key}
                      </Typography>
                    </Box>
                  )}

                  {/* The archiver is a single Deployment -- a Delta table
                      takes exactly one writer -- so these counters cover the
                      whole stream rather than one replica's slice. */}
                  <Typography
                    variant="caption"
                    color="text.secondary"
                    component="div"
                    sx={{ mt: 2 }}
                  >
                    Zähler seit dem Start des Archivers. v = Delta-Version.
                  </Typography>
                </>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
}

export default DashboardView;
