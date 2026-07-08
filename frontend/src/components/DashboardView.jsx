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
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import ReportProblemOutlinedIcon from '@mui/icons-material/ReportProblemOutlined';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import { useLiveData } from '../LiveDataContext';

const SENSOR_ICONS = {
  temperature: <ThermostatIcon />,
  humidity: <WaterDropIcon />,
  pressure: <SpeedIcon />,
  vibration: <VibrationIcon />,
};

const LOG_META = {
  info: { color: 'info', icon: <InfoOutlinedIcon fontSize="small" /> },
  warning: { color: 'warning', icon: <ReportProblemOutlinedIcon fontSize="small" /> },
  error: { color: 'error', icon: <ErrorOutlineIcon fontSize="small" /> },
};

const fmtTime = (value) => {
  try {
    return new Date(value).toLocaleTimeString();
  } catch {
    return String(value);
  }
};

function DashboardView() {
  const { aggregates, alerts, logs, connected, lastUpdated } = useLiveData();

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

  const stats = [
    {
      label: 'Active Alerts',
      value: alerts.length,
      sub: `${criticalCount} critical`,
      color: alerts.length ? 'error.main' : 'success.main',
      icon: <WarningAmberIcon />,
    },
    {
      label: 'Active Sensors',
      value: activeSensors,
      sub: 'reporting',
      color: 'primary.main',
      icon: <SensorsIcon />,
    },
    {
      label: 'Events / min',
      value: eventsPerMin,
      sub: latestWindow ? `window ${latestWindow}` : '—',
      color: 'success.main',
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
            Wichtigste Kennzahlen, aktive Alerts und System-Log.
          </Typography>
        </Box>
        <Chip
          size="small"
          variant="outlined"
          color={connected ? 'success' : 'default'}
          label={
            connected
              ? `Live · ${lastUpdated ? fmtTime(lastUpdated) : ''}`
              : 'Demo-Daten (kein Backend)'
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
        <Grid item xs={12} md={5}>
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
                    Keine aktiven Alerts.
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

        {/* Live system log */}
        <Grid item xs={12} md={7}>
          <Card sx={{ height: '100%' }}>
            <CardContent>
              <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                <Typography variant="h6">Live System Log</Typography>
                <Typography variant="caption" color="text.secondary">
                  (Sub-Alert-Ereignisse · geplant: /api/logs)
                </Typography>
              </Stack>
              <Divider sx={{ mb: 2 }} />
              <Stack divider={<Divider flexItem />} sx={{ maxHeight: 440, overflow: 'auto' }}>
                {logs.map((log) => {
                  const meta = LOG_META[log.level] || LOG_META.info;
                  return (
                    <Stack
                      key={log.id}
                      direction="row"
                      spacing={1.5}
                      alignItems="flex-start"
                      sx={{ py: 1 }}
                    >
                      <Chip
                        size="small"
                        color={meta.color}
                        variant="outlined"
                        icon={meta.icon}
                        label={log.level}
                        sx={{ minWidth: 96 }}
                      />
                      <Box sx={{ flexGrow: 1 }}>
                        <Typography variant="body2">{log.message}</Typography>
                        <Typography variant="caption" color="text.secondary">
                          {log.source} · {fmtTime(log.timestamp)}
                        </Typography>
                      </Box>
                    </Stack>
                  );
                })}
              </Stack>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
}

export default DashboardView;
