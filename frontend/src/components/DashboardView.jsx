import { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Grid,
  Typography,
  Chip,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Divider,
} from '@mui/material';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import ThermostatIcon from '@mui/icons-material/Thermostat';
import WaterDropIcon from '@mui/icons-material/WaterDrop';
import SpeedIcon from '@mui/icons-material/Speed';
import VibrationIcon from '@mui/icons-material/Vibration';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts';

// Mock data for development
const MOCK_AGGREGATES = [
  { window_start: '10:00', sensor_id: 'sensor-001', sensor_type: 'temperature', location: 'Hall A', avg_value: 24.5, min_value: 22.1, max_value: 27.3, event_count: 60 },
  { window_start: '10:01', sensor_id: 'sensor-001', sensor_type: 'temperature', location: 'Hall A', avg_value: 25.1, min_value: 22.8, max_value: 28.1, event_count: 58 },
  { window_start: '10:02', sensor_id: 'sensor-001', sensor_type: 'temperature', location: 'Hall A', avg_value: 26.8, min_value: 24.0, max_value: 30.2, event_count: 62 },
  { window_start: '10:03', sensor_id: 'sensor-001', sensor_type: 'temperature', location: 'Hall A', avg_value: 28.3, min_value: 25.5, max_value: 32.1, event_count: 59 },
  { window_start: '10:04', sensor_id: 'sensor-001', sensor_type: 'temperature', location: 'Hall A', avg_value: 27.1, min_value: 24.2, max_value: 29.8, event_count: 61 },
  { window_start: '10:05', sensor_id: 'sensor-001', sensor_type: 'temperature', location: 'Hall A', avg_value: 25.6, min_value: 23.0, max_value: 28.4, event_count: 60 },
];

const MOCK_ALERTS = [
  { id: 1, sensor_id: 'sensor-003', sensor_type: 'temperature', location: 'Hall B', value: 78.2, threshold: 70, timestamp: '2026-06-17T10:03:22Z', severity: 'critical' },
  { id: 2, sensor_id: 'sensor-007', sensor_type: 'vibration', location: 'Hall A', value: 42.5, threshold: 35, timestamp: '2026-06-17T10:02:45Z', severity: 'warning' },
  { id: 3, sensor_id: 'sensor-012', sensor_type: 'pressure', location: 'Hall C', value: 1085, threshold: 1080, timestamp: '2026-06-17T09:58:10Z', severity: 'warning' },
];

const SENSOR_ICONS = {
  temperature: <ThermostatIcon />,
  humidity: <WaterDropIcon />,
  pressure: <SpeedIcon />,
  vibration: <VibrationIcon />,
};

const STAT_CARDS = [
  { label: 'Active Sensors', value: '12', color: 'primary.main' },
  { label: 'Events / min', value: '240', color: 'success.main' },
  { label: 'Active Alerts', value: '3', color: 'error.main' },
  { label: 'Avg Temperature', value: '25.6°C', color: 'warning.main' },
];

function DashboardView() {
  const [aggregates, setAggregates] = useState(MOCK_AGGREGATES);
  const [alerts, setAlerts] = useState(MOCK_ALERTS);

  useEffect(() => {
    // Fetch real data when backend is available
    const fetchData = async () => {
      try {
        const [aggRes, alertRes] = await Promise.all([
          fetch('/api/aggregates'),
          fetch('/api/alerts'),
        ]);
        if (aggRes.ok) setAggregates(await aggRes.json());
        if (alertRes.ok) setAlerts(await alertRes.json());
      } catch {
        // Use mock data during development
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Dashboard
      </Typography>
      <Typography variant="body1" color="text.secondary" sx={{ mb: 3 }}>
        Real-time overview of sensor aggregates and alerts.
      </Typography>

      {/* Stat Cards */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        {STAT_CARDS.map((stat) => (
          <Grid item xs={6} md={3} key={stat.label}>
            <Card>
              <CardContent sx={{ textAlign: 'center', py: 2 }}>
                <Typography variant="body2" color="text.secondary">
                  {stat.label}
                </Typography>
                <Typography variant="h4" sx={{ color: stat.color, mt: 0.5 }}>
                  {stat.value}
                </Typography>
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>

      <Grid container spacing={3}>
        {/* Time Series Chart */}
        <Grid item xs={12} lg={8}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Temperature Trend (sensor-001)
              </Typography>
              <Divider sx={{ mb: 2 }} />
              <ResponsiveContainer width="100%" height={280}>
                <LineChart data={aggregates}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                  <XAxis dataKey="window_start" />
                  <YAxis domain={['auto', 'auto']} />
                  <Tooltip />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="avg_value"
                    stroke="#1976d2"
                    strokeWidth={2}
                    name="Average"
                    dot={{ r: 4 }}
                  />
                  <Line
                    type="monotone"
                    dataKey="max_value"
                    stroke="#d32f2f"
                    strokeWidth={1}
                    strokeDasharray="4 4"
                    name="Max"
                    dot={false}
                  />
                  <Line
                    type="monotone"
                    dataKey="min_value"
                    stroke="#2e7d32"
                    strokeWidth={1}
                    strokeDasharray="4 4"
                    name="Min"
                    dot={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </Grid>

        {/* Alerts Panel */}
        <Grid item xs={12} lg={4}>
          <Card>
            <CardContent>
              <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                <WarningAmberIcon color="warning" />
                <Typography variant="h6">Active Alerts</Typography>
              </Stack>
              <Divider sx={{ mb: 2 }} />
              <Stack spacing={2}>
                {alerts.map((alert) => (
                  <Paper
                    key={alert.id}
                    variant="outlined"
                    sx={{
                      p: 1.5,
                      borderLeft: 4,
                      borderColor: alert.severity === 'critical' ? 'error.main' : 'warning.main',
                    }}
                  >
                    <Stack direction="row" justifyContent="space-between" alignItems="center">
                      <Stack direction="row" alignItems="center" spacing={1}>
                        {SENSOR_ICONS[alert.sensor_type]}
                        <Box>
                          <Typography variant="subtitle2">{alert.sensor_id}</Typography>
                          <Typography variant="caption" color="text.secondary">
                            {alert.location}
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
                    </Typography>
                  </Paper>
                ))}
              </Stack>
            </CardContent>
          </Card>
        </Grid>

        {/* Aggregates Table */}
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Recent Aggregates
              </Typography>
              <Divider sx={{ mb: 2 }} />
              <TableContainer>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Window</TableCell>
                      <TableCell>Sensor</TableCell>
                      <TableCell>Type</TableCell>
                      <TableCell>Location</TableCell>
                      <TableCell align="right">Avg</TableCell>
                      <TableCell align="right">Min</TableCell>
                      <TableCell align="right">Max</TableCell>
                      <TableCell align="right">Count</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {aggregates.map((row, idx) => (
                      <TableRow key={idx} hover>
                        <TableCell>{row.window_start}</TableCell>
                        <TableCell>{row.sensor_id}</TableCell>
                        <TableCell>
                          <Chip label={row.sensor_type} size="small" variant="outlined" />
                        </TableCell>
                        <TableCell>{row.location}</TableCell>
                        <TableCell align="right">{row.avg_value.toFixed(1)}</TableCell>
                        <TableCell align="right">{row.min_value.toFixed(1)}</TableCell>
                        <TableCell align="right">{row.max_value.toFixed(1)}</TableCell>
                        <TableCell align="right">{row.event_count}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
}

export default DashboardView;
