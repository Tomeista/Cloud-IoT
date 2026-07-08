import { useState, useMemo } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Divider,
  Stack,
  Chip,
  ToggleButton,
  ToggleButtonGroup,
  Autocomplete,
  TextField,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from '@mui/material';
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
import { useLiveData } from '../LiveDataContext';
import { SENSOR_GROUPS, SENSOR_TYPES } from '../mockData';

const LINE_COLORS = [
  '#1976d2', '#d32f2f', '#2e7d32', '#ed6c02',
  '#7b1fa2', '#0288d1', '#c2185b', '#00796b',
];

const DIMENSION_LABEL = { group: 'Gruppe', type: 'Sensortyp', sensor: 'Sensor' };

function SensorsView() {
  const { aggregates } = useLiveData();
  const [dimension, setDimension] = useState('type');
  const [selection, setSelection] = useState(null);

  // Available options for the searchable dropdown, based on the dimension.
  const options = useMemo(() => {
    if (dimension === 'group') return SENSOR_GROUPS.map((g) => g.name);
    if (dimension === 'type') return [...new Set(aggregates.map((a) => a.sensor_type))].sort();
    return [...new Set(aggregates.map((a) => a.sensor_id))].sort();
  }, [dimension, aggregates]);

  // Fall back to the first option when nothing valid is selected.
  const current = selection && options.includes(selection) ? selection : options[0] || null;
  const singleSensor = dimension === 'sensor';

  // Aggregate rows matching the current selection.
  const rows = useMemo(() => {
    if (!current) return [];
    if (dimension === 'group') {
      const grp = SENSOR_GROUPS.find((g) => g.name === current);
      const set = new Set(grp ? grp.sensors : []);
      return aggregates.filter((a) => set.has(a.sensor_id));
    }
    if (dimension === 'type') return aggregates.filter((a) => a.sensor_type === current);
    return aggregates.filter((a) => a.sensor_id === current);
  }, [aggregates, dimension, current]);

  // Build chart series: one avg line per sensor for group/type, or
  // avg/min/max for a single sensor.
  const { chartData, seriesKeys } = useMemo(() => {
    if (singleSensor) {
      const sorted = [...rows].sort((a, b) => a.window_start.localeCompare(b.window_start));
      return { chartData: sorted, seriesKeys: ['avg_value', 'min_value', 'max_value'] };
    }
    const byWindow = {};
    rows.forEach((r) => {
      byWindow[r.window_start] = byWindow[r.window_start] || { window_start: r.window_start };
      byWindow[r.window_start][r.sensor_id] = r.avg_value;
    });
    const data = Object.values(byWindow).sort((a, b) =>
      a.window_start.localeCompare(b.window_start),
    );
    const sensors = [...new Set(rows.map((r) => r.sensor_id))].sort();
    return { chartData: data, seriesKeys: sensors };
  }, [rows, singleSensor]);

  const sensorCount = new Set(rows.map((r) => r.sensor_id)).size;
  const unit = singleSensor
    ? SENSOR_TYPES[rows[0]?.sensor_type]?.unit || ''
    : dimension === 'type'
      ? SENSOR_TYPES[current]?.unit || ''
      : '';

  const tableRows = [...rows]
    .sort((a, b) => b.window_start.localeCompare(a.window_start))
    .slice(0, 15);

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Sensors
      </Typography>
      <Typography variant="body1" color="text.secondary" sx={{ mb: 3 }}>
        Verlauf nach Gruppe, Sensortyp oder einzelnem Sensor.
      </Typography>

      {/* Selector */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Stack
            direction={{ xs: 'column', sm: 'row' }}
            spacing={2}
            alignItems={{ sm: 'center' }}
          >
            <ToggleButtonGroup
              size="small"
              exclusive
              value={dimension}
              onChange={(_, v) => {
                if (v) {
                  setDimension(v);
                  setSelection(null);
                }
              }}
            >
              <ToggleButton value="group">Gruppe</ToggleButton>
              <ToggleButton value="type">Typ</ToggleButton>
              <ToggleButton value="sensor">Sensor</ToggleButton>
            </ToggleButtonGroup>
            <Autocomplete
              size="small"
              options={options}
              value={current}
              onChange={(_, v) => setSelection(v)}
              disableClearable
              sx={{ minWidth: 280 }}
              renderInput={(params) => (
                <TextField {...params} label={DIMENSION_LABEL[dimension]} />
              )}
            />
            <Box sx={{ flexGrow: 1 }} />
            <Chip
              size="small"
              variant="outlined"
              label={`${sensorCount} Sensor${sensorCount === 1 ? '' : 'en'}`}
            />
          </Stack>
        </CardContent>
      </Card>

      {/* Chart */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            {DIMENSION_LABEL[dimension]}: {current || '—'} {unit && `(${unit})`}
          </Typography>
          <Divider sx={{ mb: 2 }} />
          {chartData.length === 0 ? (
            <Typography variant="body2" color="text.secondary">
              Keine Daten für diese Auswahl.
            </Typography>
          ) : (
            <ResponsiveContainer width="100%" height={340}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                <XAxis dataKey="window_start" />
                <YAxis domain={['auto', 'auto']} />
                <Tooltip />
                <Legend />
                {singleSensor ? (
                  <>
                    <Line type="monotone" dataKey="avg_value" name="Ø" stroke="#1976d2" strokeWidth={2} dot={{ r: 3 }} />
                    <Line type="monotone" dataKey="max_value" name="Max" stroke="#d32f2f" strokeWidth={1} strokeDasharray="4 4" dot={false} />
                    <Line type="monotone" dataKey="min_value" name="Min" stroke="#2e7d32" strokeWidth={1} strokeDasharray="4 4" dot={false} />
                  </>
                ) : (
                  seriesKeys.map((key, i) => (
                    <Line
                      key={key}
                      type="monotone"
                      dataKey={key}
                      name={key}
                      stroke={LINE_COLORS[i % LINE_COLORS.length]}
                      strokeWidth={2}
                      dot={{ r: 2 }}
                      connectNulls
                    />
                  ))
                )}
              </LineChart>
            </ResponsiveContainer>
          )}
        </CardContent>
      </Card>

      {/* Table */}
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
                {tableRows.map((row, idx) => (
                  <TableRow key={`${row.sensor_id}-${row.window_start}-${idx}`} hover>
                    <TableCell>{row.window_start}</TableCell>
                    <TableCell>{row.sensor_id}</TableCell>
                    <TableCell>
                      <Chip label={row.sensor_type} size="small" variant="outlined" />
                    </TableCell>
                    <TableCell>{row.location}</TableCell>
                    <TableCell align="right">{Number(row.avg_value).toFixed(1)}</TableCell>
                    <TableCell align="right">{Number(row.min_value).toFixed(1)}</TableCell>
                    <TableCell align="right">{Number(row.max_value).toFixed(1)}</TableCell>
                    <TableCell align="right">{row.event_count}</TableCell>
                  </TableRow>
                ))}
                {tableRows.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={8}>
                      <Typography variant="body2" color="text.secondary">
                        Keine Daten.
                      </Typography>
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
      </Card>
    </Box>
  );
}

export default SensorsView;
