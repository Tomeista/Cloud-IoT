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
import { SENSOR_TYPES } from '../sensorTypes';

const LINE_COLORS = [
  '#1976d2', '#d32f2f', '#2e7d32', '#ed6c02',
  '#7b1fa2', '#0288d1', '#c2185b', '#00796b',
];

const DIMENSION_LABEL = { group: 'Gruppe', type: 'Sensortyp', sensor: 'Sensor' };

// Chronological key of an aggregate. `window_start` is only an "HH:MM" label
// for the axis, so ordering by it would interleave windows from different days
// and collapse yesterday's 14:05 into today's. The full timestamp is on the
// record for exactly this reason; fall back to the label when it is absent.
const windowKey = (row) => row.window_start_ts || row.window_start;

const byWindowAsc = (a, b) => windowKey(a).localeCompare(windowKey(b));

function SensorsView() {
  const { aggregates, status } = useLiveData();
  // Distinguishes "the pipeline produced nothing for this selection" from
  // "we never got an answer", so an empty chart is never ambiguous.
  const emptyMessage =
    status === 'offline'
      ? 'Backend nicht erreichbar — keine Daten.'
      : status === 'loading'
        ? 'Lade …'
        : 'Keine Daten für diese Auswahl.';
  const [dimension, setDimension] = useState('type');
  const [selection, setSelection] = useState(null);

  // Available options for the searchable dropdown, based on the dimension.
  // All three come out of the aggregates themselves: `group` rides along on
  // every record because the Flink job enriches events from the sensor
  // catalogue, so the view needs no grouping table of its own to drift from it.
  const options = useMemo(() => {
    const field = { group: 'group', type: 'sensor_type', sensor: 'sensor_id' }[dimension];
    return [...new Set(aggregates.map((a) => a[field]).filter(Boolean))].sort();
  }, [dimension, aggregates]);

  // Fall back to the first option when nothing valid is selected.
  const current = selection && options.includes(selection) ? selection : options[0] || null;
  const singleSensor = dimension === 'sensor';

  // Aggregate rows matching the current selection.
  const rows = useMemo(() => {
    if (!current) return [];
    const field = { group: 'group', type: 'sensor_type', sensor: 'sensor_id' }[dimension];
    return aggregates.filter((a) => a[field] === current);
  }, [aggregates, dimension, current]);

  // Build chart series: one avg line per sensor for group/type, or
  // avg/min/max for a single sensor.
  const { chartData, seriesKeys } = useMemo(() => {
    if (singleSensor) {
      const sorted = [...rows].sort(byWindowAsc);
      return { chartData: sorted, seriesKeys: ['avg_value', 'min_value', 'max_value'] };
    }
    // Group by the chronological key so two same-labelled windows from
    // different days stay separate points, but plot the short label.
    const byWindow = {};
    rows.forEach((r) => {
      const key = windowKey(r);
      byWindow[key] = byWindow[key] || {
        window_start: r.window_start,
        window_start_ts: r.window_start_ts,
      };
      byWindow[key][r.sensor_id] = r.avg_value;
    });
    const data = Object.values(byWindow).sort(byWindowAsc);
    const sensors = [...new Set(rows.map((r) => r.sensor_id))].sort();
    return { chartData: data, seriesKeys: sensors };
  }, [rows, singleSensor]);

  const sensorCount = new Set(rows.map((r) => r.sensor_id)).size;
  const unit = singleSensor
    ? SENSOR_TYPES[rows[0]?.sensor_type]?.unit || ''
    : dimension === 'type'
      ? SENSOR_TYPES[current]?.unit || ''
      : '';

  const tableRows = [...rows].sort((a, b) => byWindowAsc(b, a)).slice(0, 15);

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
              {emptyMessage}
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
                        {emptyMessage}
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
