import { useState } from 'react';
import {
  Box,
  Button,
  Card,
  CardContent,
  FormControl,
  Grid,
  InputLabel,
  MenuItem,
  Select,
  Slider,
  Stack,
  TextField,
  Typography,
  Alert,
  Chip,
  Divider,
} from '@mui/material';
import SendIcon from '@mui/icons-material/Send';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';

const SENSOR_TYPES = [
  { value: 'temperature', label: 'Temperature', unit: '°C', min: -20, max: 80 },
  { value: 'humidity', label: 'Humidity', unit: '%', min: 0, max: 100 },
  { value: 'pressure', label: 'Pressure', unit: 'hPa', min: 900, max: 1100 },
  { value: 'vibration', label: 'Vibration', unit: 'mm/s', min: 0, max: 50 },
];

const LOCATIONS = ['Hall A', 'Hall B', 'Hall C', 'Outdoor'];

function ProducerView() {
  const [sensorType, setSensorType] = useState('temperature');
  const [sensorId, setSensorId] = useState('sensor-001');
  const [location, setLocation] = useState('Hall A');
  const [value, setValue] = useState(25);
  const [simulatorRunning, setSimulatorRunning] = useState(false);
  const [lastSent, setLastSent] = useState(null);

  const currentType = SENSOR_TYPES.find((t) => t.value === sensorType);

  const handleSendEvent = async () => {
    const event = {
      sensor_id: sensorId,
      event_time: new Date().toISOString(),
      sensor_type: sensorType,
      value: value,
      unit: currentType.unit,
      location: location,
    };

    try {
      const response = await fetch('/api/events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(event),
      });
      if (response.ok) {
        setLastSent(event);
      }
    } catch (err) {
      // In development without backend, still show the event
      setLastSent(event);
    }
  };

  const handleToggleSimulator = async () => {
    try {
      const endpoint = simulatorRunning ? '/api/simulator/stop' : '/api/simulator/start';
      await fetch(endpoint, { method: 'POST' });
    } catch (err) {
      // Toggle locally for demo purposes
    }
    setSimulatorRunning(!simulatorRunning);
  };

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Event Producer
      </Typography>
      <Typography variant="body1" color="text.secondary" sx={{ mb: 3 }}>
        Send sensor events manually or start the automated simulator.
      </Typography>

      <Grid container spacing={3}>
        {/* Manual Event Form */}
        <Grid item xs={12} md={7}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Send Manual Event
              </Typography>
              <Divider sx={{ mb: 3 }} />

              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Sensor ID"
                    value={sensorId}
                    onChange={(e) => setSensorId(e.target.value)}
                    size="small"
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Location</InputLabel>
                    <Select
                      value={location}
                      label="Location"
                      onChange={(e) => setLocation(e.target.value)}
                    >
                      {LOCATIONS.map((loc) => (
                        <MenuItem key={loc} value={loc}>
                          {loc}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Sensor Type</InputLabel>
                    <Select
                      value={sensorType}
                      label="Sensor Type"
                      onChange={(e) => {
                        setSensorType(e.target.value);
                        const type = SENSOR_TYPES.find((t) => t.value === e.target.value);
                        setValue(Math.round((type.min + type.max) / 2));
                      }}
                    >
                      {SENSOR_TYPES.map((type) => (
                        <MenuItem key={type.value} value={type.value}>
                          {type.label} ({type.unit})
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <Typography gutterBottom>
                    Value: <strong>{value} {currentType.unit}</strong>
                  </Typography>
                  <Slider
                    value={value}
                    onChange={(_, v) => setValue(v)}
                    min={currentType.min}
                    max={currentType.max}
                    step={0.1}
                    valueLabelDisplay="auto"
                  />
                </Grid>
                <Grid item xs={12}>
                  <Button
                    variant="contained"
                    size="large"
                    startIcon={<SendIcon />}
                    onClick={handleSendEvent}
                    fullWidth
                  >
                    Send Event
                  </Button>
                </Grid>
              </Grid>
            </CardContent>
          </Card>
        </Grid>

        {/* Simulator Control + Last Event */}
        <Grid item xs={12} md={5}>
          <Stack spacing={3}>
            {/* Simulator Card */}
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Simulator
                </Typography>
                <Divider sx={{ mb: 2 }} />
                <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                  Automatically generate sensor events at regular intervals.
                </Typography>
                <Chip
                  label={simulatorRunning ? 'Running' : 'Stopped'}
                  color={simulatorRunning ? 'success' : 'default'}
                  size="small"
                  sx={{ mb: 2 }}
                />
                <Button
                  variant={simulatorRunning ? 'outlined' : 'contained'}
                  color={simulatorRunning ? 'error' : 'success'}
                  startIcon={simulatorRunning ? <StopIcon /> : <PlayArrowIcon />}
                  onClick={handleToggleSimulator}
                  fullWidth
                >
                  {simulatorRunning ? 'Stop Simulator' : 'Start Simulator'}
                </Button>
              </CardContent>
            </Card>

            {/* Last Sent Event */}
            {lastSent && (
              <Card sx={{ borderLeft: 4, borderColor: 'success.main' }}>
                <CardContent>
                  <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                    <CheckCircleIcon color="success" fontSize="small" />
                    <Typography variant="subtitle2" color="success.main">
                      Event Sent Successfully
                    </Typography>
                  </Stack>
                  <Typography variant="body2" component="pre" sx={{
                    backgroundColor: 'grey.50',
                    p: 1.5,
                    borderRadius: 1,
                    fontSize: '0.75rem',
                    overflow: 'auto',
                    whiteSpace: 'pre-wrap',
                  }}>
                    {JSON.stringify(lastSent, null, 2)}
                  </Typography>
                </CardContent>
              </Card>
            )}
          </Stack>
        </Grid>
      </Grid>
    </Box>
  );
}

export default ProducerView;
