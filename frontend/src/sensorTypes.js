// Display metadata per sensor type: the unit to label axes and values with,
// and a stable colour so a type keeps the same colour across views.
//
// This is presentation metadata, not measurement data. The aggregate records
// the pipeline emits carry no `unit` field -- units belong to the sensor type,
// not to each window -- so the UI resolves them here.
export const SENSOR_TYPES = {
  temperature: { label: 'Temperature', unit: '°C', color: '#d32f2f' },
  humidity: { label: 'Humidity', unit: '%', color: '#0288d1' },
  pressure: { label: 'Pressure', unit: 'hPa', color: '#7b1fa2' },
  vibration: { label: 'Vibration', unit: 'mm/s', color: '#ed6c02' },
};
