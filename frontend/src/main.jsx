import React from 'react';
import ReactDOM from 'react-dom/client';
import { ThemeProvider, CssBaseline } from '@mui/material';
import { BrowserRouter } from 'react-router-dom';
import App from './App';
import theme from './theme';
import { LiveDataProvider } from './LiveDataContext';

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <LiveDataProvider>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </LiveDataProvider>
    </ThemeProvider>
  </React.StrictMode>
);
