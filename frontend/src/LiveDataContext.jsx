import { createContext, useContext, useState, useEffect } from 'react';
import { MOCK_AGGREGATES, MOCK_ALERTS, MOCK_LOGS } from './mockData';

// Single source of live data for the whole app. Polls the backend once and
// shares the result via context, so the AppBar status and every view stay in
// sync without each running its own poller. Falls back to mock data (and marks
// `connected = false`) when the backend is unreachable.

const LiveDataContext = createContext(null);

export function LiveDataProvider({ children, intervalMs = 5000 }) {
  const [aggregates, setAggregates] = useState(MOCK_AGGREGATES);
  const [alerts, setAlerts] = useState(MOCK_ALERTS);
  const [logs] = useState(MOCK_LOGS); // no backend endpoint yet
  const [connected, setConnected] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);

  useEffect(() => {
    let active = true;

    const fetchData = async () => {
      try {
        const [aggRes, alertRes] = await Promise.all([
          fetch('/api/aggregates?limit=500'),
          fetch('/api/alerts?limit=100'),
        ]);
        if (!active) return;
        if (aggRes.ok && alertRes.ok) {
          const agg = await aggRes.json();
          const alt = await alertRes.json();
          setAggregates(Array.isArray(agg) ? agg : []);
          setAlerts(Array.isArray(alt) ? alt : []);
          setConnected(true);
          setLastUpdated(new Date());
        } else {
          setConnected(false);
        }
      } catch {
        if (active) setConnected(false);
      }
    };

    fetchData();
    const id = setInterval(fetchData, intervalMs);
    return () => {
      active = false;
      clearInterval(id);
    };
  }, [intervalMs]);

  return (
    <LiveDataContext.Provider value={{ aggregates, alerts, logs, connected, lastUpdated }}>
      {children}
    </LiveDataContext.Provider>
  );
}

export function useLiveData() {
  const ctx = useContext(LiveDataContext);
  if (!ctx) {
    throw new Error('useLiveData must be used within a LiveDataProvider');
  }
  return ctx;
}
