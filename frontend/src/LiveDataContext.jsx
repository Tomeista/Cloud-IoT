import { createContext, useContext, useState, useEffect } from 'react';

// Single source of live data for the whole app. Polls the backend once and
// shares the result via context, so the AppBar status and every view stay in
// sync without each running its own poller.
//
// There is deliberately no fallback data. Every value the UI renders is one the
// pipeline actually produced; when the backend cannot be reached the views show
// an explicit empty state instead of stand-in numbers, so a screenshot can
// never be mistaken for a running system.

const LiveDataContext = createContext(null);

export function LiveDataProvider({ children, intervalMs = 5000 }) {
  const [aggregates, setAggregates] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [archive, setArchive] = useState(null);
  // 'loading' until the first poll settles, then 'online' or 'offline'. Three
  // states rather than a boolean so the UI can tell "nothing yet" from "nothing
  // there" from "nobody answering".
  const [status, setStatus] = useState('loading');
  const [lastUpdated, setLastUpdated] = useState(null);

  useEffect(() => {
    let active = true;

    const goOffline = () => {
      // Drop what we hold rather than leaving the last good response on screen:
      // stale readings under a disconnected banner still look like live ones.
      setStatus('offline');
      setAggregates([]);
      setAlerts([]);
      setArchive(null);
    };

    const fetchData = async () => {
      try {
        const [aggRes, alertRes, archiveRes] = await Promise.all([
          fetch('/api/aggregates?limit=500'),
          fetch('/api/alerts?limit=100'),
          fetch('/api/archive/status'),
        ]);
        if (!active) return;
        if (aggRes.ok && alertRes.ok) {
          const agg = await aggRes.json();
          const alt = await alertRes.json();
          setAggregates(Array.isArray(agg) ? agg : []);
          setAlerts(Array.isArray(alt) ? alt : []);
          setStatus('online');
          setLastUpdated(new Date());
          // Independent of the two above: a failure here must not cost us the
          // aggregates and alerts we just fetched successfully.
          setArchive(archiveRes.ok ? await archiveRes.json() : null);
        } else {
          goOffline();
        }
      } catch {
        if (active) goOffline();
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
    <LiveDataContext.Provider
      value={{ aggregates, alerts, archive, status, lastUpdated }}
    >
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
