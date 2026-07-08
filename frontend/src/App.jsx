import { Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import DashboardView from './components/DashboardView';
import SensorsView from './components/SensorsView';
import ProducerView from './components/ProducerView';

function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Navigate to="/dashboard" replace />} />
        <Route path="dashboard" element={<DashboardView />} />
        <Route path="sensors" element={<SensorsView />} />
        <Route path="producer" element={<ProducerView />} />
      </Route>
    </Routes>
  );
}

export default App;
