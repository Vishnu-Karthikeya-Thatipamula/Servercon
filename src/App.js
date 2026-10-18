// src/App.js
import React, { useState, useEffect, useCallback, useRef } from 'react';
import axios from 'axios';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import './App.css';

const API_BASE_RAW = process.env.REACT_APP_API_BASE || "https://servercon.onrender.com";
const API_BASE = API_BASE_RAW.endsWith("/") ? API_BASE_RAW.slice(0, -1) : API_BASE_RAW;
// Render free tier sleeps + has no always-on cron: poll slower than local
// so each point on the chart is a real new ping, not a re-fetch of the same row.
const POLL_MS = 30000;
const REQUEST_TIMEOUT_MS = 15000;

// Backend returns either { error: "..." }, { message: "..." }, or a raw string.
// Stringify only as a last resort so the banner stays readable.
function getFriendlyApiErrorMessage(error) {
  const data = error?.response?.data;
  if (data) {
    if (typeof data === 'string') return data;
    if (typeof data.error === 'string') return data.error;
    if (typeof data.message === 'string') return data.message;
    try {
      return JSON.stringify(data);
    } catch {
      return String(data);
    }
  }
  return error?.message || 'Unknown error';
}

function App() {
  const [targets, setTargets] = useState([]);
  const [newUrl, setNewUrl] = useState('');
  const [newName, setNewName] = useState('');
  const [error, setError] = useState('');
  const [adding, setAdding] = useState(false);
  const [loading, setLoading] = useState(true);

  const [lastUpdated, setLastUpdated] = useState(null);
  const isMountedRef = useRef(true);

  const fetchTargets = useCallback(async () => {
    try {
      // Connects to the Flask backend (Render prod, same as your working screenshot)
      const response = await axios.get(`${API_BASE}/api/targets`, { timeout: REQUEST_TIMEOUT_MS });
      const data = Array.isArray(response.data) ? response.data : [];
      if (!isMountedRef.current) return;
      setTargets(data);
      if (data.length > 0) setError('');
      setLastUpdated(new Date());
    } catch (error) {
      console.error("Error fetching data, Flask server might be down.", error);
      if (!isMountedRef.current) return;
      // Don't wipe existing cards on a transient poll failure (Render cold start)
      // — only show the error banner.
      if (error?.response) {
        setError(`Backend error ${error.response.status}: ${getFriendlyApiErrorMessage(error)}`);
      } else if (error?.code === 'ECONNABORTED') {
        setError('Backend is waking up (request timed out). Retrying automatically…');
      } else {
        setError(`Cannot reach backend: ${getFriendlyApiErrorMessage(error)}`);
      }
    } finally {
      if (isMountedRef.current) setLoading(false);
    }
  }, []);

  useEffect(() => {
    isMountedRef.current = true;
    fetchTargets();
    // Auto-refresh the dashboard (matches backend PING_INTERVAL so the graph walks)
    const interval = setInterval(fetchTargets, POLL_MS);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchTargets]);

  const handleAddTarget = async (e) => {
    e.preventDefault();
    const name = newName.trim();
    let url = newUrl.trim();
    // <input type="url"> requires a scheme; be lenient and prepend https://.
    if (url && !/^[a-zA-Z][a-zA-Z\d+\-.]*:\/\//.test(url)) {
      url = `https://${url}`;
    }
    if (!name || !url) {
      setError('Please enter both an app name and a URL.');
      return;
    }
    if (adding) return;
    setAdding(true);
    setError('');
    try {
      const res = await axios.post(
        `${API_BASE}/api/targets`,
        { friendly_name: name, target_url: url },
        { timeout: REQUEST_TIMEOUT_MS }
      );
      console.log("Added target:", res.data);
      setNewName('');
      setNewUrl('');
      await fetchTargets();
    } catch (error) {
      console.error("Failed to add target", error);
      if (error?.response) {
        setError(`Add failed (${error.response.status}): ${getFriendlyApiErrorMessage(error)}`);
      } else if (error?.code === 'ECONNABORTED') {
        setError('Add timed out — the backend may still be waking up. Check the list in a moment.');
      } else {
        setError(`Add failed: ${getFriendlyApiErrorMessage(error)}`);
      }
    } finally {
      if (isMountedRef.current) setAdding(false);
    }
  };

  return (
    <div className="dashboard-container">
      <header className="dashboard-header">
        <h1>Cloud Uptime Monitor</h1>
        <p>Live Site Connectivity Checker{lastUpdated ? ` • Updated ${lastUpdated.toLocaleTimeString()}` : ''}</p>
      </header>

      <div className="add-target-panel">
        <form onSubmit={handleAddTarget}>
          <input 
            type="text" 
            placeholder="App Name (e.g. Web-Genie)" 
            value={newName} 
            onChange={(e) => setNewName(e.target.value)} 
            required 
          />
          <input 
            type="url" 
            placeholder="https://example.com" 
            value={newUrl} 
            onChange={(e) => setNewUrl(e.target.value)} 
            required 
          />
          <button type="submit" disabled={adding}>{adding ? 'Adding...' : 'Add Target'}</button>
        </form>
        {error && <p className="error-msg" style={{ color: '#ff6b6b', marginTop: '10px' }}>{error}</p>}
      </div>

      <div className="targets-grid">
        {loading ? (
          <p className="no-data">Loading targets from Flask backend…</p>
        ) : targets.length === 0 ? (
          <p className="no-data">
            {error ? 'Could not load targets. The backend may be waking up — retrying automatically…' : 'No targets configured. Add one above to start monitoring.'}
          </p>
        ) : (
          targets.map((target, index) => {
            const logs = Array.isArray(target.uptime_logs) ? target.uptime_logs : [];
            return (
            <div key={target.site_id ?? target.target_url ?? index} className={`target-card ${target.is_online ? 'online' : 'offline'}`}>
              <div className="target-info">
                <h2>{target.friendly_name || 'Unnamed target'}</h2>
                <a href={target.target_url} target="_blank" rel="noopener noreferrer">{target.target_url}</a>
                <span className="status-badge">{target.is_online ? 'ONLINE' : 'OFFLINE'}</span>
              </div>

              <div className="chart-container" style={{ width: '100%', height: 220 }}>
                {logs.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={logs} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis dataKey="time" stroke="#888" tick={{ fontSize: 12 }} minTickGap={40} />
                    <YAxis dataKey="latency_ms" stroke="#888" tick={{ fontSize: 12 }} width={50} domain={['auto', 'auto']} />
                    <Tooltip contentStyle={{ backgroundColor: '#1e1e1e', border: 'none', color: '#fff' }} labelStyle={{ color: '#00ff9d' }} />
                    <Line type="monotone" dataKey="latency_ms" stroke="#00ff9d" strokeWidth={2} dot={{ r: 3, fill: '#00ff9d' }} activeDot={{ r: 5 }} isAnimationActive={true} animationDuration={500} />
                  </LineChart>
                </ResponsiveContainer>
                ) : (
                  <p className="no-data">No ping data yet — waiting for the next backend check…</p>
                )}
              </div>
            </div>
            );
          })
        )}
      </div>
    </div>
  );
}

export default App;


