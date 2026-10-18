// src/App.js
import React, { useState, useEffect, useCallback, useRef } from 'react';
import axios from 'axios';
import { io } from 'socket.io-client';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import './App.css';

// Fix "Failed to construct 'URL': Invalid URL":
// 1. CRA .env values must NOT be wrapped in quotes (they're read literally).
// 2. A bare host ("servercon.onrender.com") without scheme crashes the URL
//    parser -- auto-prepend https:// when no scheme is present.
// 3. On localhost default to the LOCAL Flask backend so local dev doesn't
//    wait on Render cold starts. Env override still wins when properly set.
function resolveApiBase() {
  const raw = (process.env.REACT_APP_API_BASE || '').trim().replace(/^["']|["']$/g, '');
  if (raw) {
    const withScheme = /^[a-zA-Z][a-zA-Z\d+\-.]*:\/\//.test(raw) ? raw : `https://${raw}`;
    return withScheme.endsWith('/') ? withScheme.slice(0, -1) : withScheme;
  }
  if (typeof window !== 'undefined' && /^(localhost|127\.0\.0\.1)/.test(window.location.hostname)) {
    return 'http://localhost:5000';
  }
  return 'https://servercon.onrender.com';
}
const API_BASE = resolveApiBase();
const REQUEST_TIMEOUT_MS = 15000;
const MAX_POINTS = 15; // rolling window so the chart never grows unbounded

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
  const [live, setLive] = useState(false);
  const isMountedRef = useRef(true);

  // Bottleneck #3 fix (REST polling): fetch the full history ONCE, then
  // listen on the 'ping_update' websocket for fresh points and append them
  // in place. No more 30s full re-fetch; chart updates the ms pings finish.
  const fetchInitialData = useCallback(async () => {
    try {
      // Connects to the Flask backend (local Flask on localhost, Render prod otherwise)
      const response = await axios.get(`${API_BASE}/api/targets`, { timeout: REQUEST_TIMEOUT_MS });
      const data = Array.isArray(response.data) ? response.data : [];
      if (!isMountedRef.current) return;
      setTargets(data);
      if (data.length > 0) setError('');
      setLastUpdated(new Date());
    } catch (error) {
      console.error("Error fetching data, Flask server might be down.", error);
      if (!isMountedRef.current) return;
      // Don't wipe existing cards on a transient failure (Render cold start)
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
    fetchInitialData();

    // Live channel: server emits 'ping_update' with fresh points each cycle
    const socket = io(API_BASE, { transports: ['websocket', 'polling'] });
    socket.on('connect', () => { if (isMountedRef.current) setLive(true); });
    socket.on('disconnect', () => { if (isMountedRef.current) setLive(false); });
    socket.on('ping_update', (newPings) => {
      if (!isMountedRef.current || !Array.isArray(newPings)) return;
      setTargets((prevTargets) => prevTargets.map((target) => {
        const ping = newPings.find((p) => p.site_id === target.site_id);
        if (!ping) return target;
        const point = {
          latency_ms: Number(ping.latency_ms) || 0,
          is_online: Boolean(ping.is_online),
          time: ping.time,
        };
        const updatedLogs = [...(target.uptime_logs || []), point].slice(-MAX_POINTS);
        return { ...target, is_online: point.is_online, uptime_logs: updatedLogs };
      }));
      setLastUpdated(new Date());
      setError('');
    });

    return () => {
      isMountedRef.current = false;
      socket.disconnect();
    };
  }, [fetchInitialData]);

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
      await fetchInitialData();
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

  const handleDeleteTarget = async (siteId, friendlyName) => {
    if (siteId == null) return;
    const label = friendlyName || `site ${siteId}`;
    if (typeof window !== 'undefined' && !window.confirm(`Stop monitoring "${label}"? Its history will be deleted.`)) {
      return;
    }
    // Optimistic removal so the card vanishes instantly; restored on failure.
    let snapshot = null;
    setTargets((prev) => {
      snapshot = prev;
      return prev.filter((t) => t.site_id !== siteId);
    });
    try {
      await axios.delete(`${API_BASE}/api/targets/${siteId}`, { timeout: REQUEST_TIMEOUT_MS });
      if (isMountedRef.current) setError('');
    } catch (error) {
      console.error('Failed to delete target', error);
      if (isMountedRef.current) {
        if (snapshot) setTargets(snapshot);
        if (error?.response?.status === 404) {
          setError(`"${label}" was already deleted. Refreshing…`);
          fetchInitialData();
        } else {
          setError(`Delete failed: ${getFriendlyApiErrorMessage(error)}`);
        }
      }
    }
  };

  return (
    <div className="dashboard-container">
      <header className="dashboard-header">
        <h1>Cloud Uptime Monitor</h1>
        <p>Live Site Connectivity Checker{lastUpdated ? ` • Updated ${lastUpdated.toLocaleTimeString()}` : ''}{live ? ' • ● LIVE' : ' • ○ connecting…'}</p>
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
            <div key={target.site_id ?? target.target_url ?? index} className={`target-card ${target.is_online ? 'online' : 'offline'}`} style={{ position: 'relative' }}>
              <button
                type="button"
                aria-label={`Delete ${target.friendly_name || target.target_url || 'target'}`}
                title="Stop monitoring this target"
                onClick={() => handleDeleteTarget(target.site_id, target.friendly_name)}
                style={{
                  position: 'absolute',
                  top: '10px',
                  right: '10px',
                  width: '28px',
                  height: '28px',
                  borderRadius: '50%',
                  border: '1px solid #444',
                  background: '#2d2d2d',
                  color: '#ff6b6b',
                  fontSize: '16px',
                  lineHeight: '1',
                  cursor: 'pointer',
                }}
              >
                ×
              </button>
              <div className="target-info">
                <h2 style={{ paddingRight: '36px' }}>{target.friendly_name || 'Unnamed target'}</h2>
                <a href={target.target_url} target="_blank" rel="noopener noreferrer">{target.target_url}</a>
                <div style={{ display: 'flex', gap: '8px', alignItems: 'center', marginTop: '6px' }}>
                  <span className="status-badge">
                    {target.is_online ? 'ONLINE' : 'OFFLINE'}
                  </span>
                  <span
                    title="Share of successful checks in the last 24 hours"
                    style={{
                      fontSize: '0.8rem',
                      padding: '2px 8px',
                      borderRadius: '12px',
                      backgroundColor: '#262626',
                      color: (target.uptime_pct ?? 100) >= 99 ? '#00ff9d' : (target.uptime_pct ?? 100) >= 95 ? '#ffcc00' : '#ff6b6b',
                      border: '1px solid #333'
                    }}
                  >
                    {target.uptime_pct !== undefined ? `${target.uptime_pct}% (24h)` : '100% (24h)'}
                  </span>
                </div>
              </div>

              <div className="chart-container" style={{ width: '100%', height: 220 }}>
                {logs.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={logs} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis dataKey="time" stroke="#888" tick={{ fontSize: 12 }} minTickGap={40} />
                    <YAxis dataKey="latency_ms" stroke="#888" tick={{ fontSize: 12 }} width={50} domain={['auto', 'auto']} />
                    <Tooltip contentStyle={{ backgroundColor: '#1e1e1e', border: 'none', color: '#fff' }} labelStyle={{ color: '#00ff9d' }} />
                    {/* VISUAL FIX: dot={false} removes per-point dots for the
                        smooth continuous reference-style line; activeDot shows
                        a dot only on hover. Animation off so live appends
                        don't replay the whole path. */}
                    <Line type="monotone" dataKey="latency_ms" stroke="#00ff9d" strokeWidth={2} dot={false} activeDot={{ r: 5 }} isAnimationActive={false} />
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


