// src/App.js
import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import './App.css';

function App() {
  const [targets, setTargets] = useState([]);
  const [newUrl, setNewUrl] = useState('');
  const [newName, setNewName] = useState('');

  const fetchTargets = async () => {
    try {
      // Connects to the Flask backend we will build next
      const response = await axios.get('http://localhost:5000/api/targets');
      setTargets(response.data);
    } catch (error) {
      console.error("Error fetching data, Flask server might be down.", error);
    }
  };

  useEffect(() => {
    fetchTargets();
    // Auto-refresh the dashboard every 5 seconds
    const interval = setInterval(fetchTargets, 5000); 
    return () => clearInterval(interval);
  }, []);

  const handleAddTarget = async (e) => {
    e.preventDefault();
    try {
      await axios.post('http://localhost:5000/api/targets', {
        friendly_name: newName,
        target_url: newUrl
      });
      setNewName('');
      setNewUrl('');
      fetchTargets();
    } catch (error) {
      console.error("Failed to add target", error);
    }
  };

  return (
    <div className="dashboard-container">
      <header className="dashboard-header">
        <h1>Cloud Uptime Monitor</h1>
        <p>Live Site Connectivity Checker</p>
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
          <button type="submit">Add Target</button>
        </form>
      </div>

      <div className="targets-grid">
        {targets.length === 0 ? (
          <p className="no-data">No targets configured. Waiting for Flask backend...</p>
        ) : (
          targets.map(target => (
            <div key={target.site_id} className={`target-card ${target.is_online ? 'online' : 'offline'}`}>
              <div className="target-info">
                <h2>{target.friendly_name}</h2>
                <a href={target.target_url} target="_blank" rel="noreferrer">{target.target_url}</a>
                <span className="status-badge">{target.is_online ? 'ONLINE' : 'OFFLINE'}</span>
              </div>

              <div className="chart-container">
                <ResponsiveContainer width="100%" height={150}>
                  <LineChart data={target.uptime_logs}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis dataKey="time" stroke="#888" />
                    <YAxis dataKey="latency_ms" stroke="#888" />
                    <Tooltip contentStyle={{ backgroundColor: '#1e1e1e', border: 'none', color: '#fff' }} />
                    <Line type="monotone" dataKey="latency_ms" stroke="#00ff9d" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

export default App;


