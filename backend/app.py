import atexit
import os
import time
from urllib.parse import urlparse
import requests
import mysql.connector
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

# Load .env that lives next to this file (works regardless of cwd).
# File lives at servercon/backend/venv/app.py in this project.
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

app = Flask(__name__)
CORS(app)  # Allows React (http://localhost:3000) to call this Flask API

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "uptime_monitor")

REQUEST_TIMEOUT = int(os.getenv("PING_TIMEOUT", "10"))
PING_INTERVAL = int(os.getenv("PING_INTERVAL", "30"))


def _safe_close(obj):
    """Best-effort close for DB connections/cursors.

    `close()` can raise more than `mysql.connector.Error`
    (e.g. `AttributeError`/`ValueError` on a half-open object,
    or `Exception` during interpreter teardown), so callers
    must never let cleanup mask the real error.
    """
    if obj is None:
        return
    try:
        obj.close()
    except Exception:
        pass


# Database connection helper
def get_db_connection(with_db=True):
    kwargs = {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
    }
    if with_db:
        kwargs["database"] = DB_NAME
    return mysql.connector.connect(**kwargs)
    

# Initialize Database + Schema (creates DB and tables if they don't exist)
def init_db():
    # Step 1: ensure the database itself exists (bootstrap connection w/o DB).
    bootstrap = None
    cursor = None
    try:
        bootstrap = get_db_connection(with_db=False)
        cursor = bootstrap.cursor()
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}`")
        finally:
            _safe_close(cursor)
            cursor = None
    finally:
        _safe_close(cursor)
        _safe_close(bootstrap)

    # Step 2: ensure tables exist.
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monitored_targets (
                    site_id INT AUTO_INCREMENT PRIMARY KEY,
                    friendly_name VARCHAR(100) NOT NULL,
                    target_url VARCHAR(255) NOT NULL UNIQUE
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS uptime_logs (
                    log_id INT AUTO_INCREMENT PRIMARY KEY,
                    site_id INT,
                    ping_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    status_code INT NOT NULL,
                    latency_ms FLOAT NOT NULL,
                    is_online BOOLEAN NOT NULL,
                    FOREIGN KEY (site_id) REFERENCES monitored_targets(site_id) ON DELETE CASCADE
                )
            """)
            conn.commit()
        finally:
            _safe_close(cursor)
    finally:
        _safe_close(conn)

# The Background Worker: Pings sites and saves to MySQL
def ping_websites():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Pinging active targets...")
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
    except mysql.connector.Error as err:
        print(f"[ping] DB connection failed: {err}")
        return
    try:
        try:
            cursor = conn.cursor(dictionary=True)
        except mysql.connector.Error as err:
            print(f"[ping] Could not create cursor: {err}")
            return
        try:
            try:
                cursor.execute("SELECT site_id, target_url FROM monitored_targets")
                targets = cursor.fetchall()
            except mysql.connector.Error as err:
                print(f"[ping] SELECT failed: {err}")
                return

            for target in targets:
                site_id = target['site_id']
                url = target['target_url']
                status_code = 0
                latency_ms = 0.0
                is_online = False

                try:
                    start_time = time.time()
                    response = requests.get(url, timeout=REQUEST_TIMEOUT)
                    latency_ms = round((time.time() - start_time) * 1000, 2)
                    status_code = response.status_code
                    is_online = (200 <= status_code < 300)
                except requests.exceptions.RequestException as exc:
                    print(f"[ping] {url} unreachable: {exc}")
                    latency_ms = 0.0
                    status_code = 0
                    is_online = False

                # Log the result (one target failing must not kill the whole batch)
                try:
                    cursor.execute("""
                        INSERT INTO uptime_logs (site_id, status_code, latency_ms, is_online)
                        VALUES (%s, %s, %s, %s)
                    """, (site_id, status_code, latency_ms, is_online))
                except mysql.connector.Error as err:
                    print(f"[ping] INSERT failed for site {site_id}: {err}")
                    continue

            try:
                conn.commit()
            except mysql.connector.Error as err:
                print(f"[ping] COMMIT failed: {err}")
        finally:
            _safe_close(cursor)
    finally:
        _safe_close(conn)

# Background scheduler (Runs every PING_INTERVAL seconds).
# NOTE: Created/started lazily via start_scheduler() from the __main__
# block (see bottom of file) so that importing this module
# (tests, gunicorn, flask shell) does not create threads/jobs
# or ping the DB before tables exist.
scheduler = None


def get_scheduler():
    """Create the singleton scheduler on first use (no threads until start)."""
    global scheduler
    if scheduler is None:
        sched = BackgroundScheduler()
        sched.add_job(
            func=ping_websites,
            trigger="interval",
            seconds=PING_INTERVAL,
            id="ping_websites",
            coalesce=True,       # collapse missed runs into one
            max_instances=1,     # never overlap two ping cycles
            misfire_grace_time=15,
        )
        scheduler = sched
    return scheduler


def start_scheduler():
    """Start the interval job AFTER tables exist. Safe to call twice."""
    sched = get_scheduler()
    if not sched.running:
        sched.start()
        print(f"[startup] Scheduler started (every {PING_INTERVAL}s).")
    return sched


def _shutdown_scheduler():
    global scheduler
    try:
        if scheduler is not None and scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception:
        pass


atexit.register(_shutdown_scheduler)

# API Route: Get all targets & their chart data for React
@app.route('/api/targets', methods=['GET'])
def get_targets():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT site_id, friendly_name, target_url FROM monitored_targets ORDER BY site_id")
        targets = cursor.fetchall()

        for target in targets:
            site_id = target['site_id']
            # Fetch the last 15 pings for the React line chart
            cursor.execute("""
                SELECT ping_timestamp, latency_ms, is_online
                FROM uptime_logs
                WHERE site_id = %s
                ORDER BY ping_timestamp DESC LIMIT 15
            """, (site_id,))
            logs = cursor.fetchall()

            for log in logs:
                # Format the raw datetime object directly in Python
                if log.get('ping_timestamp'):
                    log['time'] = log['ping_timestamp'].strftime('%H:%M:%S')
                else:
                    log['time'] = None
                
                # Remove the raw datetime object so jsonify doesn't fail
                log.pop('ping_timestamp', None)

                # mysql-connector returns TINYINT/Decimal; normalize for JSON/Recharts
                log['is_online'] = bool(log.get('is_online'))
                try:
                    log['latency_ms'] = float(log.get('latency_ms', 0) or 0)
                except (TypeError, ValueError):
                    log['latency_ms'] = 0.0

            # Reverse to chronological order for the chart
            target['uptime_logs'] = logs[::-1]
            # Determine current status based on the most recent log
            target['is_online'] = bool(logs[0]['is_online']) if logs else False

        return jsonify(targets)
    except mysql.connector.Error as err:
        return jsonify({"error": f"Database error: {err}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        _safe_close(cursor)
        _safe_close(conn)


def _validate_target_payload(data):
    if not isinstance(data, dict):
        return None, "Request body must be JSON with 'friendly_name' and 'target_url'."
    name = (data.get('friendly_name') or '').strip()
    url = (data.get('target_url') or '').strip()
    if not name:
        return None, "'friendly_name' is required."
    if not url:
        return None, "'target_url' is required."
    if len(name) > 100:
        return None, "'friendly_name' must be <= 100 characters."
    if len(url) > 255:
        return None, "'target_url' must be <= 255 characters."
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        return None, "'target_url' must be a valid http(s) URL, e.g. https://example.com."
    return (name, url), None


# API Route: Add a new target from React
@app.route('/api/targets', methods=['POST'])
def add_target():
    data = request.get_json(silent=True)
    validated, err_msg = _validate_target_payload(data)
    if err_msg:
        return jsonify({"error": err_msg}), 400
    friendly_name, target_url = validated

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO monitored_targets (friendly_name, target_url) VALUES (%s, %s)",
            (friendly_name, target_url)
        )
        new_id = cursor.lastrowid
        conn.commit()
    except mysql.connector.Error as err:
        if getattr(err, 'errno', None) == 1062:
            return jsonify({"error": "That URL is already being monitored."}), 409
        return jsonify({"error": str(err)}), 400
    finally:
        _safe_close(cursor)
        _safe_close(conn)

    # Trigger an immediate ping so the UI updates instantly.
    # A ping failure must not turn a successful insert into an error.
    try:
        ping_websites()
    except Exception as exc:
        print(f"[add_target] immediate ping failed: {exc}")

    return jsonify({"message": "Target added successfully", "site_id": new_id}), 201


# API Route: Remove a target (lets the dashboard delete cards)
@app.route('/api/targets/<int:site_id>', methods=['DELETE'])
def delete_target(site_id):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM monitored_targets WHERE site_id = %s", (site_id,))
        conn.commit()
        if cursor.rowcount == 0:
            return jsonify({"error": "Target not found."}), 404
        return jsonify({"message": "Target deleted successfully"}), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 400
    finally:
        _safe_close(cursor)
        _safe_close(conn)

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "message": "Uptime Monitor API is running",
        "endpoints": {
            "health": "/api/health",
            "targets": "/api/targets",
        },
    }), 200


@app.errorhandler(404)
def not_found(e):
    return jsonify({
        "error": "Not found. Try /api/health or /api/targets.",
    }), 404


@app.route('/api/health', methods=['GET'])
def health():
    conn = None
    try:
        conn = get_db_connection()
        db_ok = True
    except Exception:
        db_ok = False
    finally:
        _safe_close(conn)
    sched = scheduler  # may be None until start_scheduler() runs
    return jsonify({
        "status": "ok" if db_ok else "degraded",
        "db": "up" if db_ok else "down",
        "scheduler_running": bool(sched is not None and sched.running),
        "time": datetime.now().isoformat(timespec="seconds"),
    }), (200 if db_ok else 503)


if __name__ == '__main__':
    try:
        init_db()
    except mysql.connector.Error as err:
        print(f"[startup] MySQL init failed: {err}")
        print(f"[startup] Check DB_HOST={DB_HOST} DB_PORT={DB_PORT} "
              f"DB_USER={DB_USER} DB_NAME={DB_NAME} and that MySQL is running.")
        raise SystemExit(1) from err

    # Start the scheduler AFTER tables exist (importing this file alone
    # must never start background threads).
    start_scheduler()

    # Ping once at boot so the React chart is not empty. Failure here
    # must not prevent the API from serving.
    try:
        ping_websites()
    except Exception as exc:
        print(f"[startup] Initial ping failed (server still running): {exc}")

    # Runs the Flask server on Port 5000
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)