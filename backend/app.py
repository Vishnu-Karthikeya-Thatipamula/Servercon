import atexit
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
import requests
import mysql.connector
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

# Load .env that lives next to this file (works regardless of cwd).
# File lives at servercon/backend/app.py in this project.
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

app = Flask(__name__)
# Allow requests from all origins (including your localhost and future frontend host)
CORS(app, resources={r"/api/*": {"origins": "*"}})# Allows React (http://localhost:3000) to call this Flask API
# Real-time pushes: SocketIO emits fresh ping points the ms the batch finishes
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

DB_HOST = os.getenv("DB_HOST", "localhost") or "localhost"
DB_USER = os.getenv("DB_USER", "root") or "root"
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "uptime_monitor") or "uptime_monitor"


def _int_env(name, default, min_value=None, max_value=None, aliases=()):
    """Parse an int env var without ever crashing import/startup.

    Empty / missing / non-numeric values fall back to ``default``
    (with a warning) instead of raising ``ValueError`` from ``int()``.
    Optional min/max clamping keeps scheduler/timeout values sane.
    """
    raw = os.getenv(name)
    if (raw is None or str(raw).strip() == "") and aliases:
        for alias in aliases:
            alt = os.getenv(alias)
            if alt is not None and str(alt).strip() != "":
                raw = alt
                break
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        print(f"[config] Invalid {name}={raw!r}; using default {default}.")
        return default
    if min_value is not None and value < min_value:
        print(f"[config] {name}={value} below minimum {min_value}; clamped.")
        return min_value
    if max_value is not None and value > max_value:
        print(f"[config] {name}={value} above maximum {max_value}; clamped.")
        return max_value
    return value


DB_PORT = _int_env("DB_PORT", 3306, min_value=1, max_value=65535)
# NOTE: historic env name is PING_TIMEOUT; also accept REQUEST_TIMEOUT.
REQUEST_TIMEOUT = _int_env("REQUEST_TIMEOUT", 10, min_value=1, max_value=120,
                           aliases=("PING_TIMEOUT",))
# 1s updates: aggressive (sites may rate-limit; DB fills 30x faster).
# Bump to 5-10s if charts flatline or Aiven storage spikes.
PING_INTERVAL = _int_env("PING_INTERVAL", 1, min_value=1, max_value=86400)

ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()
# Tracks the previous online/offline state of each site_id so webhooks fire
# ONLY on state transitions (ONLINE->OFFLINE or OFFLINE->ONLINE), never on
# every failed check. Seeded from DB on first ping cycle (see ping_websites).
_last_known_status = {}


def _send_webhook_alert(friendly_name, url, is_online, status_code):
    """Fire a Discord/Slack-compatible webhook on a state transition."""
    if not ALERT_WEBHOOK_URL:
        return
    state_str = "🟢 RECOVERED (ONLINE)" if is_online else f"🔴 DOWN (Status: {status_code})"
    payload = {
        "content": f"**[Uptime Monitor Alert]**\n**Target:** {friendly_name} ({url})\n**Status:** {state_str}\n**Time (UTC):** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    }
    try:
        requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=5)
    except Exception as exc:
        print(f"[webhook] Failed to send alert: {exc}")

# Fail fast on an unsafe database identifier since it is interpolated
# into CREATE DATABASE (identifiers cannot be parameterised).
if not re.fullmatch(r"[A-Za-z0-9_]+", DB_NAME):
    raise SystemExit(
        f"[config] Refusing to start: unsafe DB_NAME={DB_NAME!r}. "
        "Use only letters, digits and underscore."
    )


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


def _safe_rollback(conn):
    """Best-effort rollback so one bad INSERT doesn't poison the batch."""
    if conn is None:
        return
    try:
        conn.rollback()
    except Exception:
        pass


# Database connection helper
def get_db_connection(with_db=True):
    kwargs = {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "connection_timeout": _int_env("DB_CONNECT_TIMEOUT", 10, min_value=1, max_value=60),
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
            # Index for the 24h SLA aggregation + ranked history query.
            # Critical at 1s ping intervals (~86k rows/target/day).
            try:
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_logs_site_time
                    ON uptime_logs (site_id, ping_timestamp DESC)
                """)
            except mysql.connector.Error:
                # MySQL < 8 / MariaDB may not support IF NOT EXISTS on indexes;
                # fall back to a plain CREATE and ignore "duplicate key" (1061).
                try:
                    cursor.execute("""
                        CREATE INDEX idx_logs_site_time
                        ON uptime_logs (site_id, ping_timestamp DESC)
                    """)
                except mysql.connector.Error as idx_err:
                    if getattr(idx_err, 'errno', None) != 1061:
                        raise
            conn.commit()
        finally:
            _safe_close(cursor)
    finally:
        _safe_close(conn)

# The Background Worker: Pings sites in PARALLEL and saves to MySQL in bulk.
# Bottleneck #1 fix (synchronous pinging): ThreadPoolExecutor(max_workers=20)
# pings all targets at once instead of one-by-one, so 50 sites take ~2s
# instead of ~50s. Results are bulk-inserted with a single executemany.
HISTORY_LIMIT = _int_env("HISTORY_LIMIT", 15, min_value=5, max_value=300)
PING_WORKERS = _int_env("PING_WORKERS", 20, min_value=1, max_value=100)


def _ping_one(target):
    """Worker: ping one target dict. Returns a React-ready result dict."""
    site_id = target['site_id']
    url = target['target_url']
    status_code = 0
    latency_ms = 0.0
    is_online = False
    try:
        start_time = time.time()
        response = requests.get(
            url,
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": "UptimeMonitor/1.0"},
        )
        latency_ms = round((time.time() - start_time) * 1000, 2)
        status_code = response.status_code or 0
        is_online = (200 <= status_code < 300)
    except requests.exceptions.RequestException as exc:
        print(f"[ping] {url} unreachable: {exc}")
    except Exception as exc:  # defensive: never let one URL kill the batch
        print(f"[ping] {url} unexpected error: {exc}")
    return {
        'site_id': site_id,
        'status_code': status_code,
        'latency_ms': latency_ms,
        'is_online': is_online,
        'time': datetime.now().strftime('%H:%M:%S'),  # pre-formatted for React
    }


def ping_websites(site_id=None, emit=True):
    """Ping all targets (or one site_id) in parallel, bulk-insert, push live."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Pinging active targets in parallel...")
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
    except Exception as err:  # mysql errors + DNS/socket errors
        print(f"[ping] DB connection failed: {err}")
        return []
    results = []
    try:
        try:
            cursor = conn.cursor(dictionary=True)
        except Exception as err:
            print(f"[ping] Could not create cursor: {err}")
            return []
        try:
            # CLEANUP: keep the table lean -- drop logs older than 7 days
            try:
                cursor.execute("DELETE FROM uptime_logs WHERE ping_timestamp < NOW() - INTERVAL 7 DAY")
            except mysql.connector.Error as err:
                print(f"[ping] cleanup DELETE failed (non-fatal): {err}")
                _safe_rollback(conn)

            try:
                if site_id is None:
                    cursor.execute("SELECT site_id, friendly_name, target_url FROM monitored_targets")
                else:
                    cursor.execute(
                        "SELECT site_id, friendly_name, target_url FROM monitored_targets WHERE site_id = %s",
                        (site_id,),
                    )
                targets = cursor.fetchall()
            except mysql.connector.Error as err:
                print(f"[ping] SELECT failed: {err}")
                return []

            if not targets:
                return []

            # Seed the transition tracker from the latest DB row so a server
            # restart doesn't fire false DOWN/RECOVERED alerts for the state
            # that already existed before boot.
            if not _last_known_status:
                try:
                    cursor.execute("""
                        SELECT t.site_id, l.is_online FROM monitored_targets t
                        LEFT JOIN uptime_logs l ON l.log_id = (
                            SELECT log_id FROM uptime_logs
                            WHERE site_id = t.site_id
                            ORDER BY ping_timestamp DESC, log_id DESC LIMIT 1
                        )
                    """)
                    for row in cursor.fetchall():
                        if row.get('is_online') is not None:
                            _last_known_status[row['site_id']] = bool(row['is_online'])
                except mysql.connector.Error as err:
                    print(f"[ping] state-seed query failed (non-fatal): {err}")

            # PARALLEL PING: all targets at once
            with ThreadPoolExecutor(max_workers=PING_WORKERS) as executor:
                results = list(executor.map(_ping_one, targets))

            # STATE-TRANSITION ALERTS: fire webhook ONLY when a site flips
            # ONLINE<->OFFLINE (never on every failed check -- no spam).
            for target, res in zip(targets, results):
                sid = target['site_id']
                current_online = bool(res['is_online'])
                prev_online = _last_known_status.get(sid)
                if prev_online is not None and prev_online != current_online:
                    _send_webhook_alert(
                        target.get('friendly_name') or f"site {sid}",
                        target['target_url'],
                        current_online,
                        res['status_code'],
                    )
                _last_known_status[sid] = current_online

            # BULK INSERT: one executemany instead of N single INSERTs
            try:
                cursor.executemany(
                    """INSERT INTO uptime_logs (site_id, status_code, latency_ms, is_online)
                       VALUES (%s, %s, %s, %s)""",
                    [(r['site_id'], r['status_code'], r['latency_ms'], r['is_online'])
                     for r in results],
                )
                conn.commit()
            except mysql.connector.Error as err:
                print(f"[ping] BULK INSERT failed: {err}")
                _safe_rollback(conn)
                return []
        finally:
            _safe_close(cursor)
            cursor = None
    finally:
        _safe_close(conn)

    # WEBSOCKET PUSH: send only the fresh points; React appends them live
    if emit and results:
        try:
            socketio.emit('ping_update', results)
        except Exception as exc:
            print(f"[ping] socket emit failed (non-fatal): {exc}")
    return results

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
# Bottleneck #2 fix (N+1 queries): exactly 2 SQL statements no matter how
# many targets -- one for targets, one window-function query fetching the
# last HISTORY_LIMIT logs for ALL targets at once. Logs are grouped in
# Python memory. Tiebreak on log_id keeps same-second pings ordered.
@app.route('/api/targets', methods=['GET'])
def get_targets():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Query 1: all targets
        cursor.execute("SELECT site_id, friendly_name, target_url FROM monitored_targets ORDER BY site_id")
        targets = cursor.fetchall()

        # Query 2: 24-hour SLA % for every target in ONE aggregation pass.
        # At 1s pinging this scans ~86k rows/target/day -- the ping_timestamp
        # index (created in init_db) keeps it fast even at that volume.
        sla_rows = {}
        try:
            cursor.execute("""
                SELECT site_id,
                       ROUND(AVG(CASE WHEN is_online THEN 100.0 ELSE 0.0 END), 2) AS uptime_pct
                FROM uptime_logs
                WHERE ping_timestamp >= NOW() - INTERVAL 24 HOUR
                GROUP BY site_id
            """)
            sla_rows = {row['site_id']: float(row['uptime_pct']) for row in cursor.fetchall()}
        except mysql.connector.Error as err:
            print(f"[targets] SLA aggregation failed (non-fatal, defaulting 100): {err}")

        # Query 3: last HISTORY_LIMIT logs for every target in one shot
        cursor.execute("""
            WITH RankedLogs AS (
                SELECT site_id, ping_timestamp, latency_ms, is_online,
                       ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY ping_timestamp DESC, log_id DESC) AS rn
                FROM uptime_logs
            )
            SELECT site_id, ping_timestamp, latency_ms, is_online
            FROM RankedLogs
            WHERE rn <= %s
        """, (HISTORY_LIMIT,))
        all_logs = cursor.fetchall()

        # Group by site_id in memory
        logs_by_site = {t['site_id']: [] for t in targets}
        for log in all_logs:
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

            if log['site_id'] in logs_by_site:
                logs_by_site[log['site_id']].append(log)

        for target in targets:
            # Ranked query returns newest-first; reverse for chart left-to-right
            site_logs = logs_by_site.get(target['site_id'], [])[::-1]
            target['uptime_logs'] = site_logs
            # Determine current status based on the most recent log
            target['is_online'] = bool(site_logs[-1]['is_online']) if site_logs else False
            # 24h SLA badge: default 100 when a target has no logs in the window
            target['uptime_pct'] = sla_rows.get(target['site_id'], 100.0)

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
    new_id = None
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
        _safe_rollback(conn)
        if getattr(err, 'errno', None) == 1062:
            return jsonify({"error": "That URL is already being monitored."}), 409
        return jsonify({"error": str(err)}), 400
    except Exception as exc:
        _safe_rollback(conn)
        return jsonify({"error": f"Database error: {exc}"}), 500
    finally:
        _safe_close(cursor)
        _safe_close(conn)

    if new_id is None:  # defensive: commit succeeded but no id returned
        return jsonify({"error": "Insert succeeded but no id was returned."}), 500

    # Trigger an immediate ping of JUST this target so the UI updates
    # instantly (pinging everything here would be slow with many targets).
    # A ping failure must not turn a successful insert into an error.
    try:
        ping_websites(site_id=new_id)
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
        _safe_rollback(conn)
        return jsonify({"error": str(err)}), 400
    except Exception as exc:
        _safe_rollback(conn)
        return jsonify({"error": f"Database error: {exc}"}), 500
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
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        db_ok = True
    except Exception:
        db_ok = False
    finally:
        _safe_close(cursor)
        _safe_close(conn)
    sched = scheduler  # may be None until start_scheduler() runs
    return jsonify({
        "status": "ok" if db_ok else "degraded",
        "db": "up" if db_ok else "down",
        "scheduler_running": bool(sched is not None and sched.running),
        "time": datetime.now().isoformat(timespec="seconds"),
    }), (200 if db_ok else 503)


# Auto-start the scheduler when running under a WSGI server (Render/gunicorn).
# Local `python app.py` still uses the __main__ block below. This is guarded so
# tests/imports don't spawn threads unless explicitly enabled.
if os.getenv("ENABLE_SCHEDULER", "false").lower() in ("1", "true", "yes"):
    try:
        init_db()
    except Exception as exc:
        print(f"[wsgi] DB init skipped/failed: {exc}")
    try:
        start_scheduler()
        print("[wsgi] Background scheduler started.")
    except Exception as exc:
        print(f"[wsgi] Scheduler failed to start: {exc}")


if __name__ == '__main__':
    try:
        init_db()
    except Exception as err:  # mysql errors + DNS/socket/timeout errors
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
        ping_websites(emit=False)  # no socket listeners yet at boot
    except Exception as exc:
        print(f"[startup] Initial ping failed (server still running): {exc}")

    # Runs the Flask+SocketIO server on Port 5000.
    # IMPORTANT: use socketio.run, not app.run, or websocket pushes never work.
    socketio.run(app, host="0.0.0.0", port=5000, debug=True, use_reloader=False)