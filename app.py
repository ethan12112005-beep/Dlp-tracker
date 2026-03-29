"""
DLP Wait Tracker — Serveur Railway
Collecte auto toutes les heures · API JSON · Dashboard mobile
"""
import os, json, time, threading, urllib.request, logging, sqlite3
from datetime import datetime
from flask import Flask, jsonify, render_template, request

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

app = Flask(__name__)

# ── CONFIG ─────────────────────────────────────────────────────
PARK_ID = 6
API_URL = f"https://queue-times.com/parks/{PARK_ID}/queue_times.json"
DB_PATH = os.environ.get("DB_PATH", "/data/dlp_data.db")
PORT    = int(os.environ.get("PORT", 8080))
INTERVAL_MINUTES = 60

TARGET_KEYWORDS = [
    "peter pan", "big thunder", "hyperspace mountain", "star wars",
    "small world", "star tours", "pirates of the caribbean",
    "phantom manor", "buzz lightyear", "tower of terror",
    "spider-man", "spider man", "avengers assemble",
    "crush", "ratatouille", "rapunzel", "tangled",
    "frozen ever after", "frozen",
]

SLOTS = [
    {"key":"ouverture", "label":"Ouverture",  "range":"9h30–10h30",  "icon":"🌅", "start":9.5,  "end":10.5},
    {"key":"matinee",   "label":"Matinée",    "range":"10h30–12h00", "icon":"☀️", "start":10.5, "end":12.0},
    {"key":"midi",      "label":"Midi",       "range":"12h00–14h00", "icon":"🕛", "start":12.0, "end":14.0},
    {"key":"apresmidi", "label":"Après-midi", "range":"14h00–16h30", "icon":"🌤️","start":14.0, "end":16.5},
    {"key":"fin",       "label":"Fin de J.",  "range":"16h30–19h00", "icon":"🌇", "start":16.5, "end":19.0},
    {"key":"soiree",    "label":"Soirée",     "range":"19h00–22h00", "icon":"🌙", "start":19.0, "end":22.0},
]

FAST_PASS = [
    "Peter Pan", "Buzz Lightyear Laser Blast", "Crush's Coaster",
    "Ratatouille", "Frozen Ever After", "Avengers Assemble"
]

# ── DATABASE ───────────────────────────────────────────────────
def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS collections (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        INTEGER NOT NULL,
                ts_str    TEXT,
                slot      TEXT,
                hour      INTEGER,
                date_key  TEXT,
                ride_id   TEXT,
                ride_name TEXT,
                land      TEXT,
                wait      INTEGER,
                is_open   INTEGER
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dk   ON collections(date_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_name ON collections(ride_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ts   ON collections(ts)")
        conn.commit()
    log.info(f"✅ DB prête : {DB_PATH}")

# ── HELPERS ────────────────────────────────────────────────────
def decimal_hour(dt=None):
    dt = dt or datetime.now()
    return dt.hour + dt.minute / 60

def get_slot(dt=None):
    h = decimal_hour(dt)
    for s in SLOTS:
        if s["start"] <= h < s["end"]:
            return s["key"]
    return None

def is_target(name):
    return any(k in name.lower() for k in TARGET_KEYWORDS)

def is_fp(name):
    return any(fp.lower() in name.lower() for fp in FAST_PASS)

def avg_list(lst):
    lst = [x for x in lst if x is not None]
    return round(sum(lst)/len(lst)) if lst else None

# ── COLLECTOR ──────────────────────────────────────────────────
_collect_lock = threading.Lock()
_last_collect_ts = 0

def collect():
    global _last_collect_ts
    if not _collect_lock.acquire(blocking=False):
        return
    try:
        log.info("🔄 Collecte en cours...")
        req = urllib.request.Request(API_URL, headers={"User-Agent": "DLP-Tracker/3.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())

        now   = datetime.now()
        ts    = int(time.time())
        slot  = get_slot(now)
        dk    = now.strftime("%Y-%m-%d")
        hour  = now.hour
        ts_str = now.strftime("%Y-%m-%d %H:%M:%S")

        rides = []
        if "lands" in data:
            for land in data["lands"]:
                for ride in land.get("rides", []):
                    if is_target(ride.get("name", "")):
                        rides.append({
                            "id":   str(ride["id"]),
                            "name": ride["name"],
                            "land": land["name"],
                            "wait": ride["wait_time"] if ride["is_open"] else None,
                            "open": 1 if ride["is_open"] else 0,
                        })

        if not rides:
            log.warning("⚠️ Aucune attraction trouvée")
            return

        with get_db() as conn:
            for r in rides:
                conn.execute("""
                    INSERT INTO collections
                    (ts,ts_str,slot,hour,date_key,ride_id,ride_name,land,wait,is_open)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (ts, ts_str, slot, hour, dk,
                      r["id"], r["name"], r["land"], r["wait"], r["open"]))
            conn.execute("DELETE FROM collections WHERE ts < ?", (ts - 60*86400,))
            conn.commit()

        _last_collect_ts = ts
        open_rides = [r for r in rides if r["open"] and r["wait"] is not None]
        log.info(f"✅ {len(rides)} ciblées · {len(open_rides)} ouvertes · slot={slot}")

    except Exception as e:
        log.error(f"❌ {e}")
    finally:
        _collect_lock.release()

def scheduler_loop():
    """Boucle infinie : collecte toutes les INTERVAL_MINUTES"""
    time.sleep(5)  # laisse Flask démarrer
    while True:
        collect()
        time.sleep(INTERVAL_MINUTES * 60)

# ── API ENDPOINTS ──────────────────────────────────────────────
@app.route("/health")
def health():
    return "OK", 200

@app.route("/api/status")
def api_status():
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) as n FROM collections").fetchone()["n"]
        last  = conn.execute("SELECT MAX(ts_str) as t FROM collections").fetchone()["t"]
        days  = conn.execute("SELECT COUNT(DISTINCT date_key) as n FROM collections").fetchone()["n"]
        rides = conn.execute("SELECT COUNT(DISTINCT ride_name) as n FROM collections").fetchone()["n"]
    return jsonify({
        "ok": True,
        "total_rows": total,
        "last_collect": last or "—",
        "days_collected": days,
        "rides_tracked": rides,
        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
    })

@app.route("/api/latest")
def api_latest():
    with get_db() as conn:
        last_ts = conn.execute("SELECT MAX(ts) as t FROM collections").fetchone()["t"]
        if not last_ts:
            return jsonify([])
        rows = conn.execute("""
            SELECT ride_name, land, wait, is_open, ts_str, slot, hour
            FROM collections WHERE ts=? ORDER BY COALESCE(wait,-1) DESC
        """, (last_ts,)).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/stats")
def api_stats():
    with get_db() as conn:
        names = [r[0] for r in conn.execute(
            "SELECT DISTINCT ride_name FROM collections ORDER BY ride_name").fetchall()]
        result = {}
        for name in names:
            base = {"ride_name": name}
            # global
            gvals = [r[0] for r in conn.execute(
                "SELECT wait FROM collections WHERE ride_name=? AND is_open=1 AND wait IS NOT NULL",
                (name,)).fetchall()]
            base["global_avg"] = avg_list(gvals)
            base["data_points"] = len(gvals)
            # land
            lr = conn.execute("SELECT land FROM collections WHERE ride_name=? LIMIT 1",(name,)).fetchone()
            base["land"] = lr["land"] if lr else ""
            # slot avgs
            slot_avgs = {}
            for s in SLOTS:
                vals = [r[0] for r in conn.execute(
                    "SELECT wait FROM collections WHERE ride_name=? AND slot=? AND is_open=1 AND wait IS NOT NULL",
                    (name, s["key"])).fetchall()]
                slot_avgs[s["key"]] = avg_list(vals)
            base["slot_avgs"] = slot_avgs
            # hourly avgs
            hourly = {}
            for h in range(9, 23):
                vals = [r[0] for r in conn.execute(
                    "SELECT wait FROM collections WHERE ride_name=? AND hour=? AND is_open=1 AND wait IS NOT NULL",
                    (name, h)).fetchall()]
                v = avg_list(vals)
                if v is not None:
                    hourly[str(h)] = v
            base["hourly_avgs"] = hourly
            # daily avgs
            daily_rows = conn.execute("""
                SELECT date_key, ROUND(AVG(wait)) as aw FROM collections
                WHERE ride_name=? AND is_open=1 AND wait IS NOT NULL
                GROUP BY date_key ORDER BY date_key
            """, (name,)).fetchall()
            base["daily_avgs"] = {r["date_key"]: int(r["aw"]) for r in daily_rows}
            base["is_fp"] = is_fp(name)
            result[name] = base
    return jsonify(result)

@app.route("/api/daily")
def api_daily():
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db() as conn:
        rows = conn.execute("""
            SELECT ride_name, land, slot, hour, wait, is_open, ts_str
            FROM collections WHERE date_key=? AND is_open=1 AND wait IS NOT NULL
            ORDER BY ts
        """, (today,)).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/collect", methods=["POST"])
def api_collect():
    threading.Thread(target=collect, daemon=True).start()
    return jsonify({"status": "started"})

@app.route("/api/slots")
def api_slots():
    return jsonify(SLOTS)

@app.route("/")
def index():
    return render_template("dashboard.html")

# ── START ──────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)
