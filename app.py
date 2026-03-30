import os,json,time,threading,urllib.request,sqlite3
from datetime import datetime
from flask import Flask,jsonify,render_template

app=Flask(__name__)
DB=os.environ.get("DB_PATH","dlp.db")
PORT=int(os.environ.get("PORT",8080))
TARGET=["peter pan","big thunder","hyperspace mountain","star wars","small world","star tours","pirates of the caribbean","phantom manor","buzz lightyear","tower of terror","spider-man","spider man","avengers assemble","crush","ratatouille","rapunzel","tangled","frozen ever after","frozen"]
FP=["peter pan","buzz lightyear","crush","ratatouille","frozen","avengers assemble"]
SLOTS=[{"key":"ouverture","start":9.5,"end":10.5},{"key":"matinee","start":10.5,"end":12},{"key":"midi","start":12,"end":14},{"key":"apresmidi","start":14,"end":16.5},{"key":"fin","start":16.5,"end":19},{"key":"soiree","start":19,"end":22}]

def db():
    c=sqlite3.connect(DB,check_same_thread=False)
    c.row_factory=sqlite3.Row
    return c

def init():
    with db() as c:
        c.execute("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY,ts INTEGER,slot TEXT,hour INTEGER,dk TEXT,name TEXT,land TEXT,wait INTEGER,open INTEGER)")
        c.execute("CREATE INDEX IF NOT EXISTS i1 ON t(name)")
        c.execute("CREATE INDEX IF NOT EXISTS i2 ON t(dk)")
        c.commit()

def slot(dt):
    h=dt.hour+dt.minute/60
    for s in SLOTS:
        if s["start"]<=h<s["end"]:return s["key"]
    return None

def avg(l):
    l=[x for x in l if x is not None]
    return round(sum(l)/len(l)) if l else None

def collect():
    try:
        req=urllib.request.Request(f"https://queue-times.com/parks/6/queue_times.json",headers={"User-Agent":"DLP/1.0"})
        with urllib.request.urlopen(req,timeout=20) as r:
            data=json.loads(r.read().decode())
        now=datetime.now()
        ts=int(time.time())
        sl=slot(now)
        dk=now.strftime("%Y-%m-%d")
        rides=[]
        for land in data.get("lands",[]):
            for ride in land.get("rides",[]):
                if any(k in ride["name"].lower() for k in TARGET):
                    rides.append((ts,sl,now.hour,dk,ride["name"],land["name"],ride["wait_time"] if ride["is_open"] else None,1 if ride["is_open"] else 0))
        with db() as c:
            c.executemany("INSERT INTO t(ts,slot,hour,dk,name,land,wait,open) VALUES(?,?,?,?,?,?,?,?)",rides)
            c.execute("DELETE FROM t WHERE ts<?",((ts-60*86400),))
            c.commit()
    except Exception as e:
        print(f"ERR: {e}")

def loop():
    time.sleep(3)
    while True:
        collect()
        time.sleep(3600)

@app.route("/health")
def health():return "OK"

@app.route("/api/status")
def status():
    with db() as c:
        n=c.execute("SELECT COUNT(*) as n FROM t").fetchone()["n"]
        last=c.execute("SELECT MAX(dk||' '||printf('%02d',hour)||'h') as t FROM t").fetchone()["t"]
        days=c.execute("SELECT COUNT(DISTINCT dk) as n FROM t").fetchone()["n"]
        rides=c.execute("SELECT COUNT(DISTINCT name) as n FROM t").fetchone()["n"]
    return jsonify({"ok":True,"total_rows":n,"last_collect":last or "—","days_collected":days,"rides_tracked":rides})

@app.route("/api/latest")
def latest():
    with db() as c:
        mts=c.execute("SELECT MAX(ts) as t FROM t").fetchone()["t"]
        if not mts:return jsonify([])
        rows=c.execute("SELECT name as ride_name,land,wait,open as is_open,dk as ts_str,slot FROM t WHERE ts=? ORDER BY COALESCE(wait,-1) DESC",(mts,)).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/stats")
def stats():
    with db() as c:
        names=[r[0] for r in c.execute("SELECT DISTINCT name FROM t ORDER BY name").fetchall()]
        res={}
        for name in names:
            gv=[r[0] for r in c.execute("SELECT wait FROM t WHERE name=? AND open=1 AND wait IS NOT NULL",(name,)).fetchall()]
            sa={}
            for s in SLOTS:
                v=[r[0] for r in c.execute("SELECT wait FROM t WHERE name=? AND slot=? AND open=1 AND wait IS NOT NULL",(name,s["key"])).fetchall()]
                sa[s["key"]]=avg(v)
            ha={}
            for h in range(9,23):
                v=[r[0] for r in c.execute("SELECT wait FROM t WHERE name=? AND hour=? AND open=1 AND wait IS NOT NULL",(name,h)).fetchall()]
                a=avg(v)
                if a is not None:ha[str(h)]=a
            da={}
            for r in c.execute("SELECT dk,ROUND(AVG(wait)) as a FROM t WHERE name=? AND open=1 AND wait IS NOT NULL GROUP BY dk",(name,)).fetchall():
                da[r["dk"]]=int(r["a"])
            lr=c.execute("SELECT land FROM t WHERE name=? LIMIT 1",(name,)).fetchone()
            res[name]={"land":lr["land"] if lr else "","global_avg":avg(gv),"slot_avgs":sa,"hourly_avgs":ha,"daily_avgs":da,"data_points":len(gv),"is_fp":any(f in name.lower() for f in FP)}
    return jsonify(res)

@app.route("/api/collect",methods=["POST"])
def api_collect():
    threading.Thread(target=collect,daemon=True).start()
    return jsonify({"status":"started"})

@app.route("/")
def index():return render_template("dashboard.html")

if __name__=="__main__":
    init()
    threading.Thread(target=loop,daemon=True).start()
    app.run(host="0.0.0.0",port=PORT)
