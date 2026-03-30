import os,json,time,threading,urllib.request,sqlite3
from datetime import datetime
from flask import Flask,jsonify,render_template
app=Flask(__name__)
DB="dlp.db"
PORT=int(os.environ.get("PORT",8080))
TARGET=["peter pan","big thunder","hyperspace mountain","star wars","small world","star tours","pirates of the caribbean","phantom manor","buzz lightyear","tower of terror","spider-man","spider man","avengers assemble","crush","ratatouille","rapunzel","tangled","frozen ever after","frozen"]
FP=["peter pan","buzz lightyear","crush","ratatouille","frozen","avengers assemble"]
SLOTS=[{"key":"ouverture","start":9.5,"end":10.5},{"key":"matinee","start":10.5,"end":12},{"key":"midi","start":12,"end":14},{"key":"apresmidi","start":14,"end":16.5},{"key":"fin","start":16.5,"end":19},{"key":"soiree","start":19,"end":22}]
conn=sqlite3.connect(DB,check_same_thread=False)
conn.row_factory=sqlite3.Row
conn.execute("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY,ts INTEGER,slot TEXT,hour INTEGER,dk TEXT,name TEXT,land TEXT,wait INTEGER,open INTEGER)")
conn.execute("CREATE INDEX IF NOT EXISTS i1 ON t(name)")
conn.execute("CREATE INDEX IF NOT EXISTS i2 ON t(dk)")
conn.commit()
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
      req=urllib.request.Request("https://queue-times.com/parks/6/queue_times.json",headers={"User-Agent":"Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1","Accept":"application/json","Accept-Language":"fr-FR,fr;q=0.9","Referer":"https://queue-times.com/"})
        with urllib.request.urlopen(req,timeout=20) as r:
            data=json.loads(r.read().decode())
        now=datetime.now()
        ts=int(time.time())
        sl=slot(now)
        dk=now.strftime("%Y-%m-%d")
        rows=[]
        for land in data.get("lands",[]):
            for ride in land.get("rides",[]):
                if any(k in ride["name"].lower() for k in TARGET):
                    rows.append((ts,sl,now.hour,dk,ride["name"],land["name"],ride["wait_time"] if ride["is_open"] else None,1 if ride["is_open"] else 0))
        if rows:
            conn.executemany("INSERT INTO t(ts,slot,hour,dk,name,land,wait,open) VALUES(?,?,?,?,?,?,?,?)",rows)
            conn.execute("DELETE FROM t WHERE ts<?",((ts-60*86400),))
            conn.commit()
            print(f"OK {len(rows)} rides")
    except Exception as e:
        print(f"ERR {e}")
def loop():
    collect()
    while True:
        time.sleep(3600)
        collect()
@app.route("/health")
def health():return "OK"
@app.route("/api/status")
def status():
    n=conn.execute("SELECT COUNT(*) as n FROM t").fetchone()["n"]
    last=conn.execute("SELECT MAX(ts_str) as t FROM(SELECT dk||' '||printf('%02d',hour)||'h' as ts_str FROM t)").fetchone()["t"]
    days=conn.execute("SELECT COUNT(DISTINCT dk) as n FROM t").fetchone()["n"]
    rides=conn.execute("SELECT COUNT(DISTINCT name) as n FROM t").fetchone()["n"]
    return jsonify({"ok":True,"total_rows":n,"last_collect":last or "—","days_collected":days,"rides_tracked":rides})
@app.route("/api/latest")
def latest():
    mts=conn.execute("SELECT MAX(ts) as t FROM t").fetchone()["t"]
    if not mts:return jsonify([])
    rows=conn.execute("SELECT name as ride_name,land,wait,open as is_open,dk as ts_str,slot FROM t WHERE ts=? ORDER BY COALESCE(wait,-1) DESC",(mts,)).fetchall()
    return jsonify([dict(r) for r in rows])
@app.route("/api/stats")
def stats():
    names=[r[0] for r in conn.execute("SELECT DISTINCT name FROM t ORDER BY name").fetchall()]
    res={}
    for name in names:
        gv=[r[0] for r in conn.execute("SELECT wait FROM t WHERE name=? AND open=1 AND wait IS NOT NULL",(name,)).fetchall()]
        sa={}
        for s in SLOTS:
            v=[r[0] for r in conn.execute("SELECT wait FROM t WHERE name=? AND slot=? AND open=1 AND wait IS NOT NULL",(name,s["key"])).fetchall()]
            sa[s["key"]]=avg(v)
        ha={}
        for h in range(9,23):
            v=[r[0] for r in conn.execute("SELECT wait FROM t WHERE name=? AND hour=? AND open=1 AND wait IS NOT NULL",(name,h)).fetchall()]
            a=avg(v)
            if a is not None:ha[str(h)]=a
        da={}
        for r in conn.execute("SELECT dk,ROUND(AVG(wait)) as a FROM t WHERE name=? AND open=1 AND wait IS NOT NULL GROUP BY dk",(name,)).fetchall():
            da[r["dk"]]=int(r["a"])
        lr=conn.execute("SELECT land FROM t WHERE name=? LIMIT 1",(name,)).fetchone()
        res[name]={"land":lr["land"] if lr else "","global_avg":avg(gv),"slot_avgs":sa,"hourly_avgs":ha,"daily_avgs":da,"data_points":len(gv),"is_fp":any(f in name.lower() for f in FP)}
    return jsonify(res)
@app.route("/api/collect",methods=["POST"])
def api_collect():
    threading.Thread(target=collect,daemon=True).start()
    return jsonify({"status":"started"})
@app.route("/")
def index():return render_template("dashboard.html")
if __name__=="__main__":
    threading.Thread(target=loop,daemon=True).start()
    app.run(host="0.0.0.0",port=PORT)
