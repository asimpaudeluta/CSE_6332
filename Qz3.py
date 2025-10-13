import os, json, mimetypes, csv, io, requests, sqlite3, tempfile
from flask import Flask, request, Response, render_template, redirect, jsonify, url_for
import re, operator
import pandas as pd
import pyodbc
from datetime import datetime
app = Flask(__name__)

TEXT_FILE_NAME = "_placeholder.log"
IMAGE_FILE_NAME = "mypic.jpg"
CONTAINER_URL = "https://cse6332.blob.core.windows.net/privatecontainer"
DIRECTORY =  "Qz3"
SAS_TOKEN = os.getenv("SAS_TOKEN")

if not SAS_TOKEN:
    try:
        with open("secrets.json") as f:
            secrets = json.load(f)
        SAS_TOKEN = str(secrets["SAS_TOKEN"])
    except Exception:
        SAS_TOKEN = ""

#--- GENERAL HELPERS ---#
def get_blob_url(blob_name: str) -> str:
    token = SAS_TOKEN.lstrip("?")
    sep = "?" if "?" not in CONTAINER_URL else "&"
    return f"{CONTAINER_URL}/{DIRECTORY}/{blob_name}{sep}{token}" if token else f"{CONTAINER_URL}/{DIRECTORY}/{blob_name}"

def blob_exists(filename: str) -> bool:
    url = get_blob_url(filename)
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except requests.RequestException:
        return False

def read_text_blob(filename: str = TEXT_FILE_NAME) -> str:
    url = get_blob_url(filename)
    try:
        r = requests.get(url, timeout=10)
        if r.ok:
            return r.text
        return f"Failed to load text from blob. HTTP {r.status_code}"
    except requests.RequestException as e:
        return f"Failed to load text from blob. Error: {e}"

def write_text_blob(comment: str, filename: str = TEXT_FILE_NAME) -> bool:
    if comment is None or comment.strip() == "":
        return False
    url = get_blob_url(filename)
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "text/plain; charset=utf-8",
    }
    try:
        r = requests.put(url, headers=headers, data=comment.encode("utf-8"), timeout=10)
        return r.status_code in (201, 202)
    except requests.RequestException:
        return False

@app.route("/get_image")
def get_image():
    url = get_blob_url(IMAGE_FILE_NAME)
    try:
        r = requests.get(url, timeout=10)
        if not r.ok:
            return "Failed to load image from blob.", 502
        return Response(r.content, mimetype=mimetypes.guess_type(IMAGE_FILE_NAME)[0])
    except requests.RequestException:
        return "Failed to load image from blob.", 502

#--- METADATA HELPER ---#
def read_csv_rows(filename: str = "data.csv"):
    url = get_blob_url(filename)
    r = requests.get(url, timeout=10)
    if not r.ok:
        return None, f"HTTP {r.status_code}"
    f = io.StringIO(r.text, newline="")
    reader = csv.reader(f)
    rows = []
    for row in reader:
        rows.append([ (c if c.strip() != "" else None) for c in row ])
    return rows, None

#--- ROUTES ---#
@app.route("/", methods=["GET"])
def redirect_root():
    return redirect(url_for("qz3"))

@app.route("/Qz3", methods=["GET"])
def qz3():
    last_download_time = None
    if blob_exists("date.txt"):
        date_content = read_text_blob("date.txt")
        if date_content and not date_content.startswith("Failed"):
            last_download_time = date_content.strip()
    return render_template(
        "Qz3.html",
        last_download_time= last_download_time
    )

import time

AZURE_SQL_SERVER = os.getenv("AZURE_SQL_SERVER", "querytest-server.database.windows.net")
AZURE_SQL_DATABASE = os.getenv("AZURE_SQL_DATABASE", "querytest-database")
AZURE_SQL_USER = os.getenv("AZURE_SQL_USER", "querytest-server-admin@querytest-server")
AZURE_SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD", "sevpwA.P2#")
AZURE_SQL_ENCRYPT = os.getenv("AZURE_SQL_ENCRYPT", "yes")          # yes/no
AZURE_SQL_TRUST_CERT = os.getenv("AZURE_SQL_TRUST_CERT", "no")     # no/yes
AZURE_SQL_TIMEOUT = int(os.getenv("AZURE_SQL_TIMEOUT", "30"))

ODBC_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER=tcp:{AZURE_SQL_SERVER},1433;"
    f"DATABASE={AZURE_SQL_DATABASE};"
    f"UID={AZURE_SQL_USER};PWD={AZURE_SQL_PASSWORD};"
    f"Encrypt={AZURE_SQL_ENCRYPT};TrustServerCertificate={AZURE_SQL_TRUST_CERT};"
    f"Connection Timeout={AZURE_SQL_TIMEOUT};"
)

def get_conn():
    return pyodbc.connect(ODBC_CONN_STR, autocommit=False)

def redis_flush_query_cache():
    return

TABLE = "dbo.quakes"
DDL_CREATE = f"""
CREATE TABLE {TABLE}(
    [time]      INT           NOT NULL,
    [latitude]  FLOAT         NULL,
    [longitude] FLOAT         NULL,
    [depth]     FLOAT         NULL,
    [mag]       FLOAT         NULL,
    [net]       NVARCHAR(16)  NULL,
    [id]        NVARCHAR(64)  NOT NULL PRIMARY KEY
);
"""
DDL_INDEXES = [
    f"CREATE INDEX IX_quakes_time ON {TABLE}([time]);",
    f"CREATE INDEX IX_quakes_net_time ON {TABLE}([net],[time]);"
]
def reset_and_load_csv_from_blob(blob_name: str = "dataset.csv") -> dict:
    t0 = time.perf_counter()
    r = requests.get(get_blob_url(blob_name), timeout=60)
    if not r.ok:
        return {"ok": False, "msg": f"CSV download failed: HTTP {r.status_code}"}
    try:
        df = pd.read_csv(io.StringIO(r.text))
    except Exception as e:
        return {"ok": False, "msg": f"CSV parse error: {e}"}
    need = ["time", "latitude", "longitude", "depth", "mag", "net", "id"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        return {"ok": False, "msg": f"CSV missing columns: {missing}"}
    try:
        df = df.astype({
            "time": "int64",
            "latitude": "float64",
            "longitude": "float64",
            "depth": "float64",
            "mag": "float64",
            "net": "string",
            "id": "string"
        })
    except Exception:
        pass

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"IF OBJECT_ID(N'{TABLE}', N'U') IS NOT NULL DROP TABLE {TABLE};")
        cur.execute(DDL_CREATE)
        cur.fast_executemany = True
        rows = df[need].itertuples(index=False, name=None)
        batch = list(rows)
        if batch:
            cur.executemany(
                f"INSERT INTO {TABLE}([time],latitude,longitude,depth,mag,[net],[id]) VALUES (?,?,?,?,?,?,?)",
                batch
            )
        for sql in DDL_INDEXES:
            cur.execute(sql)
        conn.commit()
    elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
    return {"ok": True, "msg": "Reset & load complete", "rows_inserted": len(batch), "elapsed_ms": elapsed_ms}

@app.route("/load_dataset_reset", methods=["POST"])
def load_dataset_reset():
    blob_name = (request.json or {}).get("blob_name", "dataset.csv")
    result = reset_and_load_csv_from_blob(blob_name)
    try:
        redis_flush_query_cache()
    except Exception:
        pass
    return jsonify(result), (200 if result.get("ok") else 400)

def query_data_sqlite_blob(sql_query: str):
    blob_name = "data.db"
    blob_url = get_blob_url(blob_name)
    try:
        r = requests.get(blob_url, timeout=10)
        if not r.ok:
            return None, f"Failed to download DB blob. HTTP {r.status_code}"
        db_bytes = r.content
    except requests.RequestException as e:
        return None, f"Download error: {e}"
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            temp_db_path = tmpfile.name
            tmpfile.write(db_bytes)
        # Make sure file is closed before SQLite tries to use it
    except Exception as e:
        return None, f"Temp file write error: {e}"
    try:
        with sqlite3.connect(temp_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            results = cursor.fetchall()
    except sqlite3.Error as e:
        return None, f"SQLite error: {e}"
    finally:
        try:
            os.remove(temp_db_path)
        except Exception as e:
            print(f"Warning: Failed to delete temp file: {e}")
    return results, None

@app.route("/Qz3/query", methods=["POST"])
def run_query():
    sql_query = request.form.get("sql_query", "").strip()
    query_results = []
    column_names = []
    query_error = None
    if not sql_query:
        query_error = "Query is empty."
    else:
        results, error = query_data_sqlite_blob(sql_query)
        if error:
            query_error = error
        else:
            query_results = results
            try:
                blob_name = "data.db"
                blob_url = get_blob_url(blob_name)
                r = requests.get(blob_url, timeout=10)
                if r.ok:
                    db_bytes = r.content
                    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
                        tmpfile.write(db_bytes)
                        temp_db_path = tmpfile.name
                    conn = sqlite3.connect(temp_db_path)
                    cursor = conn.cursor()
                    cursor.execute(sql_query)
                    column_names = [desc[0] for desc in cursor.description]
                    cursor.close()
                    conn.close()
                    os.remove(temp_db_path)
            except Exception:
                column_names = []

    last_download_time = None
    if blob_exists("date.txt"):
        date_content = read_text_blob("date.txt")
        if date_content and not date_content.startswith("Failed"):
            last_download_time = date_content.strip()
    # had to repeat this

    return render_template(
        "Qz3.html",
        last_download_time=last_download_time,
        query_results=query_results,
        column_names=column_names,
        query_error=query_error,
        last_query=sql_query
    )

@app.route("/Qz3/prepared", methods=["POST"])
def run_prepared_query():
    qtype = request.form.get("query_type", "").strip()
    p1 = (request.form.get("param1") or "").strip()
    p2 = (request.form.get("param2") or "").strip()
    p3 = (request.form.get("param3") or "").strip()

    query_results = []
    column_names = []
    query_error = None
    timing_ms = None

    try:
        if qtype == "time_range":
            min_time = int(p1)
            max_time = int(p2)

            t0 = time.perf_counter()
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(f"""
                    SELECT [id], [net], [time], [latitude], [longitude]
                    FROM {TABLE}
                    WHERE [time] BETWEEN ? AND ?
                    ORDER BY [time], [id]
                """, (min_time, max_time))
                column_names = [d[0] for d in cur.description]
                query_results = [list(r) for r in cur.fetchall()]
            timing_ms = round((time.perf_counter() - t0) * 1000.0, 3)

        elif qtype == "start_net_count":
            start_time = int(p1)
            net = p2
            count_c = int(p3)

            t0 = time.perf_counter()
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(f"""
                    SELECT TOP (?)
                      [id], [net], [time], [latitude], [longitude]
                    FROM {TABLE}
                    WHERE [time] >= ? AND [net] = ?
                    ORDER BY [time], [id]
                """, (count_c, start_time, net))
                column_names = [d[0] for d in cur.description]
                query_results = [list(r) for r in cur.fetchall()]
            timing_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        else:
            query_error = "Invalid query type selected."
    except ValueError:
        query_error = "Invalid input: please ensure time/count values are integers."
    except Exception as e:
        query_error = f"SQL error: {e}"
    last_download_time = None
    if blob_exists("date.txt"):
        date_content = read_text_blob("date.txt")
        if date_content and not date_content.startswith("Failed"):
            last_download_time = date_content.strip()

    return render_template(
        "Qz3.html",
        last_download_time=last_download_time,
        query_results=query_results,
        column_names=column_names,
        query_error=query_error,
        last_query="",                     # not used for this form
        last_query_type=qtype,
        last_params={"param1": p1, "param2": p2, "param3": p3},
        timing_ms=timing_ms
    )

# --- add these imports near the top if not present ---
import os, time, json
import redis
from flask import request, jsonify

# ---------- Redis setup (env-driven) ----------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
REDIS_SSL = os.getenv("REDIS_SSL", "false").lower() == "true"
REDIS_TTL = int(os.getenv("REDIS_TTL_SECONDS", "120"))

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    ssl=REDIS_SSL,
    decode_responses=True,  # store JSON strings
)

HITS_KEY = "qcache:hits"
MISSES_KEY = "qcache:misses"

def _cache_key(query_name: str, payload: dict) -> str:
    return f"qcache:{query_name}:{json.dumps(payload, sort_keys=True)}"

def redis_get(query_name: str, payload: dict):
    key = _cache_key(query_name, payload)
    v = redis_client.get(key)
    if v is not None:
        redis_client.hincrby(HITS_KEY, query_name, 1)
        return json.loads(v)
    redis_client.hincrby(MISSES_KEY, query_name, 1)
    return None

def redis_set(query_name: str, payload: dict, value: dict):
    key = _cache_key(query_name, payload)
    redis_client.set(key, json.dumps(value), ex=REDIS_TTL)

def redis_flush_query_cache():
    for k in redis_client.scan_iter("qcache:*"):
        redis_client.delete(k)

def redis_cache_stats():
    hits = redis_client.hgetall(HITS_KEY) or {}
    misses = redis_client.hgetall(MISSES_KEY) or {}
    # count only entry keys (q10a/q10b payloads)
    entry_count = 0
    for _ in redis_client.scan_iter("qcache:q10*"):
        entry_count += 1
    return {
        "hits": {k:int(v) for k,v in hits.items()},
        "misses": {k:int(v) for k,v in misses.items()},
        "entry_count": entry_count,
        "ttl_seconds": REDIS_TTL
    }

# ---------- Query cores used by 10(a), 10(b), 11 ----------
def q10a_core(min_time: int, max_time: int):
    """
    Returns {"columns": [...], "rows": [[...], ...]} and elapsed ms (db time if miss, 0 if cached).
    """
    payload = {"min_time": min_time, "max_time": max_time}
    cached = redis_get("q10a", payload)
    if cached is not None:
        return cached, 0.0

    t0 = time.perf_counter()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT [id], [net], [time], [latitude], [longitude]
            FROM {TABLE}
            WHERE [time] BETWEEN ? AND ?
            ORDER BY [time], [id]
        """, (min_time, max_time))
        cols = [d[0] for d in cur.description]
        rows = [list(r) for r in cur.fetchall()]
        result = {"columns": cols, "rows": rows}
    dt_ms = (time.perf_counter() - t0) * 1000.0
    redis_set("q10a", payload, result)
    return result, dt_ms

def q10b_core(start_time: int, net: str, count_c: int):
    """
    Returns {"columns": [...], "rows": [[...], ...]} and elapsed ms (db time if miss, 0 if cached).
    """
    payload = {"start_time": start_time, "net": net, "count": count_c}
    cached = redis_get("q10b", payload)
    if cached is not None:
        return cached, 0.0

    t0 = time.perf_counter()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT TOP (?)
              [id], [net], [time], [latitude], [longitude]
            FROM {TABLE}
            WHERE [time] >= ? AND [net] = ?
            ORDER BY [time], [id]
        """, (count_c, start_time, net))
        cols = [d[0] for d in cur.description]
        rows = [list(r) for r in cur.fetchall()]
        result = {"columns": cols, "rows": rows}
    dt_ms = (time.perf_counter() - t0) * 1000.0
    redis_set("q10b", payload, result)
    return result, dt_ms

# ---------- Routes used by your HTML ----------

# 10(a) — time range (cached)
@app.route("/q10a", methods=["POST"])
def r10a():
    j = request.get_json(force=True, silent=True) or {}
    tmin = int(j.get("min_time"))
    tmax = int(j.get("max_time"))
    t0 = time.perf_counter()
    result, db_or_cache_ms = q10a_core(tmin, tmax)
    total_ms = (time.perf_counter() - t0) * 1000.0
    return jsonify({
        "result": result,
        "timing_ms": {
            "db_or_cache_ms": round(db_or_cache_ms, 3),
            "total_ms": round(total_ms, 3)
        }
    })

# 10(b) — start time + net + count C (cached)
@app.route("/q10b", methods=["POST"])
def r10b():
    j = request.get_json(force=True, silent=True) or {}
    start_t = int(j.get("start_time"))
    net = str(j.get("net"))
    C = int(j.get("count"))
    t0 = time.perf_counter()
    result, db_or_cache_ms = q10b_core(start_t, net, C)
    total_ms = (time.perf_counter() - t0) * 1000.0
    return jsonify({
        "result": result,
        "timing_ms": {
            "db_or_cache_ms": round(db_or_cache_ms, 3),
            "total_ms": round(total_ms, 3)
        }
    })

# 11 — repeat both queries T times, return per-iteration times and total (cached layer is active)
@app.route("/q11", methods=["POST"])
def r11():
    j = request.get_json(force=True, silent=True) or {}
    T = int(j.get("T", 1))
    a = j.get("q10a", {})
    b = j.get("q10b", {})
    a_times, b_times = [], []
    a_results, b_results = [], []
    total_t0 = time.perf_counter()

    for _ in range(T):
        t0 = time.perf_counter()
        res_a, _ = q10a_core(int(a["min_time"]), int(a["max_time"]))
        a_times.append(round((time.perf_counter() - t0) * 1000.0, 3))
        a_results.append(res_a)

        t0 = time.perf_counter()
        res_b, _ = q10b_core(int(b["start_time"]), str(b["net"]), int(b["count"]))
        b_times.append(round((time.perf_counter() - t0) * 1000.0, 3))
        b_results.append(res_b)

    total_ms = (time.perf_counter() - total_t0) * 1000.0
    return jsonify({
        "q10a_times_ms": a_times,
        "q10b_times_ms": b_times,
        "total_time_ms": round(total_ms, 3),
        "last_q10a_result": a_results[-1] if a_results else None,
        "last_q10b_result": b_results[-1] if b_results else None
    })

# 12 — update by time (invalidate cache after change)
@app.route("/q12_update", methods=["POST"])
def r12_update():
    """
    body: {"time": 20160, "updates": {"mag": 1.23, "net": "nc"}}
    """
    j = request.get_json(force=True, silent=True) or {}
    time_value = int(j.get("time"))
    updates = j.get("updates", {}) or {}

    # numeric coercion where appropriate
    for k in ("latitude", "longitude", "depth", "mag"):
        if k in updates and updates[k] is not None and updates[k] != "":
            updates[k] = float(updates[k])
    if "time" in updates and updates["time"] not in (None, ""):
        updates["time"] = int(updates["time"])

    # perform update
    changed = 0
    try:
        allowed = {"latitude","longitude","depth","mag","net","id","time"}
        sets, params = [], []
        for k, v in updates.items():
            if k in allowed:
                sets.append(f"[{k}] = ?")
                params.append(v)
        if sets:
            params.append(time_value)
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(f"UPDATE {TABLE} SET {', '.join(sets)} WHERE [time] = ?", params)
                changed = cur.rowcount
                conn.commit()
    except Exception as e:
        return jsonify({"updated_rows": 0, "error": str(e)}), 500

    # invalidate cache after mutation
    try:
        redis_flush_query_cache()
    except Exception:
        pass

    return jsonify({"updated_rows": changed})

# 13 — cache stats
@app.route("/q13_stats", methods=["GET"])
def r13_stats():
    return jsonify(redis_cache_stats())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
