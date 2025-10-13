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
    qtype = request.form.get("query_type")
    p1 = request.form.get("param1")
    p2 = request.form.get("param2")
    p3 = request.form.get("param3")
    p4 = request.form.get("param4")
    p5 = request.form.get("param5")

    sql = ""
    error = None

    try:
        if qtype == "mag_range":
            mlow = float(p1)
            mhigh = float(p2)
            sql = f"""
                SELECT time, lat, long, id, mag FROM data_tab
                WHERE mag BETWEEN {mlow} AND {mhigh}
            """

        elif qtype == "buffer_quakes":
            mlow = float(p1)
            mhigh = float(p2)
            lat = float(p3)
            lon = float(p4)
            n = float(p5)
            sql = f"""
                SELECT time, lat, long, id, mag FROM data_tab
                WHERE mag BETWEEN {mlow} AND {mhigh}
                  AND lat BETWEEN {lat - n} AND {lat + n}
                  AND long BETWEEN {lon - n} AND {lon + n}
            """
        else:
            error = "Invalid query type selected."

    except ValueError:
        error = "Invalid input: Please ensure all numeric fields are valid numbers."

    results = []
    colnames = []

    if not error:
        results, error = query_data_sqlite_blob(sql)

        # Fetch column names
        if results and not error:
            try:
                blob_url = get_blob_url("data.db")
                r = requests.get(blob_url, timeout=10)
                with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
                    tmpfile.write(r.content)
                    db_path = tmpfile.name
                conn = sqlite3.connect(db_path)
                c = conn.cursor()
                c.execute(sql)
                colnames = [desc[0] for desc in c.description]
                c.close()
                conn.close()
                os.remove(db_path)
            except Exception as e:
                error = f"Error fetching column names: {e}"

    last_download_time = None
    if blob_exists("date.txt"):
        date_content = read_text_blob("date.txt")
        if date_content and not date_content.startswith("Failed"):
            last_download_time = date_content.strip()

    return render_template("Qz3.html",
                           last_download_time=last_download_time,
                           query_results=results,
                           column_names=colnames,
                           query_error=error,
                           last_query="",
                           last_query_type=qtype,
                           last_params={
                               "param1": p1,
                               "param2": p2,
                               "param3": p3,
                               "param4": p4,
                               "param5": p5
                           })

def get_temp_db_connection():
    blob_url = get_blob_url("data.db")
    try:
        r = requests.get(blob_url, timeout=10)
        if not r.ok:
            return None, None, f"Failed to download DB blob. HTTP {r.status_code}"
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            tmpfile.write(r.content)
            temp_db_path = tmpfile.name
        conn = sqlite3.connect(temp_db_path)
        return conn, temp_db_path, None
    except Exception as e:
        return None, None, f"Error accessing DB: {e}"

@app.route("/Qz3/delete_by_net", methods=["POST"])
def delete_by_net():
    net_value = request.form.get("net_value", "").strip()
    if not net_value:
        return "Net value is required", 400
    conn, temp_db_path, error = get_temp_db_connection()
    if error:
        return error, 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM data_tab WHERE net = ?", (net_value,))
        count_to_delete = cur.fetchone()[0]
        cur.execute("DELETE FROM data_tab WHERE net = ?", (net_value,))
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM data_tab")
        remaining = cur.fetchone()[0]
        conn.close()
        with open(temp_db_path, "rb") as f:
            db_bytes = f.read()
        upload_url = get_blob_url("data.db")
        headers = {
            "x-ms-blob-type": "BlockBlob",
            "Content-Type": "application/octet-stream",
        }
        requests.put(upload_url, headers=headers, data=db_bytes, timeout=10)
    finally:
        os.remove(temp_db_path)
    return f"Deleted {count_to_delete} entries with net='{net_value}'. Remaining: {remaining}"

@app.route("/Qz3/insert_row", methods=["POST"])
def insert_row():
    try:
        data = {
            "time": int(request.form.get("time")),
            "lat": float(request.form.get("lat")),
            "long": float(request.form.get("long")),
            "mag": float(request.form.get("mag")),
            "nst": int(request.form.get("nst")),
            "net": request.form.get("net"),
            "id": request.form.get("id")
        }
    except (ValueError, TypeError):
        return "Invalid input. Please enter proper types.", 400
    conn, temp_db_path, error = get_temp_db_connection()
    if error:
        return error, 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM data_tab WHERE id = ?", (data["id"],))
        if cur.fetchone()[0] > 0:
            return f"Row with ID {data['id']} already exists.", 400
        cur.execute("""
            INSERT INTO data_tab (time, lat, long, mag, nst, net, id)
            VALUES (:time, :lat, :long, :mag, :nst, :net, :id)
        """, data)
        conn.commit()
        conn.close()
        with open(temp_db_path, "rb") as f:
            db_bytes = f.read()
        upload_url = get_blob_url("data.db")
        headers = {
            "x-ms-blob-type": "BlockBlob",
            "Content-Type": "application/octet-stream",
        }
        requests.put(upload_url, headers=headers, data=db_bytes, timeout=10)
    finally:
        os.remove(temp_db_path)
    return f"Row with ID {data['id']} inserted successfully."

@app.route("/Qz3/update_row", methods=["POST"])
def update_row():
    target_id = request.form.get("target_id", "").strip()
    target_time = request.form.get("target_time", "").strip()
    updates = {}
    for field in ["lat", "long", "mag", "nst", "net"]:
        val = request.form.get(field)
        if val:
            updates[field] = val
    if not updates:
        return "No fields provided to update.", 400
    where_clause = ""
    where_value = None
    if target_id:
        where_clause = "id = ?"
        where_value = target_id
    elif target_time:
        try:
            where_clause = "time = ?"
            where_value = int(target_time)
        except ValueError:
            return "Invalid time format.", 400
    else:
        return "Provide either ID or time to update.", 400
    set_clause = ", ".join([f"{k} = ?" for k in updates])
    params = list(updates.values()) + [where_value]
    conn, temp_db_path, error = get_temp_db_connection()
    if error:
        return error, 500
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE data_tab SET {set_clause} WHERE {where_clause}", params)
        updated = cur.rowcount
        conn.commit()
        conn.close()
        with open(temp_db_path, "rb") as f:
            db_bytes = f.read()
        upload_url = get_blob_url("data.db")
        headers = {
            "x-ms-blob-type": "BlockBlob",
            "Content-Type": "application/octet-stream",
        }
        requests.put(upload_url, headers=headers, data=db_bytes, timeout=10)
    finally:
        os.remove(temp_db_path)
    return f"Updated {updated} row(s)."


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
