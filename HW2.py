import os, json, mimetypes, csv, io, requests, sqlite3, tempfile
from flask import Flask, request, Response, render_template, redirect, jsonify, url_for
import re, operator
from datetime import datetime
app = Flask(__name__)

TEXT_FILE_NAME = "_placeholder.log"
IMAGE_FILE_NAME = "milkyway.jpg"
CONTAINER_URL = "https://cse6332.blob.core.windows.net/privatecontainer"
DIRECTORY =  "HW2"
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
    return redirect(url_for("hw2"))

@app.route("/HW2", methods=["GET"])
def hw2():
    last_download_time = None
    if blob_exists("date.txt"):
        date_content = read_text_blob("date.txt")
        if date_content and not date_content.startswith("Failed"):
            last_download_time = date_content.strip()
    return render_template(
        "HW2.html",
        last_download_time= last_download_time
    )

def get_url_csv_to_blob(url: str = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv",force: bool = False) -> bool:
    csv_blob_name = "data.csv"
    db_blob_name = "data.db"
    if not force and blob_exists(csv_blob_name) and blob_exists(db_blob_name):
        return True
    try:
        response = requests.get(
            url=url,
            timeout=15
        )
        if not response.ok:
            print(f"Failed to download CSV: HTTP {response.status_code}")
            return False
        csv_data = response.content
    except requests.RequestException as e:
        print(f"Download error: {e}")
        return False
    csv_url = get_blob_url(csv_blob_name)
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "text/csv; charset=utf-8",
    }
    try:
        r = requests.put(csv_url, headers=headers, data=csv_data, timeout=10)
        if r.status_code not in (201, 202):
            print(f"CSV upload failed: HTTP {r.status_code}")
            return False
    except requests.RequestException as e:
        print(f"CSV upload error: {e}")
        return False

    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE Earthquakes (
            time TEXT,
            latitude REAL,
            longitude REAL,
            depth REAL,
            mag REAL,
            magType TEXT,
            nst INTEGER,
            gap REAL,
            dmin REAL,
            rms REAL,
            net TEXT,
            id TEXT PRIMARY KEY,
            updated TEXT,
            place TEXT,
            type TEXT,
            horizontalError REAL,
            depthError REAL,
            magError REAL,
            magNst INTEGER,
            status TEXT,
            locationSource TEXT,
            magSource TEXT
        )
    """)
    csv_text = csv_data.decode("utf-8")
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        values = [row.get(col) for col in reader.fieldnames]
        placeholders = ",".join("?" * len(values))
        cursor.execute(f"INSERT OR IGNORE INTO Earthquakes VALUES ({placeholders})", values)
    conn.commit()
    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        temp_db_path = tmpfile.name
    try:
        disk_conn = sqlite3.connect(temp_db_path)
        conn.backup(disk_conn) #bckp in tempfile->memory just incase
        disk_conn.close()
        with open(temp_db_path, "rb") as f:
            db_bytes = f.read()
    finally:
        os.remove(temp_db_path)
        conn.close()

    db_url = get_blob_url(db_blob_name)
    db_headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "application/octet-stream",
    }
    try:
        r = requests.put(db_url, headers=db_headers, data=db_bytes, timeout=10)
        if r.status_code not in (201, 202):
            print(f"DB upload failed: HTTP {r.status_code}")
            return False
    except requests.RequestException as e:
        print(f"DB upload error: {e}")
        return False

    date_str = datetime.now().strftime("%m-%d-%Y")
    date_blob_url = get_blob_url("date.txt")
    date_headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "text/plain; charset=utf-8",
    }
    try:
        r = requests.put(date_blob_url, headers=date_headers, data=date_str.encode("utf-8"), timeout=10)
        if r.status_code in (201, 202):
            return True
        else:
            print(f"Date upload failed: HTTP {r.status_code}")
            return False
    except requests.RequestException as e:
        print(f"Date upload error: {e}")
        return False

@app.route("/HW2/download", methods=["POST"])
def download_data():
    success = get_url_csv_to_blob(force=True)
    return redirect(url_for("hw2"))

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
    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        temp_db_path = tmpfile.name
        tmpfile.write(db_bytes)
    try:
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
    except sqlite3.Error as e:
        return None, f"SQLite error: {e}"
    finally:
        os.remove(temp_db_path)
    return results, None

@app.route("/HW2/query", methods=["POST"])
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
        "HW2.html",
        last_download_time=last_download_time,
        query_results=query_results,
        column_names=column_names,
        query_error=query_error,
        last_query=sql_query
    )

@app.route("/HW2/prepared", methods=["POST"])
def run_prepared_query():
    qtype = request.form.get("query_type")
    p1 = request.form.get("param1")
    p2 = request.form.get("param2")
    p3 = request.form.get("param3")
    p4 = request.form.get("param4")
    p5 = request.form.get("param5")

    if qtype == "largest_n":
        sql = f"""
            SELECT place, mag, time FROM Earthquakes
            WHERE mag IS NOT NULL
            ORDER BY mag DESC
            LIMIT {p1 or 5}
        """
        # account for blank values
    elif qtype == "buffer_quakes":
        lat, lon, km = float(p1), float(p2), float(p3)
        deg = km / 111
        sql = f"""
            SELECT place, mag, time FROM Earthquakes
            WHERE latitude BETWEEN {lat - deg} AND {lat + deg}
              AND longitude BETWEEN {lon - deg} AND {lon + deg}
        """
    elif qtype == "date_range":
        sql = f"""
            SELECT place, mag, time FROM Earthquakes
            WHERE time BETWEEN '{p1}' AND '{p2}'
            AND mag > '{p3}'
        """
    elif qtype == "count_by_mag":
        sql = """
            SELECT
                COUNT(CASE WHEN mag >= 1 AND mag < 2 THEN 1 END) AS "1-2",
                COUNT(CASE WHEN mag >= 2 AND mag < 3 THEN 1 END) AS "2-3",
                COUNT(CASE WHEN mag >= 3 AND mag < 4 THEN 1 END) AS "3-4",
                COUNT(CASE WHEN mag >= 4 AND mag < 5 THEN 1 END) AS "4-5",
                COUNT(CASE WHEN mag >= 5 AND mag < 6 THEN 1 END) AS "5-6",
                COUNT(CASE WHEN mag >= 6 AND mag < 7 THEN 1 END) AS "6-7",
                COUNT(CASE WHEN mag >= 7 THEN 1 END) AS "7+"
            FROM Earthquakes
            WHERE time >= datetime('now', '-3 days')
        """
    elif qtype == "compare_regions":
        x1, y1, b1 = float(p1), float(p2), float(p3)
        x2, y2, b2 = float(p4), float(p5), float(p3)
        d1 = b1 / 111
        d2 = b2 / 111
        # approx (within bounding box of buffer not eucledian distn)
        sql = f"""
            SELECT 'Region A' AS region, COUNT(*) FROM Earthquakes
            WHERE latitude BETWEEN {x1 - d1} AND {x1 + d1}
              AND longitude BETWEEN {y1 - d1} AND {y1 + d1}
            UNION
            SELECT 'Region B', COUNT(*) FROM Earthquakes
            WHERE latitude BETWEEN {x2 - d2} AND {x2 + d2}
              AND longitude BETWEEN {y2 - d2} AND {y2 + d2}
        """
    elif qtype == "largest_near":
        x, y, km = float(p1), float(p2), float(p3)
        deg = km / 111
        sql = f"""
            SELECT place, mag, time FROM Earthquakes
            WHERE latitude BETWEEN {x - deg} AND {x + deg}
              AND longitude BETWEEN {y - deg} AND {y + deg}
            ORDER BY mag DESC LIMIT 1
        """
    else:
        sql = "SELECT 'Invalid query type'"

    results, error = query_data_sqlite_blob(sql)

    # Get column names if query successful
    colnames = []
    if results and not error:
        blob_url = get_blob_url("data.db")
        r = requests.get(blob_url, timeout=10)
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            tmpfile.write(r.content)
            db_path = tmpfile.name
        try:
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute(sql)
            colnames = [desc[0] for desc in c.description]
            c.close()
            conn.close()
        except Exception as e:
            error = f"Error extracting column names: {e}"
        finally:
            try:
                os.remove(db_path)
            except Exception as e:
                print(f"Temp file removal error: {e}")

    # Load last download time
    last_download_time = None
    if blob_exists("date.txt"):
        date_content = read_text_blob("date.txt")
        if date_content and not date_content.startswith("Failed"):
            last_download_time = date_content.strip()

    return render_template("HW2.html",
                           last_download_time=last_download_time,
                           query_results=results,
                           column_names=colnames,
                           query_error=error,
                           last_query="",
                           last_query_type=qtype,
                           last_params={"param1": p1, "param2": p2, "param3": p3, "param4": p4, "param5": p5}
                           )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
