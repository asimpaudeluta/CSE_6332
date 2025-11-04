import os
import json
import mimetypes
import csv
import io
import requests
import pandas as pd
import pyodbc
import redis
import time
from datetime import datetime
from flask import Flask, request, Response, render_template, redirect, jsonify, url_for, g, session
import re, operator
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY","dev-key")

TEXT_FILE_NAME = "_placeholder.log"
IMAGE_FILE_NAME = "mypic.jpg"
TABLE="dbo.quakes"
CONTAINER_URL = "https://cse6332.blob.core.windows.net/privatecontainer"
SAS_TOKEN = os.getenv("SAS_TOKEN")
AZURE_SQL_SERVER = os.getenv("AZURE_SQL_SERVER", "querytest-server.database.windows.net")
AZURE_SQL_DATABASE = os.getenv("AZURE_SQL_DATABASE", "querytest-database")
AZURE_SQL_USER = os.getenv("AZURE_SQL_USER", "querytest-server-admin@querytest-server")
AZURE_SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD", "sevpwA.P2#")
AZURE_SQL_ENCRYPT = os.getenv("AZURE_SQL_ENCRYPT", "yes")
AZURE_SQL_TRUST_CERT = os.getenv("AZURE_SQL_TRUST_CERT", "no")
AZURE_SQL_TIMEOUT = int(os.getenv("AZURE_SQL_TIMEOUT", "30"))
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
REDIS_SSL = os.getenv("REDIS_SSL", "false").lower() == "true"
REDIS_TTL = int(os.getenv("REDIS_TTL_SECONDS", "120"))
AZURE_SQL_LOGIN_TIMEOUT = int(os.getenv("AZURE_SQL_LOGIN_TIMEOUT", "60"))
AZURE_SQL_QUERY_TIMEOUT = int(os.getenv("AZURE_SQL_QUERY_TIMEOUT", "60"))

if not SAS_TOKEN:
    try:
        with open("secrets.json") as f:
            secrets = json.load(f)
        SAS_TOKEN = str(secrets["SAS_TOKEN"])
    except Exception:
        SAS_TOKEN = ""

DIRECTORY_DEFAULT = "Qz3"

def get_session_vals():
    return {"blob_dir": session.get("blob_dir", DIRECTORY_DEFAULT),
            "text_file_name": session.get("text_file_name", TEXT_FILE_NAME),
            "image_file_name": session.get("image_file_name", IMAGE_FILE_NAME),
            "csv_file_name": session.get("csv_file_name", TEXT_FILE_NAME),
            "dataset_path": session.get("dataset_path", DIRECTORY_DEFAULT)}

def set_blob_dir(d: str):
    session["blob_dir"] = d

def set_text_file_name(s: str):
    session["text_file_name"] = s

def set_image_file_name(s: str):
    session["image_file_name"] = s

def set_csv_file_name(s: str):
    session["csv_file_name"] = s

def set_dataset_path(s: str):
    session["dataset_path"] = s

def get_blob_url(blob_name: str) -> str:
    token = (SAS_TOKEN or "").lstrip("?")
    sep = "?" if "?" not in CONTAINER_URL else "&"
    d = get_session_vals()["blob_dir"]
    base = f"{CONTAINER_URL}/{d}/{blob_name}"
    return f"{base}{sep}{token}" if token else base

def blob_exists(filename: str) -> bool:
    url = get_blob_url(filename)
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except requests.RequestException:
        return False

def read_text_blob(filename: str = None) -> str:
    if filename is None:
        filename = get_session_vals()["text_file_name"]
    url = get_blob_url(filename)
    try:
        r = requests.get(url, timeout=10)
        if r.ok:
            return r.text
        return f"Failed to load text from blob. HTTP {r.status_code}"
    except requests.RequestException as e:
        return f"Failed to load text from blob. Error: {e}"

def write_text_blob(comment: str, filename: str = None) -> bool:
    if filename is None:
        filename = get_session_vals()["text_file_name"]
    if comment is None or comment.strip() == "":
        return False
    url = get_blob_url(filename)
    headers = {"x-ms-blob-type": "BlockBlob", "Content-Type": "text/plain; charset=utf-8"}
    try:
        r = requests.put(url, headers=headers, data=comment.encode("utf-8"), timeout=10)
        return r.status_code in (201, 202)
    except requests.RequestException:
        return False

@app.route("/get_text")
def get_text(filename:str =None):
    if filename is None:
        filename = get_session_vals()["text_file_name"]
    text = read_text_blob(filename)
    status = 200 if not text.startswith("Failed to load text") else 502
    return Response(text, mimetype="text/plain; charset=utf-8", status=status)

@app.route("/get_image")
def get_image(filename:str =None):
    if filename is None:
        filename = get_session_vals()["image_file_name"]
    url = get_blob_url(filename)
    try:
        r = requests.get(url, timeout=10)
        if not r.ok:
            return "Failed to load image from blob.", 502
        return Response(r.content, mimetype=mimetypes.guess_type(filename)[0])
    except requests.RequestException:
        return "Failed to load image from blob.", 502

@app.route("/upload_csv", methods=["POST"])
def upload_csv(filename:str =None):
    if filename is None:
        filename = get_session_vals()["csv_file_name"]
    file = request.files.get("file")
    if not file or not file.filename:
        return "No file provided", 400
    url = get_blob_url(filename)
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "text/csv"
    }
    r = requests.put(url, headers=headers, data=file.read(), timeout=30)
    if r.status_code in (201, 202):
        return redirect("/")
    return f"Failed to upload metadata. HTTP {r.status_code}", 500

def read_csv_rows(filename: str = None):
    if filename is None:
        filename = get_session_vals()["csv_file_name"]
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

#HOME#
@app.route("/", methods=["GET"])
def redirect_root():
    return render_template("home.html")
    # return redirect(url_for("Qz3"))

#CW1#
@app.route("/Cw1", methods=["GET"])
def cw1():
    set_blob_dir("CW1")
    set_text_file_name("milkyway.txt")
    set_image_file_name("milkyway.jpg")
    return render_template("CW1.html", content=read_text_blob())

@app.route("/upload_text", methods=["POST"])
def upload_text(filename:str =None):
    if filename is None:
        filename = get_session_vals()["text_file_name"]
    comment = request.form.get("comment", "")
    if write_text_blob(comment, filename=filename):
        return redirect("/Cw1")
        pass
    return "Failed to upload comment.", 400

#HW1#
OPS = {">": operator.gt, "<": operator.lt, ">=": operator.ge, "<=": operator.le, "==": operator.eq,
           "!=": operator.ne}
@app.route("/Hw1", methods=["GET"])
def hw1():
    set_blob_dir("HW1")
    set_text_file_name("_placeholder.log")
    set_csv_file_name("metadata.csv")
    content = read_text_blob()
    meta_exists = blob_exists(get_session_vals()["csv_file_name"])
    metadata_rows = None
    name_options, columns = [], []

    if meta_exists:
        metadata_rows, err = read_csv_rows(get_session_vals()["csv_file_name"])
        if err:
            meta_exists = False
            metadata_rows = None
        else:
            columns = metadata_rows[0] if metadata_rows else []
            idx = columns.index("Name") if (columns and "Name" in columns) else 0
            seen = set()
            for row in metadata_rows[1:]:
                if idx < len(row) and row[idx]:
                    n = row[idx]
                    if n not in seen:
                        seen.add(n)
                        name_options.append(n)

    img_msg  = request.args.get("img_msg")
    meta_msg = request.args.get("meta_msg")
    return render_template(
        "HW1.html",
        content=content,
        metadata_exists=meta_exists,
        metadata_rows=metadata_rows,
        name_options=name_options,
        columns=columns,
        img_msg=img_msg,
        meta_msg=meta_msg,
    )

@app.route("/upload_image", methods=["POST"])
def upload_image():
    name = (request.form.get("name") or "").strip()
    file = request.files.get("image")
    if not name:
        return redirect(url_for("index", img_msg="error: no name selected"))
    if not file or not file.filename:
        return redirect(url_for("index", img_msg="error: no file provided"))
    ctype = (mimetypes.guess_type(file.filename)[0] or file.mimetype or "").lower()
    if not ctype.startswith("image/"):
        return redirect(url_for("index", img_msg="error: file is not an image"))
    ext = os.path.splitext(file.filename)[1] or (mimetypes.guess_extension(ctype) or "")
    target = f"{name}{ext}"
    existed = blob_exists(target)
    url = get_blob_url(target)
    headers = {"x-ms-blob-type": "BlockBlob", "Content-Type": ctype}
    r = requests.put(url, headers=headers, data=file.read(), timeout=30)
    if r.status_code in (201, 202):
        msg = f"{'replaced' if existed else 'added'} {target}"
        return redirect(url_for("index", img_msg=msg))
    return redirect(url_for("index", img_msg=f"error: HTTP {r.status_code}"))

@app.route("/delete_image", methods=["POST"])
def delete_image():
    name = (request.form.get("name") or "").strip()
    if not name:
        return redirect(url_for("index", img_msg_del="error: no name selected"))
    filename = find_image_for_name(name)
    if not filename:
        return redirect(url_for("index", img_msg_del=f"not found for {name}"))
    url = get_blob_url(filename)
    try:
        r = requests.delete(url, timeout=15)
        if r.status_code in (202, 200, 204):
            return redirect(url_for("index", img_msg_del=f"deleted {filename}"))
        return redirect(url_for("index", img_msg_del=f"error: HTTP {r.status_code}"))
    except requests.RequestException as e:
        return redirect(url_for("index", img_msg_del=f"error: {e}"))

IMAGE_EXTS = [".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff"]
def find_image_for_name(name: str):
    for ext in IMAGE_EXTS:
        fn = f"{name}{ext}"
        if blob_exists(fn):
            return fn
    return None

@app.route("/update_metadata_image", methods=["POST"])
def update_metadata():
    if not blob_exists(get_session_vals()["csv_file_name"]):
        return redirect(url_for("index", img_msg=None, meta_msg="error: metadata.csv not found"))
    rows, err = read_csv_rows(get_session_vals()["csv_file_name"])
    if err or not rows:
        return redirect(url_for("index", meta_msg=f"error: {err or 'empty metadata'}"))
    header = rows[0]
    name_idx = header.index("Name") if ("Name" in header) else 0
    if "Picture" in header:
        pic_idx = header.index("Picture")
    else:
        header.append("Picture")
        pic_idx = len(header) - 1
        for r in rows[1:]:
            r.append(None)
    for r in rows[1:]:
        name = (r[name_idx] or "").strip()
        if not name:
            r[pic_idx] = None
            continue
        found = find_image_for_name(name)
        r[pic_idx] = found if found else None
    buf = io.StringIO(newline="")
    w = csv.writer(buf)
    for r in rows:
        w.writerow([c if c is not None else "" for c in r])
    data = buf.getvalue().encode("utf-8")
    url = get_blob_url(get_session_vals()["csv_file_name"])
    headers = {"x-ms-blob-type": "BlockBlob", "Content-Type": "text/csv"}
    r = requests.put(url, headers=headers, data=data, timeout=30)
    if r.status_code in (201, 202):
        return redirect(url_for("index", meta_msg="updated Picture column"))
    return redirect(url_for("index", meta_msg=f"error: HTTP {r.status_code}"))

@app.route("/metadata_json", methods=["GET"])
def metadata_json():
    rows, err = read_csv_rows(get_session_vals()["csv_file_name"])
    if err:
        return f"Failed to load metadata.csv: {err}", 502
    return jsonify(rows)

@app.route("/preview", methods=["GET"])
def preview_default():
    return Response("<div style='padding:8px;color:#ccc;'>Preview will appear here.</div>",
                    mimetype="text/html")

@app.route("/simple_query", methods=["POST"])
def simple_query():
    if not blob_exists(get_session_vals()["csv_file_name"]):
        return Response(render_preview("metadata.csv not found"), mimetype="text/html")
    col  = request.form.get("column")
    expr = (request.form.get("expr") or "").strip()
    val  = request.form.get("value") or ""
    rows, err = read_csv_rows(get_session_vals()["csv_file_name"])
    if err or not rows:
        return Response(render_preview("Failed to load metadata."), mimetype="text/html")
    header = rows[0]
    if col not in header:
        return Response(render_preview("Invalid column."), mimetype="text/html")
    col_idx  = header.index(col)
    name_idx = header.index("Name") if "Name" in header else 0
    pic_idx  = header.index("Picture") if "Picture" in header else None

    m = re.match(r"^\s*(>=|<=|==|!=|>|<)\s*(-?\d+(?:\.\d+)?)\s*$", expr) if expr else None

    def match(cell):
        s = "" if cell is None else str(cell).strip()
        if m:
            op, num = m.group(1), float(m.group(2))
            try:
                x = float(s)
            except ValueError:
                return False
            return OPS[op](x, num)
        if expr:
            return s.lower() == expr.lower()
        if val:
            return s == val
        return False

    filtered = [header] + [r for r in rows[1:] if col_idx < len(r) and match(r[col_idx])]
    if len(filtered) == 1:
        return Response(render_preview("<div style='padding:8px;'>No matches.</div>"), mimetype="text/html")
    table_html  = "<table style='width:100%;border-collapse:collapse;'>"
    table_html += "<thead><tr>" + "".join(f"<th style='border:1px solid #444;padding:4px;'>{h or ''}</th>" for h in header) + "</tr></thead><tbody>"
    for r in filtered[1:]:
        table_html += "<tr>" + "".join(f"<td style='border:1px solid #333;padding:4px;'>{(c or '')}</td>" for c in r) + "</tr>"
    table_html += "</tbody></table>"
    images = []
    seen_files = set()
    for r in filtered[1:]:
        pic = None
        if pic_idx is not None and pic_idx < len(r) and r[pic_idx]:
            candidate = r[pic_idx]
            if blob_exists(candidate):
                pic = candidate
        if not pic:
            nm = (r[name_idx] or "").strip()
            if nm:
                candidate = find_image_for_name(nm)
                if candidate:
                    pic = candidate
        if pic and pic not in seen_files:
            seen_files.add(pic)
            images.append(((r[name_idx] or pic), pic))
    imgs_html = ""
    if images:
        imgs_html += "<div style='margin-top:12px'><h4>Pictures</h4>"
        imgs_html += "<div style='display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:8px;'>"
        for label, fname in images:
            src = get_blob_url(fname)
            cap = label or fname
            imgs_html += (
                "<figure style='margin:0;padding:0;text-align:center;background:#000'>"
                f"<img src='{src}' alt='{cap}' style='max-width:100%;height:140px;object-fit:contain;display:block;'>"
                f"<figcaption style='font-size:12px;padding:4px 0'>{cap}</figcaption>"
                "</figure>"
            )
        imgs_html += "</div></div>"
    return Response(render_preview(table_html + imgs_html), mimetype="text/html")


# this part is to retain preview when updating/ or may be used in other stuff
def render_preview(inner_html: str, reload_parent: bool = False) -> str:
    style = (
        "body,table,th,td,div,span,p,li,code,pre,a{color:#fff;}"
        "table{width:100%;border-collapse:collapse}"
        "th,td{border:1px solid #444;padding:4px}"
        "body{background:#111;font-family:Arial,sans-serif;padding:8px}"
    )
    base = f"<!doctype html><html><head><meta charset='utf-8'><style>{style}</style></head><body>"
    html = f"{base}<div id='payload'>{inner_html}</div>"
    if reload_parent:
        html += """
<script>
try{
  var payload = document.getElementById('payload').innerHTML;
  var clean = "<!doctype html><html><head><meta charset='utf-8'><style>""" + \
              style + \
              """</style></head><body>" + payload + "</body></html>";
  window.parent.sessionStorage.setItem("previewHTML", clean);
}catch(e){}
if (window.parent && window.parent !== window) {
  window.parent.location.reload();
}
</script>
"""
    html += "</body></html>"
    return html

@app.route("/update_cell", methods=["POST"])
def update_cell():
    if not blob_exists(get_session_vals()["csv_file_name"]):
        return Response("metadata.csv not found", mimetype="text/html")
    column   = request.form.get("column")
    row_key  = (request.form.get("row_key") or "").strip()
    new_val  = request.form.get("new_value", "")
    rows, err = read_csv_rows(get_session_vals()["csv_file_name"])
    if err or not rows: return Response("Failed to load metadata.", mimetype="text/html")
    header = rows[0]
    if column not in header: return Response("Invalid column.", mimetype="text/html")
    key_idx = header.index("Name") if "Name" in header else 0
    col_idx = header.index(column)
    updated_row = None
    for r in rows[1:]:
        key = (r[key_idx] or "").strip()
        if key == row_key:
            while len(r) <= col_idx: r.append(None)
            r[col_idx] = (new_val if new_val != "" else None)
            updated_row = list(r)
            break
    if not updated_row:
        return Response("Row not found.", mimetype="text/html")
    buf = io.StringIO(newline="")
    w = csv.writer(buf)
    for r in rows:
        w.writerow([c if c is not None else "" for c in r])
    data = buf.getvalue().encode("utf-8")
    url = get_blob_url(get_session_vals()["csv_file_name"])
    hdrs = {"x-ms-blob-type": "BlockBlob", "Content-Type": "text/csv"}
    rr = requests.put(url, headers=hdrs, data=data, timeout=30)
    if rr.status_code not in (201, 202):
        return Response(f"error: HTTP {rr.status_code}", mimetype="text/html")
    html = "<div style='padding:8px;'>Updated row:</div>"
    html += "<table style='width:100%;border-collapse:collapse;'>"
    html += "<thead><tr>" + "".join(f"<th style='border:1px solid #444;padding:4px;'>{h or ''}</th>" for h in header) + "</tr></thead>"
    html += "<tbody><tr>" + "".join(f"<td style='border:1px solid #333;padding:4px;'>{(c or '')}</td>" for c in updated_row) + "</tr></tbody></table>"
    return Response(render_preview(html, reload_parent=True), mimetype="text/html")

@app.route("/delete_row", methods=["POST"])
def delete_row():
    if not blob_exists(get_session_vals()["csv_file_name"]):
        return Response(render_preview("metadata.csv not found"), mimetype="text/html")
    row_key = (request.form.get("row_key") or "").strip()
    if not row_key:
        return Response(render_preview("no row key provided"), mimetype="text/html")
    rows, err = read_csv_rows(get_session_vals()["csv_file_name"])
    if err or not rows:
        return Response(render_preview("Failed to load metadata."), mimetype="text/html")
    header = rows[0]
    key_idx = header.index("Name") if "Name" in header else 0
    deleted_row = None
    kept = [header]
    for r in rows[1:]:
        key = (r[key_idx] or "").strip()
        if deleted_row is None and key == row_key:
            deleted_row = list(r)
            continue
        kept.append(r)
    if not deleted_row:
        return Response(render_preview("Row not found."), mimetype="text/html")
    buf = io.StringIO(newline="")
    w = csv.writer(buf)
    for r in kept:
        w.writerow([c if c is not None else "" for c in r])
    data = buf.getvalue().encode("utf-8")
    url = get_blob_url(get_session_vals()["csv_file_name"])
    hdrs = {"x-ms-blob-type": "BlockBlob", "Content-Type": "text/csv"}
    rr = requests.put(url, headers=hdrs, data=data, timeout=30)
    if rr.status_code not in (201, 202):
        return Response(render_preview(f"error: HTTP {rr.status_code}"), mimetype="text/html")
    html = "<div style='padding:8px;'>Deleted row:</div>"
    html += "<table style='width:100%;border-collapse:collapse;'>"
    html += "<thead><tr>" + "".join(f"<th style='border:1px solid #444;padding:4px;'>{h or ''}</th>" for h in header) + "</tr></thead>"
    html += "<tbody><tr>" + "".join(f"<td style='border:1px solid #333;padding:4px;'>{(c or '')}</td>" for c in deleted_row) + "</tr></tbody></table>"
    return Response(render_preview(html, reload_parent=True), mimetype="text/html")

@app.route("/add_row", methods=["POST"])
def add_row():
    if not blob_exists(get_session_vals()["csv_file_name"]):
        return Response(render_preview("metadata.csv not found"), mimetype="text/html")
    name = (request.form.get("name") or "").strip()
    if not name:
        return Response(render_preview("no name provided"), mimetype="text/html")
    rows, err = read_csv_rows(get_session_vals()["csv_file_name"])
    if err or not rows:
        return Response(render_preview("Failed to load metadata."), mimetype="text/html")
    header = rows[0]
    name_idx = header.index("Name") if "Name" in header else 0
    for r in rows[1:]:
        key = (r[name_idx] or "").strip()
        if key == name:
            return Response(render_preview(f"error: '{name}' already exists"), mimetype="text/html")
    new_row = [None] * max(len(header), name_idx + 1)
    new_row[name_idx] = name
    rows.append(new_row)
    buf = io.StringIO(newline="")
    w = csv.writer(buf)
    for r in rows:
        w.writerow([c if c is not None else "" for c in r])
    data = buf.getvalue().encode("utf-8")
    url = get_blob_url(get_session_vals()["csv_file_name"])
    hdrs = {"x-ms-blob-type": "BlockBlob", "Content-Type": "text/csv"}
    rr = requests.put(url, headers=hdrs, data=data, timeout=30)
    if rr.status_code not in (201, 202):
        return Response(render_preview(f"error: HTTP {rr.status_code}"), mimetype="text/html")
    html = "<div style='padding:8px;'>Added row:</div>"
    html += "<table style='width:100%;border-collapse:collapse;'>"
    html += "<thead><tr>" + "".join(
        f"<th style='border:1px solid #444;padding:4px;'>{h or ''}</th>" for h in header
    ) + "</tr></thead>"
    html += "<tbody><tr>" + "".join(
        f"<td style='border:1px solid #333;padding:4px;'>{(c or '')}</td>" for c in new_row
    ) + "</tr></tbody></table>"
    return Response(render_preview(html, reload_parent=True), mimetype="text/html")

#Qz3
@app.route("/debug/odbc")
def debug_odbc():
    # winget source update
    # winget search msodbcsql
    # winget install --id=Microsoft.msodbcsql.18 -e --source winget
    # had to do this in windows
    return jsonify({"drivers": pyodbc.drivers()})

def get_conn():
    def pick_odbc_driver():
        preferred = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server"
        ]
        available = set(pyodbc.drivers())
        for name in preferred:
            if name in available:
                return name
        raise RuntimeError(f"No modern SQL Server ODBC driver found. Installed: {sorted(available)}")
    # driver = pick_odbc_driver() # not required, allow your local ip: 64.189.4.178/52.142.30.27
    ODBC_CONN_STR = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=tcp:{AZURE_SQL_SERVER},1433;"
        f"DATABASE={AZURE_SQL_DATABASE};"
        f"UID={AZURE_SQL_USER};PWD={AZURE_SQL_PASSWORD};"
        f"Encrypt={AZURE_SQL_ENCRYPT};TrustServerCertificate={AZURE_SQL_TRUST_CERT};"
        f"Connection Timeout={AZURE_SQL_LOGIN_TIMEOUT};"
        "MARS_Connection=Yes;"
        "ApplicationIntent=ReadOnly;"
    )
    return pyodbc.connect(ODBC_CONN_STR, autocommit=False)

def select_with_retry(conn, sql, params=(), query_timeout=AZURE_SQL_QUERY_TIMEOUT, retries=1):
    attempt = 0
    while True:
        try:
            cur = conn.cursor()
            cur.timeout = query_timeout
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = [list(r) for r in cur.fetchall()]
            return {"columns": cols, "rows": rows}
        except pyodbc.Error as e:
            code = getattr(e, "args", [None])[0]
            if attempt < retries and any(s in str(e) for s in ("HYT00", "HYT01", "timeout")):
                attempt += 1
                time.sleep(1.0)
                continue
            raise

def csv_to_db_reset(blob_name: str = "dataset.csv") -> dict:
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
        df = df.astype({"time": "int64", "latitude": "float64", "longitude": "float64", "depth": "float64", "mag": "float64", "net": "string", "id": "string"})
    except Exception:
        pass
    DDL_CREATE = f"""
    CREATE TABLE {TABLE}(
        [time] INT NOT NULL,
        [latitude] FLOAT NULL,
        [longitude] FLOAT NULL,
        [depth] FLOAT NULL,
        [mag] FLOAT NULL,
        [net] NVARCHAR(16) NULL,
        [id] NVARCHAR(64) NOT NULL PRIMARY KEY
    );
    """
    DDL_INDEXES = [
        f"CREATE INDEX IX_quakes_time ON {TABLE}([time]);",
        f"CREATE INDEX IX_quakes_net_time ON {TABLE}([net],[time]);"
    ]
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"IF OBJECT_ID(N'{TABLE}', N'U') IS NOT NULL DROP TABLE {TABLE};")
        cur.execute(DDL_CREATE)
        cur.fast_executemany = True
        rows = df[need].itertuples(index=False, name=None)
        batch = list(rows)
        if batch:
            cur.executemany(f"INSERT INTO {TABLE}([time],latitude,longitude,depth,mag,[net],[id]) VALUES (?,?,?,?,?,?,?)", batch)
        for sql in DDL_INDEXES:
            cur.execute(sql)
        conn.commit()
    elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
    return {"ok": True, "msg": "Reset & load complete", "rows_inserted": len(batch), "elapsed_ms": elapsed_ms}

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, ssl=REDIS_SSL, decode_responses=True)
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
    entry_count = 0
    for _ in redis_client.scan_iter("qcache:q10*"):
        entry_count += 1
    return {"hits": {k: int(v) for k, v in hits.items()}, "misses": {k: int(v) for k, v in misses.items()}, "entry_count": entry_count, "ttl_seconds": REDIS_TTL}

@app.route("/Qz3", methods=["GET"])
def qz3():
    last_download_time = None
    if blob_exists("date.txt"):
        date_content = read_text_blob("date.txt")
        if date_content and not date_content.startswith("Failed"):
            last_download_time = date_content.strip()
    return render_template("Qz3.html", last_download_time=last_download_time)

@app.route("/load_dataset_reset", methods=["POST"])
def load_dataset_reset():
    blob_name = (request.json or {}).get("blob_name", "dataset.csv")
    result = csv_to_db_reset(blob_name)
    try:
        redis_flush_query_cache()
    except Exception:
        pass
    return jsonify(result), (200 if result.get("ok") else 400)

# only using a single connection
def q10a_core(min_time: int, max_time: int, conn: pyodbc.Connection | None = None):
    payload = {"min_time": min_time, "max_time": max_time}
    cached = redis_get("q10a", payload)
    if cached is not None:
        return cached, 0.0
    owns = False
    if conn is None:
        conn = get_conn()
        owns = True
    t0 = time.perf_counter()
    try:
        result = select_with_retry(
            conn,
            f"""
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            SELECT [id],[net],[time],[latitude],[longitude]
            FROM {TABLE}
            WHERE [time] BETWEEN ? AND ?
            ORDER BY [time],[id]
            """,
            (min_time, max_time),
        )
    finally:
        if owns:
            conn.close()
    dt_ms = (time.perf_counter() - t0) * 1000.0
    redis_set("q10a", payload, result)
    return result, dt_ms

def q10b_core(start_time: int, net: str, count_c: int, conn: pyodbc.Connection | None = None):
    payload = {"start_time": start_time, "net": net, "count": count_c}
    cached = redis_get("q10b", payload)
    if cached is not None:
        return cached, 0.0
    owns = False
    if conn is None:
        conn = get_conn()
        owns = True
    t0 = time.perf_counter()
    try:
        result = select_with_retry(
            conn,
            f"""
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            SELECT TOP (?)
              [id],[net],[time],[latitude],[longitude]
            FROM {TABLE}
            WHERE [time] >= ? AND [net] = ?
            ORDER BY [time],[id]
            """,
            (count_c, start_time, net),
        )
    finally:
        if owns:
            conn.close()
    dt_ms = (time.perf_counter() - t0) * 1000.0
    redis_set("q10b", payload, result)
    return result, dt_ms

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
    return render_template("Qz3.html", last_download_time=last_download_time, query_results=query_results, column_names=column_names, query_error=query_error, last_query="", last_query_type=qtype, last_params={"param1": p1, "param2": p2, "param3": p3}, timing_ms=timing_ms)

@app.route("/Qz3/q10a", methods=["POST"])
def r10a():
    j = request.get_json(force=True, silent=True) or {}
    tmin = int(j.get("min_time"))
    tmax = int(j.get("max_time"))
    t0 = time.perf_counter()
    result, db_or_cache_ms = q10a_core(tmin, tmax)
    total_ms = (time.perf_counter() - t0) * 1000.0
    return jsonify({"result": result, "timing_ms": {"db_or_cache_ms": round(db_or_cache_ms, 3), "total_ms": round(total_ms, 3)}})

@app.route("/Qz3/q10b", methods=["POST"])
def r10b():
    j = request.get_json(force=True, silent=True) or {}
    start_t = int(j.get("start_time"))
    net = str(j.get("net"))
    C = int(j.get("count"))
    t0 = time.perf_counter()
    result, db_or_cache_ms = q10b_core(start_t, net, C)
    total_ms = (time.perf_counter() - t0) * 1000.0
    return jsonify({"result": result, "timing_ms": {"db_or_cache_ms": round(db_or_cache_ms, 3), "total_ms": round(total_ms, 3)}})

@app.route("/Qz3/q11", methods=["POST"])
def r11():
    try:
        j = request.get_json(force=True, silent=True) or {}
        if not isinstance(j, dict):
            return jsonify({"error": "invalid JSON body"}), 400
        if "T" not in j or "q10a" not in j or "q10b" not in j:
            return jsonify({"error": "missing T, q10a, or q10b"}), 400

        try:
            T = int(j.get("T", 1))
            a = j.get("q10a") or {}
            b = j.get("q10b") or {}
            a_min = int(a.get("min_time"))
            a_max = int(a.get("max_time"))
            b_start = int(b.get("start_time"))
            b_net = (b.get("net") or "").strip()
            b_count = int(b.get("count"))
        except (TypeError, ValueError):
            return jsonify({"error": "T, min_time, max_time, start_time, count must be numbers; net must be non-empty string"}), 400
        if not b_net:
            return jsonify({"error": "net is required"}), 400
        if T < 1 or b_count < 1:
            return jsonify({"error": "T and count must be >= 1"}), 400
        if T > 25:
            return jsonify({"error": "T too large for demo (max 25)"}), 400

        a_times, b_times, a_results, b_results = [], [], [], []
        total_t0 = time.perf_counter()
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            for i in range(T):
                # 10(a)
                t0 = time.perf_counter()
                res_a, _ = q10a_core(a_min, a_max, conn=conn)
                a_times.append(round((time.perf_counter() - t0) * 1000.0, 3))
                a_results.append(res_a)

                # 10(b)
                t0 = time.perf_counter()
                res_b, _ = q10b_core(b_start, b_net, b_count, conn=conn)
                b_times.append(round((time.perf_counter() - t0) * 1000.0, 3))
                b_results.append(res_b)
        except pyodbc.Error as e:
            return jsonify({"error": f"ODBC error during /q11 loop: {e}"}), 500
        finally:
            try:
                conn.close()
            except Exception:
                pass
        total_ms = (time.perf_counter() - total_t0) * 1000.0
        return jsonify({
            "q10a_times_ms": a_times,
            "q10b_times_ms": b_times,
            "total_time_ms": round(total_ms, 3),
            "last_q10a_result": a_results[-1] if a_results else None,
            "last_q10b_result": b_results[-1] if b_results else None
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/Qz3/q12_update", methods=["POST"])
def r12_update():
    try:
        j = request.get_json(force=True, silent=True) or {}
        if "time" not in j:
            return jsonify({"updated_rows": 0, "error": "missing 'time'"}), 400
        try:
            time_value = int(j.get("time"))
        except (TypeError, ValueError):
            return jsonify({"updated_rows": 0, "error": "time must be integer"}), 400

        updates = j.get("updates", {}) or {}
        if not isinstance(updates, dict) or not updates:
            return jsonify({"updated_rows": 0, "error": "no updates provided"}), 400

        for k in ("latitude", "longitude", "depth", "mag"):
            if k in updates and updates[k] not in (None, ""):
                try:
                    updates[k] = float(updates[k])
                except ValueError:
                    return jsonify({"updated_rows": 0, "error": f"{k} must be numeric"}), 400
        if "time" in updates and updates["time"] not in (None, ""):
            try:
                updates["time"] = int(updates["time"])
            except ValueError:
                return jsonify({"updated_rows": 0, "error": "new time must be integer"}), 400

        allowed = {"latitude","longitude","depth","mag","net","id","time"}
        sets, params = [], []
        for k, v in updates.items():
            if k in allowed:
                sets.append(f"[{k}] = ?")
                params.append(v)
        if not sets:
            return jsonify({"updated_rows": 0, "error": "no valid fields to update"}), 400

        params.append(time_value)
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"UPDATE {TABLE} SET {', '.join(sets)} WHERE [time] = ?", params)
            changed = cur.rowcount
            conn.commit()

        try:
            redis_flush_query_cache()
        except Exception:
            pass

        if changed == 0:
            return jsonify({"updated_rows": 0, "warning": f"no row with time={time_value}"}), 404
        return jsonify({"updated_rows": changed})
    except Exception as e:
        return jsonify({"updated_rows": 0, "error": str(e)}), 500


@app.route("/Qz3/q13_stats", methods=["GET"])
def r13_stats():
    try:
        return jsonify(redis_cache_stats())
    except Exception as e:
        return jsonify({"error": str(e), "hits": {}, "misses": {}, "entry_count": 0, "ttl_seconds": os.getenv("REDIS_TTL_SECONDS", "120")}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
