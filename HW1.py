import os, json, mimetypes, csv, io, requests
from flask import Flask, request, Response, render_template, redirect, jsonify, url_for

app = Flask(__name__)

TEXT_FILE_NAME = "_placeholder.log"
IMAGE_FILE_NAME = "milkyway.jpg"
CONTAINER_URL = "https://cse6332.blob.core.windows.net/privatecontainer"
DIRECTORY =  "HW1"
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
def read_csv_rows(filename: str = "metadata.csv"):
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
def index():
    content = read_text_blob(TEXT_FILE_NAME)
    meta_exists = blob_exists("metadata.csv")
    metadata_rows = None
    name_options, columns = [], []

    if meta_exists:
        metadata_rows, err = read_csv_rows("metadata.csv")
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


@app.route("/upload_text", methods=["POST"])
def upload_text():
    comment = request.form.get("comment", "")
    if write_text_blob(comment, filename=TEXT_FILE_NAME):
        return redirect("/")
    return "Failed to upload comment.", 400

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

@app.route("/get_text")
def get_text():
    text = read_text_blob(TEXT_FILE_NAME)
    status = 200 if not text.startswith("Failed to load text") else 502
    return Response(text, mimetype="text/plain; charset=utf-8", status=status)

@app.route("/upload_csv", methods=["POST"])
def upload_csv():
    file = request.files.get("file")
    if not file or not file.filename:
        return "No file provided", 400
    url = get_blob_url("metadata.csv") #file.filename
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "text/csv"
    }
    r = requests.put(url, headers=headers, data=file.read(), timeout=30)
    if r.status_code in (201, 202):
        return redirect("/")
    return f"Failed to upload metadata. HTTP {r.status_code}", 500

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
    if not blob_exists("metadata.csv"):
        return redirect(url_for("index", img_msg=None, meta_msg="error: metadata.csv not found"))
    rows, err = read_csv_rows("metadata.csv")
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
    url = get_blob_url("metadata.csv")
    headers = {"x-ms-blob-type": "BlockBlob", "Content-Type": "text/csv"}
    r = requests.put(url, headers=headers, data=data, timeout=30)
    if r.status_code in (201, 202):
        return redirect(url_for("index", meta_msg="updated Picture column"))
    return redirect(url_for("index", meta_msg=f"error: HTTP {r.status_code}"))

@app.route("/metadata_json", methods=["GET"])
def metadata_json():
    rows, err = read_csv_rows("metadata.csv")
    if err:
        return f"Failed to load metadata.csv: {err}", 502
    return jsonify(rows)

@app.route("/preview", methods=["GET"])
def preview_default():
    return Response("<div style='padding:8px;color:#ccc;'>Preview will appear here.</div>",
                    mimetype="text/html")

import re, operator
OPS = {">": operator.gt, "<": operator.lt, ">=": operator.ge, "<=": operator.le, "==": operator.eq, "!=": operator.ne}

@app.route("/simple_query", methods=["POST"])
def simple_query():
    if not blob_exists("metadata.csv"):
        return Response(render_preview("metadata.csv not found"), mimetype="text/html")

    import re, operator
    OPS = {">": operator.gt, "<": operator.lt, ">=": operator.ge, "<=": operator.le, "==": operator.eq, "!=": operator.ne}

    col  = request.form.get("column")
    expr = (request.form.get("expr") or "").strip()
    val  = request.form.get("value") or ""

    rows, err = read_csv_rows("metadata.csv")
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
        if expr:        # plain text equality (case-insensitive)
            return s.lower() == expr.lower()
        if val:
            return s == val
        return False

    filtered = [header] + [r for r in rows[1:] if col_idx < len(r) and match(r[col_idx])]
    if len(filtered) == 1:
        return Response(render_preview("<div style='padding:8px;'>No matches.</div>"), mimetype="text/html")

    # Table HTML
    table_html  = "<table style='width:100%;border-collapse:collapse;'>"
    table_html += "<thead><tr>" + "".join(f"<th style='border:1px solid #444;padding:4px;'>{h or ''}</th>" for h in header) + "</tr></thead><tbody>"
    for r in filtered[1:]:
        table_html += "<tr>" + "".join(f"<td style='border:1px solid #333;padding:4px;'>{(c or '')}</td>" for c in r) + "</tr>"
    table_html += "</tbody></table>"

    # Collect pictures for matched rows
    images = []
    seen_files = set()
    for r in filtered[1:]:
        # prefer Picture column if present; else try to infer by Name
        pic = None
        if pic_idx is not None and pic_idx < len(r) and r[pic_idx]:
            candidate = r[pic_idx]
            if blob_exists(candidate):
                pic = candidate
        if not pic:
            nm = (r[name_idx] or "").strip()
            if nm:
                candidate = find_image_for_name(nm)  # uses your helper and IMAGE_EXTS
                if candidate:
                    pic = candidate
        if pic and pic not in seen_files:
            seen_files.add(pic)
            images.append(((r[name_idx] or pic), pic))

    # Images grid HTML (only if any)
    imgs_html = ""
    if images:
        imgs_html += "<div style='margin-top:12px'><h4>Pictures</h4>"
        imgs_html += "<div style='display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:8px;'>"
        for label, fname in images:
            src = get_blob_url(fname)  # signed URL to the blob
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
    if not blob_exists("metadata.csv"):
        return Response("metadata.csv not found", mimetype="text/html")

    column   = request.form.get("column")
    row_key  = (request.form.get("row_key") or "").strip()
    new_val  = request.form.get("new_value", "")

    rows, err = read_csv_rows("metadata.csv")
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
    url = get_blob_url("metadata.csv")
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
    if not blob_exists("metadata.csv"):
        return Response(render_preview("metadata.csv not found"), mimetype="text/html")
    row_key = (request.form.get("row_key") or "").strip()
    if not row_key:
        return Response(render_preview("no row key provided"), mimetype="text/html")
    rows, err = read_csv_rows("metadata.csv")
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
    url = get_blob_url("metadata.csv")
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
    if not blob_exists("metadata.csv"):
        return Response(render_preview("metadata.csv not found"), mimetype="text/html")
    name = (request.form.get("name") or "").strip()
    if not name:
        return Response(render_preview("no name provided"), mimetype="text/html")
    rows, err = read_csv_rows("metadata.csv")
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
    url = get_blob_url("metadata.csv")
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
