import os
import requests
import flask
import mimetypes
from flask import request, Response, render_template, redirect
import json

app = flask.Flask(__name__)

TEXT_FILE_NAME = "milkyway.txt"
IMAGE_FILE_NAME = "milkyway.jpg"
CONTAINER_URL = "https://cse6332.blob.core.windows.net/privatecontainer"

SAS_TOKEN = os.getenv("SAS_TOKEN")
if not SAS_TOKEN:
    try:
        with open("secrets.json") as f:
            secrets = json.load(f)
        SAS_TOKEN = str(secrets["SAS_TOKEN"])
    except Exception:
        SAS_TOKEN = ""

def get_blob_url(blob_name: str) -> str:
    token = SAS_TOKEN.lstrip("?")
    sep = "?" if "?" not in CONTAINER_URL else "&"
    return f"{CONTAINER_URL}/{blob_name}{sep}{token}" if token else f"{CONTAINER_URL}/{blob_name}"

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

@app.route("/", methods=["GET"])
def index():
    content = read_text_blob(TEXT_FILE_NAME)
    return render_template("CW1.html", content=content)

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
