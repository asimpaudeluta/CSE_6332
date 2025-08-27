import requests
import flask
import mimetypes
from flask import request, Response, render_template

app = flask.Flask(__name__)

TEXT_FILE_NAME = "milkyway.txt"
IMAGE_FILE_NAME = "milkyway.jpg"
CONTAINER_URL = "https://cse6332.blob.core.windows.net/privatecontainer"
SAS_TOKEN = "sp=racwdli&st=2025-08-27T22:20:54Z&se=2025-08-28T06:35:54Z&sv=2024-11-04&sr=c&sig=IuG2t3QKTdKZNm%2B3iDNC84prQOIAI0mP%2FTHaZgFz3zY%3D"

def get_blob_url(blob_name):
    return f"{CONTAINER_URL}/{blob_name}?{SAS_TOKEN}"

def read_text_blob():
    url = get_blob_url(TEXT_FILE_NAME)
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    return "Failed to load text from blob."

def upload_comment(comment):
    url = get_blob_url(TEXT_FILE_NAME)
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "text/plain"
    }
    response = requests.put(url, headers=headers, data=comment.encode('utf-8'))
    return response.status_code == 201

@app.route("/", methods=["GET"])
def index():
    content = read_text_blob()
    return render_template("index.html", content=content)

@app.route("/upload_comment", methods=["POST"])
def upload_comment_route():
    comment = request.form.get("comment", "")
    if upload_comment(comment):
        return flask.redirect("/")
    return "Failed to upload comment.", 500

@app.route("/get_image")
def get_image():
    url = get_blob_url(IMAGE_FILE_NAME)
    response = requests.get(url)
    if response.status_code == 200:
        mime_type, _ = mimetypes.guess_type(IMAGE_FILE_NAME)
        return Response(response.content, mimetype=mime_type)
    return "Failed to load image from blob.", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
