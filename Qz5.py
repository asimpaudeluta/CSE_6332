from flask import Flask, request, jsonify, render_template
import re
from collections import Counter

app = Flask(__name__)

# data = {"Amount": [10, 2, 40, 1, 10, 50, 5, 12, 25],
#         "Food": ["Apples", "Bananas", "Cherries", "Daikon", "Fig", "Grapes", "Peach", "Celery", "Watermelon"],
#         "Category": ["F", "F", "F", "V", "F", "F", "F", "V", "F"]}

FOOD_DATA = [
    {"amount": 10, "food": "Apples",     "category": "F"},
    {"amount": 2,  "food": "Bananas",    "category": "F"},
    {"amount": 40, "food": "Cherries",   "category": "F"},
    {"amount": 1,  "food": "Daikon",     "category": "V"},
    {"amount": 10, "food": "Fig",        "category": "F"},
    {"amount": 50, "food": "Grapes",     "category": "F"},
    {"amount": 5,  "food": "Peach",      "category": "F"},
    {"amount": 12, "food": "Celery",     "category": "V"},
    {"amount": 25, "food": "Watermelon", "category": "F"},
]

@app.route("/Qz5", methods=["GET"])
def qz4_home():
    return render_template("Qz5.html")

# @app.route("/Qz5/q10", methods=["POST"])
# def q10():
#     data = request.get_json(force=True)
#     S = data.get("S", "")
#     T = data.get("T", "")
#     C = data.get("C", "")
#     S = S.lower()
#     T = T.lower()
#     C = C.lower()
#     if not S or not T or not C:
#         return jsonify({"error": "Missing S, T or C"}), 400
#
#     counts = Counter(ch for ch in S if ch in T)
#     total = sum(counts.values())
#     freq = {k: round(v / total, 3) for k, v in counts.items()} if total else {}
#     replaced = "".join(C if ch in T else ch for ch in S)
#     return jsonify({
#         "counts": counts,
#         "frequencies": freq,
#         "replaced_text": replaced
#     })
#
# @app.route("/Qz5/q11", methods=["POST"])
# def q11():
#     data = request.get_json(force=True)
#     S = data.get("S", "")
#     T = data.get("T", "")
#     S = S.lower()
#     T = T.lower()
#     if not S or not T:
#         return jsonify({"error": "Missing S or T"}), 400
#     # instead of finding punctuation I do only char actually maybe numbers also
#     words = re.findall(r"[A-Za-z0-9']+", T)
#     word_count = len(words)
#     result = {}
#     for ch in S:
#         result[ch] = [w for w in words if w.lower().startswith(ch.lower())]
#     return jsonify({
#         "total_words": word_count,
#         "words_by_initial": result
#     })
#
# @app.route("/Qz5/q12", methods=["POST"])
# def q12():
#     data = request.get_json(force=True)
#     P = [p.lower() for p in data.get("P", []) if p]
#     T = data.get("T", "")
#     T = T.lower()
#     S = data.get("S", "")
#     S = S.lower()
#     if not T:
#         return jsonify({"error": "Missing text T"}), 400
#     words_T = re.findall(r"[A-Za-z0-9']+", T)
#     words_S = re.findall(r"[A-Za-z0-9']+", S)
#     filtered = [w for w in words_T if w.lower() not in P]
#     removed_count = len(words_T) - len(filtered)
#     cleaned_text = " ".join(filtered)
#
#     bigrams = []
#     for i, w in enumerate(filtered):
#         if S and w[0].lower() in S.lower():
#             if i > 0:
#                 bigrams.append(f"{filtered[i-1]} {w}")
#             if i < len(filtered) - 1:
#                 bigrams.append(f"{w} {filtered[i+1]}")
#     return jsonify({
#         "12_a_removed_stopwords": removed_count,
#         "12_a_cleaned_text": cleaned_text,
#         "12_b_bigrams": bigrams
#     })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
