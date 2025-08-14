import os

from flask import (
    flash,
    Flask,
    jsonify,
    redirect,
    render_template_string,
    request,
    send_file,
)

app = Flask(__name__)

app = Flask(__name__)
app.secret_key = "supersecret"

UPLOAD_FOLDER: str = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


HTML_FORM = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Загрузка файла</title>
    <style>
        body { font-family: Arial, sans-serif; background: #f0f2f5; display: flex; justify-content: center; align-items: center; height: 100vh; }
        .container { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 4px 10px rgba(0,0,0,0.1); }
        input[type=file] { margin-bottom: 15px; }
        button { padding: 10px 20px; border: none; background: #4CAF50; color: white; border-radius: 5px; cursor: pointer; }
        button:hover { background: #45a049; }
        .message { color: green; margin-bottom: 15px; }
    </style>
</head>
<body>
    <div class="container">
        {% with messages = get_flashed_messages() %}
          {% if messages %}
            <div class="message">{{ messages[0] }}</div>
          {% endif %}
        {% endwith %}
        <form method="post" enctype="multipart/form-data">
            <input type="file" name="file" required>
            <br>
            <button type="submit">Загрузить</button>
        </form>
    </div>
</body>
</html>
"""


@app.route("/healthcheck", methods=["GET"])
def home():
    return jsonify({"message": "ok"})


@app.route("/upload", methods=["POST", "GET"])
def upload_file():
    if request.method == "POST":
        if "file" not in request.files:
            flash("Файл не выбран")
            return redirect(request.url)
        file = request.files["file"]
        if file.filename == "":
            flash("Файл не выбран")
            return redirect(request.url)
        filepath = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(filepath)
        flash(f"Файл '{file.filename}' успешно загружен!")
        return redirect(request.url)
    return render_template_string(HTML_FORM)


@app.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    if not os.path.exists(filepath):
        return {"error": "Файл не найден"}, 404

    return send_file(filepath, as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True, port=47900)
