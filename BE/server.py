from flask import Flask,render_template, request, jsonify
import os
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../templates'))
static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../static'))  # Adjust the path

app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
# Members API Route
@app.route("/")
def index_get():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)
