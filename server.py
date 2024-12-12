from flask import Flask, render_template, request, jsonify
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from model.song import recommend_songs

spark = SparkSession.builder.appName("Song Recommender").getOrCreate()

data = spark.read.csv("../BE/model/dataset.csv", header=True, inferSchema=True) # Replace with your dataset's path

template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../templates'))
static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../static'))

app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)

# Default route to render the homepage
@app.route("/")
def index_get():
    return render_template("index.html")

# Route to handle song recommendations
@app.route("/recommend", methods=["POST"])
def recommend():

    song_name = request.json.get("song_name")

    if not song_name:
        return jsonify({"error": "Please provide a song name."}), 400

    try:
        recommendations = recommend_songs(song_name, data)
        
        recommended_songs = [
            {"track_name": row["track_name"], "artists": row["artists"]}
            for row in recommendations.collect()
        ]

        return jsonify({"recommendations": recommended_songs})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
