from flask import Flask, render_template, request, jsonify
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql import functions as F
from model.song import recommend_songs  # Import recommendation logic

# Initialize Flask app
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../templates'))
static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../static'))

os.environ["PYSPARK_PYTHON"] = "/Users/dangmai/BigData_Project/venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/dangmai/BigData_Project/venv/bin/python"
app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)

# Start a Spark session
spark = SparkSession.builder \
    .appName("Song Recommender") \
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.port", "4041") \
    .config("spark.ui.port", "4042") \
    .getOrCreate()

# Load preprocessed dataset
file_path = os.path.join(os.path.dirname(__file__), "preprocessed_tracks.parquet")
tracks = spark.read.parquet(file_path)

# Ensure trimming but retain original case
tracks = tracks.withColumn("track_name", trim(col("track_name")))
tracks = tracks.withColumn("artists", trim(col("artists")))

tracks.cache()

print("Dataset loaded and cached.")

# Default route to render the homepage
@app.route("/")
def index():
    return render_template("index.html")

# Route to search for songs
@app.route("/search_song", methods=["POST"])
def search_song():
    try:
        query = request.json.get("query", "").strip()
        print(f"Search query received: '{query}'")

        # Case-sensitive matching
        results = tracks.filter(
            col("track_name").like(f"%{query}%")
        ).select("track_name", "artists", "popularity").limit(10).collect()

        print(f"Search results: {[row.track_name for row in results]}")

        songs = [
            {"title": row["track_name"], "artist": row["artists"], "popularity": row["popularity"]}
            for row in results
        ]

        if not songs:
            return jsonify({"error": "No matching songs found."}), 404

        return jsonify({"songs": songs})
    except Exception as e:
        print(f"Error during search: {e}")
        return jsonify({"error": str(e)}), 500


# Route to generate playlist

@app.route("/generate_playlist", methods=["POST"])
def generate_playlist():
    try:
        song_name = request.json.get("song", "").strip()
        if not song_name:
            return jsonify({"error": "Please provide a song name."}), 400

        recommendations = recommend_songs(song_name, tracks)
        if not recommendations:
            return jsonify({"error": "No recommendations found."}), 404

        return jsonify({"playlist": recommendations})
    except Exception as e:
        print(f"Error in /generate_playlist: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
# Route to get popular songs (optional)
@app.route("/recommend_initial", methods=["GET"])
def recommend_initial():
    try:
        # Fetch the top 10 most popular songs
        popular_songs = tracks.orderBy(col("popularity").desc()).limit(10).collect()
        recommended_songs = [
            {"title": row["track_name"], "artist": row["artists"], "popularity": row["popularity"]}
            for row in popular_songs
        ]
        return jsonify({"recommended_songs": recommended_songs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
