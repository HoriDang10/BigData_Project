from flask import Flask, render_template, request, jsonify
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
from pyspark.sql import functions as F
from model.song import recommend_songs  # Import recommendation logic

# Initialize Flask app
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../templates'))
static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../static'))

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
# Normalize the dataset columns
tracks = spark.read.parquet(file_path)
# Apply normalization to the dataset
tracks = tracks.withColumn("track_name", lower(trim(col("track_name"))))
tracks = tracks.withColumn("track_name", F.regexp_replace(col("track_name"), r"–", "-"))
tracks = tracks.withColumn("track_name", F.regexp_replace(col("track_name"), r"&", "and"))
tracks = tracks.withColumn("artists", lower(trim(col("artists"))))
tracks = tracks.withColumn("artists", F.regexp_replace(col("artists"), r"–", "-"))
tracks = tracks.withColumn("artists", F.regexp_replace(col("artists"), r"&", "and"))

tracks.cache()

print("Dataset loaded and cached.")

def normalize_song_name(song_name):
    if song_name:
        song_name = song_name.strip().lower()
        song_name = song_name.replace("–", "-").replace("&", "and")
        song_name = song_name.replace(";", "").replace("(", "").replace(")", "")
        song_name = " ".join(song_name.split())  # Remove extra spaces
    return song_name

# Default route to render the homepage
@app.route("/")
def index():
    return render_template("index.html")

# Route to search for songs
@app.route("/search_song", methods=["POST"])
def search_song():
    try:
        query = request.json.get("query", "").strip()
        query = normalize_song_name(query)  # Normalize the query
        print(f"Search query received: '{query}'")

        # Case-insensitive matching with normalized query
        results = tracks.filter(
            lower(col("track_name")).like(f"%{query}%")
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



def normalize_song_name(song_name):
    """
    Normalize the song name to match the preprocessing format in the dataset.
    """
    if song_name:
        song_name = song_name.strip().lower()  # Lowercase and remove surrounding spaces
        song_name = song_name.replace("–", "-").replace("&", "and")  # Replace special characters
        song_name = song_name.replace(";", "").replace("(", "").replace(")", "")  # Remove unwanted symbols
        song_name = " ".join(song_name.split())  # Remove extra spaces
    return song_name
    
# Route to generate playlist
@app.route("/generate_playlist", methods=["POST"])
def generate_playlist():
    try:
        # Receive and normalize song_name
        raw_song_name = request.json.get("song", "")
        print(f"Raw song name received from frontend: {raw_song_name}")

        if not raw_song_name:
            return jsonify({"error": "Please provide a song name."}), 400

        # Normalize input song_name
        song_name = normalize_song_name(raw_song_name)
        print(f"Normalized song_name for recommendation: '{song_name}'")

        # Call the recommendation model
        recommended_songs = recommend_songs(song_name)
        print(f"Recommended songs from model: {recommended_songs}")

        if not recommended_songs:
            return jsonify({"error": "No songs found for this playlist."}), 404

        return jsonify({"playlist": recommended_songs})
    except Exception as e:
        print(f"Error in /generate_playlist: {e}")
        return jsonify({"error": str(e)}), 500

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
