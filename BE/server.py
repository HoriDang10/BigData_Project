from flask import Flask, render_template, request, jsonify
import os
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from model.song import recommend_songs, tracks
from flask_limiter import Limiter
from pyspark.sql.functions import col, lower, trim

# Initialize Flask app and other setups
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../templates'))
static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../static'))

app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)

# Initialize Spark session globally (reuse it across requests)
spark = SparkSession.builder \
    .appName("Song Recommender") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.port", "4041") \
    .getOrCreate()

# Load and cache the dataset only once when the server starts
file_path = os.path.join(os.path.dirname(__file__), "preprocessed_tracks.parquet")
tracks = spark.read.parquet(file_path)
tracks.cache()  # Cache the DataFrame to keep it in memory
print("Dataset loaded and cached.")

# Create a thread pool executor for background tasks
executor = ThreadPoolExecutor(max_workers=4)  # Limit number of background threads

# Initialize Flask limiter for rate limiting
limiter = Limiter(app)

# Function to handle heavy recommendation processing in background
def recommend_async(song_name, callback):
    try:
        recommended_songs = recommend_songs(song_name)  # Recommendation logic
        callback({"recommendations": recommended_songs})
    except Exception as e:
        callback({"error": str(e)})

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

    # Start recommendation task in the background
    def send_recommendations(response):
        return jsonify(response)

    # Use background thread for heavy processing
    executor.submit(recommend_async, song_name, send_recommendations)
    return jsonify({"message": "Recommendation in progress..."})

# Route for recommending songs based on popularity
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

# Route for recommending songs based on a playlist
@app.route("/recommend_based_on_playlist", methods=["POST"])
def recommend_based_on_playlist():
    try:
        # Get the playlist from the request
        playlist = request.json.get("playlist", [])
        if not playlist:
            return jsonify({"error": "Your playlist is empty."}), 400

        # Convert playlist into a list of song names
        song_names = [song["title"] for song in playlist]

        # Filter tracks in the playlist (optimized with limit to avoid excessive memory use)
        playlist_tracks = tracks.filter(col("track_name").isin(song_names)).limit(100)

        if playlist_tracks.count() == 0:
            return jsonify({"error": "No matching tracks found in the dataset."}), 400

        # Get features of the playlist songs
        playlist_features = playlist_tracks.select("features").collect()

        # Aggregate features for recommendations (e.g., cosine similarity, popularity, etc.)
        recommendations = tracks.withColumn(
            "similarity", cosine_similarity_udf(
                lit(playlist_features[0]["features"]), col("features")
            )
        ).filter(~col("track_name").isin(song_names))\
          .orderBy(col("similarity").desc(), col("popularity").desc()).limit(10)

        # Prepare recommendations for the response
        recommended_songs = [
            {"title": row["track_name"], "artist": row["artists"], "popularity": row["popularity"]}
            for row in recommendations.collect()
        ]

        return jsonify({"recommended_songs": recommended_songs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Route to search song
@app.route("/search_song", methods=["POST"])
def search_song():
    try:
        # Get the search query from the request
        query = request.json.get("query", "").strip().lower()
        if not query:
            return jsonify({"error": "No search query provided."}), 400

        # Handle missing values and trim whitespace in the dataset
        clean_tracks = tracks.filter(col("track_name").isNotNull())
        clean_tracks = clean_tracks.withColumn("track_name", trim(col("track_name")))

        # Perform a case-insensitive search
        results = clean_tracks.filter(
            lower(col("track_name")).like(f"%{query}%")
        ).select("track_name", "artists", "popularity").limit(10).collect()

        # Format results for the response
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
    
# Route to load songs for the homepage
@app.route("/load_songs", methods=["GET"])
def load_songs():
    try:
        # Fetch a subset of relevant columns to send to the frontend
        song_list = tracks.select("track_name", "artists", "popularity").limit(100).collect()
        songs = [
            {"title": row["track_name"], "artist": row["artists"], "popularity": row["popularity"]}
            for row in song_list
        ]
        return jsonify({"songs": songs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)