#!/Users/dangmai/BigData_Project/venv/bin/python
from flask import Flask, render_template, request, jsonify
import os
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from flask_limiter import Limiter
from pyspark.sql.functions import col, lower, trim
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, sum
from pyspark.ml.feature import CountVectorizer, VectorAssembler, Tokenizer
from pyspark.ml.linalg import DenseVector, SparseVector
from pyspark.sql.types import ArrayType, DoubleType
import numpy as np
import os
from pyspark.sql import functions as F

# Initialize Flask app and other setups
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../templates'))
static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../static'))


app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)

os.environ["PYSPARK_PYTHON"] = "/Users/dangmai/BigData_Project/venv/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/dangmai/BigData_Project/venv/bin/python"

# Initialize Spark session globally (reuse it across requests)
spark = SparkSession.builder \
    .appName("SongRecommendation") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.port", "4041") \
    .config("spark.executorEnv.PYSPARK_PYTHON", "/Users/dangmai/BigData_Project/venv/bin/python") \
    .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", "/Users/dangmai/BigData_Project/venv/bin/python") \
    .getOrCreate()
# Load and cache the dataset only once when the server starts
file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "preprocessed_tracks.parquet"))

tracks = spark.read.parquet(file_path)
tracks.cache()  # Cache the DataFrame to keep it in memory
print("Dataset loaded and cached.")

# Create a thread pool executor for background tasks
executor = ThreadPoolExecutor(max_workers=4)  # Limit number of background threads

# Initialize Flask limiter for rate limiting
limiter = Limiter(app)

tracks = spark.read.parquet("/Users/dangmai/BigData_Project/BE/preprocessed_tracks.parquet")

# Define cosine similarity function
def cosine_similarity_list(vec1, vec2):
    vec1, vec2 = np.array(vec1), np.array(vec2)
    norm1, norm2 = np.linalg.norm(vec1), np.linalg.norm(vec2)
    return float(np.dot(vec1, vec2) / (norm1 * norm2)) if norm1 and norm2 else 0.0

cosine_similarity_udf = udf(cosine_similarity_list, DoubleType())

# Recommend function
def recommend_songs(song_name):
    global tracks  
    input_song = tracks.filter(col("track_name") == song_name).select("features_list", "artists").collect()

    if not input_song:
        print("This song is either not so popular or you entered an invalid name.\nSome songs you may like:")
        tracks.select("track_name").orderBy(col("popularity").desc()).show(5)
        return

    input_features = input_song[0]["features_list"]
    input_artist = input_song[0]["artists"]

    artist_tracks = tracks.filter(col("artists") == input_artist)

   
    if artist_tracks.count() == 0:
        print(f"No other songs by {input_artist} found, recommending based on features.")
        artist_tracks = tracks  

    
    tracks_with_similarity = tracks.withColumn(
        "similarity", cosine_similarity_udf(lit(input_features), col("features_list"))
    )

    
    recommendations = tracks_with_similarity.filter(
        col("track_name") != song_name  
    ).orderBy(
        (col("artists") == input_artist).desc(),  
        col("similarity").desc(),  
        col("popularity").desc()  
    )

    
    recommendations.select("track_name", "artists").show(10)

# Recommend songs based on popularity
def recommend_songs_by_popularity():
    popular_tracks = tracks.orderBy(col("popularity").desc())
    
    popular_tracks.select("track_name", "artists").show(10)


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
    try:
        playlist = request.json.get("playlist", [])
        print(f"Received playlist: {playlist}")

        if not playlist:
            return jsonify({"error": "No playlist data provided."}), 400

        playlist_features = []
        for song in playlist:
            song_data = tracks.filter((col("track_name") == song["title"]) & (col("artists") == song["artist"])).select("features_list").collect()
            if song_data:
                playlist_features.append(song_data[0]["features_list"])

        if not playlist_features:
            return jsonify({"error": "No matching songs found in the dataset."}), 404

        combined_features = np.mean(playlist_features, axis=0)
        tracks_with_similarity = tracks.withColumn(
            "similarity", cosine_similarity_udf(lit(combined_features), col("features_list"))
        )
        playlist_titles = [song["title"] for song in playlist]
        recommendations = tracks_with_similarity.filter(~col("track_name").isin(playlist_titles)).orderBy(
            col("similarity").desc(), col("popularity").desc()
        ).select("track_name", "artists", "similarity").limit(10).collect()

        recommended_songs = [
            {"title": row["track_name"], "artist": row["artists"], "similarity": row["similarity"]}
            for row in recommendations
        ]
        print(f"Recommended songs: {recommended_songs}")
        return jsonify({"recommendations": recommended_songs})
    except Exception as e:
        print(f"Error in /recommend: {e}")
        return jsonify({"error": str(e)}), 500

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