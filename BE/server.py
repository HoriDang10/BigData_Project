from flask import Flask, render_template, request, jsonify
import os
import multiprocessing

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from model.song import recommend_songs
from model.song import tracks 

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

@app.route("/recommend_based_on_playlist", methods=["POST"])
def recommend_based_on_playlist():
    try:
        # Get the playlist from the request
        playlist = request.json.get("playlist", [])
        if not playlist:
            return jsonify({"error": "Your playlist is empty."}), 400

        # Filter tracks in the playlist
        playlist_tracks = tracks.filter(col("track_name").isin([song["title"] for song in playlist]))

        if playlist_tracks.count() == 0:
            return jsonify({"error": "No matching tracks found in the dataset."}), 400

        # Get features of the playlist songs (e.g., top features of added songs)
        playlist_features = playlist_tracks.select("features").collect()

        # Aggregate features for recommendations (e.g., cosine similarity, popularity, etc.)
        # Example logic: Recommend songs based on the average features of the playlist
        recommendations = tracks.withColumn(
            "similarity", cosine_similarity_udf(
                lit(playlist_features[0]["features"]), col("features")
            )
        ).filter(~col("track_name").isin([song["title"] for song in playlist]))\
          .orderBy(col("similarity").desc(), col("popularity").desc()).limit(10)

        # Prepare recommendations for the response
        recommended_songs = [
            {"title": row["track_name"], "artist": row["artists"], "popularity": row["popularity"]}
            for row in recommendations.collect()
        ]

        return jsonify({"recommended_songs": recommended_songs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
    
@app.route("/search_song", methods=["POST"])
def search_song():
    try:
        # Get the search query from the request
        query = request.json.get("query", "").strip().lower()
        if not query:
            return jsonify({"error": "No search query provided."}), 400

        # Filter songs based on the query
        results = tracks.filter(
            col("track_name").rlike(f".*{query}.*")
        ).select("track_name", "artists", "popularity").limit(10).collect()

        # Format results for the response
        songs = [
            {"title": row["track_name"], "artist": row["artists"], "popularity": row["popularity"]}
            for row in results
        ]

        return jsonify({"songs": songs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
