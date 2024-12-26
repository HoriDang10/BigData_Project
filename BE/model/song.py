from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.linalg import DenseVector
import numpy as np

# Define cosine similarity function
def cosine_similarity(vec1, vec2):
    if not vec1 or not vec2:  # Check for None or empty vectors
        return 0.0
    vec1 = np.array(vec1, dtype=float)
    vec2 = np.array(vec2, dtype=float)
    dot_product = np.dot(vec1, vec2)
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    return float(dot_product / (norm1 * norm2)) if norm1 > 0 and norm2 > 0 else 0.0

# UDF for similarity calculation
def recommend_songs(song_name, tracks):
    try:
        print(f"Searching for song: {song_name}")

        # Find input song features
        input_song = tracks.filter(col("track_name") == song_name).select("features_list").collect()
        if not input_song:
            print(f"No exact match found for: {song_name}")
            return []

        # Extract input features
        input_features = input_song[0]["features_list"]

        # Broadcast input features
        broadcast_features = tracks._sc.broadcast(input_features)

        # Define UDF
        @udf(DoubleType())
        def similarity_udf(features_list):
            return cosine_similarity(broadcast_features.value, features_list)

        # Calculate similarity
        tracks_with_similarity = tracks.withColumn(
            "similarity", similarity_udf(col("features_list"))
        )

        # Get top recommendations
        recommendations = tracks_with_similarity.filter(
            col("track_name") != song_name
        ).orderBy(
            col("similarity").desc(),
            col("popularity").desc()
        ).select("track_name", "artists", "similarity").limit(10).collect()

        print("Recommendations:")
        for row in recommendations:
            print(f"Track: {row.track_name}, Artist: {row.artists}, Similarity: {row.similarity}")

        return [{"title": row.track_name, "artist": row.artists} for row in recommendations]
    except Exception as e:
        print(f"Error in recommend_songs: {e}")
        return []


# Recommend songs based on popularity
def recommend_songs_by_popularity():
    popular_tracks = tracks.orderBy(col("popularity").desc())
    
    popular_tracks.select("track_name", "artists").show(10)