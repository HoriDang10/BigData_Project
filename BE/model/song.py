
# Import libraries 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, sum
from pyspark.ml.feature import CountVectorizer, VectorAssembler, Tokenizer
from pyspark.ml.linalg import DenseVector, SparseVector
from pyspark.sql.types import ArrayType, DoubleType
import numpy as np
from pyspark.sql import functions as F

# Start a Spark session
spark = SparkSession.builder \
    .appName("SongRecommendation") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.port", "4041") \
    .getOrCreate()
    
# Preprocessing 
tracks = spark.read.parquet("preprocessed_tracks.parquet")

# Define cosine similarity function
def cosine_similarity_list(vec1, vec2):
    vec1 = np.array(vec1, dtype=float)
    vec2 = np.array(vec2, dtype=float)
    dot_product = float(np.dot(vec1, vec2))
    norm1 = float(np.linalg.norm(vec1))
    norm2 = float(np.linalg.norm(vec2))
    return dot_product / (norm1 * norm2)


cosine_similarity_udf = udf(cosine_similarity_list, DoubleType())

# Recommend function
def recommend_songs(song_name):
    global tracks
    try:
        print(f"Normalized song_name: '{song_name}'")
        print("Dataset preview (normalized):")
        tracks.select("track_name", "artists").show(10, truncate=False)

        # Check for an exact match
        input_song = tracks.filter(col("track_name") == song_name).select("features_list", "artists").collect()
        if not input_song:
            print(f"Exact match not found for: {song_name}")

            # Fallback to partial match
            input_song = tracks.filter(col("track_name").like(f"%{song_name}%")).select("features_list", "artists").collect()
            if not input_song:
                print(f"Partial match not found for: {song_name}")
                return []

        # Process input song if found
        input_features = input_song[0]["features_list"]
        print(f"Input features: {input_features}")

        # Calculate similarity
        tracks_with_similarity = tracks.withColumn(
            "similarity", cosine_similarity_udf(lit(input_features), col("features_list"))
        )

        recommendations = tracks_with_similarity.filter(
            col("track_name") != song_name
        ).orderBy(
            col("similarity").desc(),
            col("popularity").desc()
        ).select("track_name", "artists", "similarity").limit(10).collect()

        print("Final recommendations:")
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