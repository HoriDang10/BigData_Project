
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
def cosine_similarity_list(vec1, vec2): #check this 
    # Ensure vectors are Dense Arrays
    if isinstance(vec1, SparseVector):
        vec1 = vec1.toArray()
    elif isinstance(vec1, DenseVector):
        vec1 = np.array(vec1)

    if isinstance(vec2, SparseVector):
        vec2 = vec2.toArray()
    elif isinstance(vec2, DenseVector):
        vec2 = np.array(vec2)

    # If vectors are still not lists/arrays, handle gracefully
    vec1 = np.array(vec1, dtype=float)
    vec2 = np.array(vec2, dtype=float)

    # Compute cosine similarity with zero norm handling
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    if norm1 == 0 or norm2 == 0:
        return 0.0  # Avoid division by zero

    return float(np.dot(vec1, vec2) / (norm1 * norm2))

    cosine_similarity_udf = udf(cosine_similarity_list, DoubleType()) # check this 

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
