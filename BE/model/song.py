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
    .getOrCreate()
    
# Preprocessing 
tracks = spark.read.csv("../BE/model/dataset.csv", header=True, inferSchema=True)
tracks = tracks.dropna()

columns_to_check = ['popularity', 'duration_ms', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'liveness', 'valence']

# Filter out rows with non-numeric values
for col_name in columns_to_check:
    tracks = tracks.filter(F.col(col_name).rlike('^[+-]?\\d*(\\.\\d+)?([eE][+-]?\\d+)?$'))

tracks = tracks.orderBy(col("popularity").desc())
tracks = tracks.dropDuplicates(["track_name"])

tokenizer = Tokenizer(inputCol="track_genre", outputCol="genres_token")
tracks = tokenizer.transform(tracks)

cv = CountVectorizer(inputCol="genres_token", outputCol="genres_vector")
cv_model = cv.fit(tracks)
tracks = cv_model.transform(tracks)

numerical_cols = [col for col, dtype in tracks.dtypes if dtype in ("double", "float")]
assembler = VectorAssembler(inputCols=numerical_cols, outputCol="numerical_features")
tracks = assembler.transform(tracks)

assembler = VectorAssembler(
    inputCols=["genres_vector", "numerical_features"],
    outputCol="features"
)
tracks = assembler.transform(tracks)

def vector_to_numeric_list(vector):
    if isinstance(vector, SparseVector) or isinstance(vector, DenseVector):
        return [float(x) for x in vector.toArray()]
    return vector

vector_to_list_udf = udf(vector_to_numeric_list, ArrayType(DoubleType()))
tracks = tracks.withColumn("features_list", vector_to_list_udf(col("features")))

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