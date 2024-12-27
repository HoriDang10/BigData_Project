from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, monotonically_increasing_id
from pyspark.ml.feature import CountVectorizer, VectorAssembler, Tokenizer
from pyspark.ml.linalg import SparseVector, DenseVector
from pyspark.sql.types import ArrayType, DoubleType
import numpy as np
from pyspark.sql import functions as F

# Start Spark session
spark = SparkSession.builder \
    .appName("PreprocessSongData") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.port", "4041") \
    .getOrCreate()

# Load dataset
try:
    tracks = spark.read.csv("dataset.csv", header=True, inferSchema=True)
    tracks = tracks.dropna()  # Remove rows with null values
    print("Dataset loaded successfully.")
except Exception as e:
    print(f"Error loading dataset: {e}")
    exit(1)

# Filter out rows with invalid data
columns_to_check = ['popularity', 'duration_ms', 'danceability', 'energy', 
                    'key', 'loudness', 'mode', 'speechiness', 
                    'acousticness', 'liveness', 'valence']

for col_name in columns_to_check:
    if col_name not in tracks.columns:
        print(f"Warning: Column {col_name} not found in dataset.")
        continue
    tracks = tracks.filter(col(col_name).isNotNull())
                   
# Filter out rows with non-numeric values
for col_name in columns_to_check:
    tracks = tracks.filter(F.col(col_name).rlike('^[+-]?\\d*(\\.\\d+)?([eE][+-]?\\d+)?$'))

columns_to_cast = ['popularity', 'duration_ms', 'danceability', 'energy','key', 'loudness', 'mode', 'speechiness', 'acousticness','instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature']
for col_name in columns_to_cast:
    tracks = tracks.withColumn(col_name, col(col_name).cast('float'))

# Drop duplicate track names and order by popularity
tracks = tracks.dropDuplicates(["track_name"])
tracks = tracks.orderBy(col("popularity").desc())

# Tokenize the genres
if "track_genre" in tracks.columns:
    tokenizer = Tokenizer(inputCol="track_genre", outputCol="genres_token")
    tracks = tokenizer.transform(tracks)
else:
    print("Warning: Column 'track_genre' not found. Skipping tokenization.")

# CountVectorizer for genre tokens
try:
    cv = CountVectorizer(inputCol="genres_token", outputCol="genres_vector")
    cv_model = cv.fit(tracks)
    tracks = cv_model.transform(tracks)
except Exception as e:
    print(f"Error during CountVectorizer step: {e}")

# Assemble numerical columns
numerical_cols = [col_name for col_name in columns_to_check if col_name in tracks.columns]

try:
    numerical_assembler = VectorAssembler(inputCols=numerical_cols, outputCol="numerical_features")
    tracks = numerical_assembler.transform(tracks)

    feature_assembler = VectorAssembler(
        inputCols=["genres_vector", "numerical_features"],
        outputCol="features"
    )
    tracks = feature_assembler.transform(tracks)
except Exception as e:
    print(f"Error during VectorAssembler step: {e}")

# Convert features vector to list
def vector_to_numeric_list(vector):
    if isinstance(vector, (SparseVector, DenseVector)):
        return [float(x) for x in vector.toArray()]
    return []

vector_to_list_udf = udf(vector_to_numeric_list, ArrayType(DoubleType()))
tracks = tracks.withColumn("features_list", vector_to_list_udf(col("features")))

# Save preprocessed data
try:
    tracks.write.parquet("preprocessed_tracks.parquet", mode="overwrite")
    print("Preprocessing completed. Data saved to 'preprocessed_tracks.parquet'.")
except Exception as e:
    print(f"Error saving preprocessed data: {e}")
    