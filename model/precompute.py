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
    .appName("PreprocessSongData") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.port", "4041") \
    .getOrCreate()
    
# Preprocessing 
tracks = spark.read.csv("dataset.csv", header=True, inferSchema=True)
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

# Save preprocessed data
tracks.write.parquet("preprocessed_tracks.parquet")
print("Preprocessing completed. Data saved.")