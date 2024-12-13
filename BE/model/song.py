# Import libraries 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, udf
from pyspark.ml.feature import CountVectorizer, VectorAssembler
from pyspark.ml.linalg import DenseVector
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import Normalizer
from pyspark.ml.feature import Tokenizer
from pyspark.sql.functions import lit

# Start a Spark session
spark = SparkSession.builder \
    .appName("SongRecommendation") \
    .getOrCreate()
    
# Preprocessing 
tracks = spark.read.csv("../BE/model/dataset.csv", header=True, inferSchema=True) # Replace with your dataset's path
tracks = tracks.dropna()
tracks = tracks.drop("track_id")
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

def cosine_similarity_udf(vec1, vec2):
    dot_product = float(vec1.dot(vec2))
    norm1 = float(vec1.norm(2))
    norm2 = float(vec2.norm(2))
    return dot_product / (norm1 * norm2)

cosine_similarity = udf(cosine_similarity_udf)

# Recommend function
def recommend_songs(song_name, data):
    input_song = data.filter(col("track_name") == song_name).select("features").collect()

    if not input_song:
        print("This song is either not so popular or you entered an invalid name.\nSome songs you may like:")
        data.select("track_name").orderBy(col("popularity").desc()).show(5)
        return

    input_features = input_song[0]["features"]

    data = data.withColumn("similarity", cosine_similarity(lit(input_features), col("features")))

    recommendations = data.orderBy(col("similarity").desc(), col("popularity").desc())

    recommendations.filter(col("track_name") != song_name).select("track_name", "artists").show(5)