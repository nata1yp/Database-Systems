# Taking first 100 rows of "movie_genres.scv"

from pyspark.sql import SparkSession 

spark = SparkSession.builder.appName("movie-genres-100").getOrCreate()

path = "hdfs://master:9000/files/movie_genres.csv"

df = spark.read.options(delimiter=",", header=False, inferSchema=True).csv(path).limit(100)

df.write.csv('hdfs://master:9000/files/movie-genres-100.csv')
