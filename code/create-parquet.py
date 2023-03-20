from pyspark.sql import SparkSession 

spark = SparkSession.builder.appName("Csv_to_Parquet").getOrCreate()
sc = spark.sparkContext

path = 'hdfs://master:9000/files/movie_genres.csv'
df = spark.read.options(delimiter=",", header=False, inferSchema=True).csv(path)

df.write.parquet("hdfs://master:9000/files/movie_genres.parquet")
