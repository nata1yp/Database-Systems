from pyspark.sql import SparkSession
import sys
import time

t1 = time.time()
spark = SparkSession.builder.appName("query3-sql").getOrCreate()

# Give the input format as arg

input_format = sys.argv[1]

if input_format == 'csv':
        movies = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/movies.csv")
        movie_genres = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/movie_genres.csv")
        ratings = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/ratings.csv")

else:
        movie_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
        movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
        ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")


movies.registerTempTable("movies")
ratings.registerTempTable("ratings")
movie_genres.registerTempTable("movie_genres")

sqlString = "select a.Genre as Movie_Genre, avg(b.Rating) as Average_Rating, count(b.ID) as Num_of_movies \
             from \
             (select distinct _c1 as Genre, _c0 as aID from movie_genres)a \
             inner join ( \
             select distinct _c1 as ID, avg(_c2) as Rating from ratings where _c2 is not null group by _c1\
             )b \
             on a.aID = b.ID \
             group by Genre\
             order by Genre"

res = spark.sql(sqlString)
res.show()

t2 = time.time()
print(t2-t1)
