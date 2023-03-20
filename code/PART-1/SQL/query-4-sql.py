from pyspark.sql import SparkSession
import sys
import time

t1 = time.time()
spark = SparkSession.builder.appName("query4-sql").getOrCreate()

input_format = sys.argv[1]

# Give the input format as arg

if input_format == 'csv':
        df1 = spark.read.format('csv').options(header = 'false', inferSchema = 'true'). \
                load("hdfs://master:9000/files/movie_genres.csv")
        df2 = spark.read.format('csv').options(header = 'false', inferSchema = 'true'). \
                load("hdfs://master:9000/files/movies.csv")

        df1 = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
        df2 = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
else:
        df1 = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
        df2 = spark.read.parquet("hdfs://master:9000/files/movies.parquet")


df1.registerTempTable("movie_genres")
df2.registerTempTable("movies")

def quinquennium(s):
  if s > 1999 and s < 2005:
    q4 = '2000-2004'
  elif s > 2004 and s < 2010:
    q4 = '2005-2009'
  elif s > 2009 and s < 2015:
    q4 = '2010-2014'
  elif s > 2014 and s < 2020:
    q4 = '2015-2019'
  else:
    q4 = '0'
  return q4

def wordscount(s):
   return len(s.split(' '))

spark.udf.register("Wordscount", wordscount)
spark.udf.register("Quinquennium", quinquennium)

sqlString = \
        "select a.quin as Quinquennium, avg(a.Words) as AverageLengthOfSummary \
        from (select m._c0, m._c1, m._c2, Quinquennium(year(m._c3)) as quin, Wordscount(m._c2) as Words, g._c0 \
        from movies as m, movie_genres as g\
        where m._c0 == g._c0 and (g._c1) LIKE 'Drama' and year(m._c3) > 1999 and year(m._c3) < 2020 and m._c2 is not null)a\
        group by a.quin order by a.quin"

q4 = spark.sql(sqlString)
q4.show()

t2 = time.time()
print(t2-t1)
