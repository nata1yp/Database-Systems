from pyspark.sql import SparkSession
import time

t1 = time.time()

spark = SparkSession.builder.appName("query2-rdd").getOrCreate()

sc = spark.sparkContext

users = \
	sc.textFile("hdfs://master:9000/files/ratings.csv"). \
	map(lambda x : (x.split(",")[0], (float(x.split(",")[2]), 1))). \
	reduceByKey(lambda x, y : (x[0]+y[0], x[1]+y[1]) ). \
	map(lambda x : (x[0], x[1][0] / x[1][1])). \
	filter( lambda x : x[1] > 3).count()

all_users = \
	sc.textFile("hdfs://master:9000/files/ratings.csv"). \
	map(lambda x : x.split(",")[0]).distinct().count()

q2 = users/all_users * 100

print(str(q2) + '%')

t2 = time.time()

print(t2-t1)
