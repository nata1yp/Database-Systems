from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

t1 = time.time()

spark = SparkSession.builder.appName("query4-rdd").getOrCreate()

sc = spark.sparkContext

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

def find_quinquennium(x):
        if x >= 2000 and x <= 2004:
                return('2000-2004')
        elif x >= 2005 and x <= 2009:
                return('2005-2009')
        elif x >= 2010 and x<=2014:
                return('2010-2014')
        elif x >= 2015 and x <= 2019:
                return('2015-2019')

genres = \
        sc.textFile("hdfs://master:9000/files/movie_genres.csv").\
        map(lambda x : (x.split(",")[0], x.split(",")[1])).\
        filter(lambda x : x[1] == 'Drama')


movies = \
        sc.textFile("hdfs://master:9000/files/movies.csv").\
        filter(lambda x : split_complex(x)[3][0:4] != '' and split_complex(x)[2] != '' and int(split_complex(x)[3][0:4]) >= 2000 and int(split_complex(x)[3][0:4]) <= 2019).\
        map(lambda x : (split_complex(x)[0], (find_quinquennium(int(split_complex(x)[3][0:4])), len(split_complex(x)[2].split(" ")))))

q4 = genres.join(movies).\
        map(lambda x : (x[1][1][0], ((x[1][1][1], 1)))).\
        reduceByKey(lambda x, y : (x[0]+y[0], x[1]+y[1]) ).\
        sortByKey().\
        map(lambda x : (x[0], x[1][0] / x[1][1])).collect()

for i in q4:
	print(i)

t2 = time.time()
print(t2-t1)

