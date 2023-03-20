from pyspark.sql import SparkSession
import sys
import time

t1 = time.time()
spark = SparkSession.builder.appName("query2-sql").getOrCreate()

# Give the input format as arg

input_format = sys.argv[1]

if input_format == 'csv':
        df = spark.read.format('csv').options(header = 'false', inferSchema = 'true'). \
                load("hdfs://master:9000/files/ratings.csv")
else:
        df = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")

df.registerTempTable("ratings")

all_users = "select * \
                    from (select distinct _c0 from ratings)"

users = "select * \
         from ( \
         select _c0 \
         from ratings \
         group by _c0 \
         having AVG(_c2 ) > 3 )"

res = spark.sql(users)
num = res.count()

q2 = spark.sql(all_users)
den = q2.count()

percentage = 100 * (num / den)
print(str(percentage) + "%")

t2 = time.time()
print(t2-t1)

