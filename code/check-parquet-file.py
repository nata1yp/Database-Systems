from pyspark.sql import SparkSession 

spark = SparkSession.builder.appName("Csv_to_Parquet").getOrCreate()

parquetFile = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
#parquetFile.printSchema()

parquetFile.show(10)
