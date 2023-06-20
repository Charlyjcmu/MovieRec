from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
import time

def getmpgDfSpark():
	s_time = time.time()
	spark=SparkSession.builder.appName("mpg").getOrCreate()

	mpg = spark.read.csv("Data/temp_mpg.csv", header=True, inferSchema=True)
	# Split the `movie id` column into `name`, `year`, and `val`
	mpg = mpg.withColumn("name", regexp_extract("movie id", r"^(.*?)\+(\d{4})/\d$", 1))
	mpg = mpg.withColumn("year", regexp_extract("movie id", r"^(.*?)\+(\d{4})/\d$", 2))
	mpg = mpg.withColumn("val", regexp_extract("movie id", r"^(.*?)\+\d{4}/(\d)$", 1))
	# Select the columns you need and rename them
	mpg = mpg.select("time", "user id", "name", mpg.year.substr(1, 4).alias("year"), mpg.val.cast("int").alias("val"))
	mpg = mpg.withColumnRenamed("name", "movie id")
	# Save the resulting DataFrame as a CSV file
	df = mpg.select("user id","movie id","val").toPandas()
	df.write.csv("Data/spark_mpg.csv", header=True)

	e_time = time.time()
	print("Read without chunks: ", (e_time-s_time), "seconds") 
