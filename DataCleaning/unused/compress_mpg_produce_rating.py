from pyspark.sql import SparkSession
from pyspark.sql.functions import max,col,when
import time

def sparkCompress():
	stime = time.time()
	spark=SparkSession.builder.appName("mpg1").getOrCreate()

	#create dictionary, compress and save new ratings
	mpg1=spark.read.csv("Data/mpg_from_raw_mpg.csv",header=True,inferSchema=True)

	result=mpg1.groupBy("user id","movie id").agg(max("val").alias("max_timing"))
	d={(row['user id'],row['movie id']):row["max_timing"] for row in result.collect()}
	#print(d)

	d_list=[(key[0],key[1],value) for key,value in d.items()]
	d_df=spark.createDataFrame(d_list,["userid","movieid","timing"])
	mpg1=mpg1.join(d_df,(col("userid")==col("user id"))&(col("movieid")==col("movie id"))&(col("timing")==col("timing")))

	mpg1=mpg1.withColumn("rating",
	when(col("timing")<=20,1)
	.when(col("timing")<=50,2)
	.when(col("timing")<=100,3)
	.when(col("timing")<=150,4)
	.otherwise(5))

	df=mpg1.select("timing","user id","movie id","rating").toPandas()
	#df.to_csv('Data/spark_rating_from_mpg.csv')
	compression_opts = dict(method='zip',
	                        archive_name='spark_additional_rating_from_mpg_rating_score.csv')  
	df.to_csv('spark_additional_rating_from_mpg_rating_score.zip', index=False,
	          compression=compression_opts)  
	etime = time.time()
	print("Read without chunks: ", (etime-stime), "seconds")


