from pyspark.sql import SparkSession
from pyspark.sql.functions import max,col,when
import time
import csv

def sparkCompress():
	stime = time.time()
	spark=SparkSession.builder.appName("mpg1").getOrCreate()

	#create dictionary, compress and save new ratings
	mpg1=spark.read.csv("Data/mpg_from_raw_mpg.csv",header=True,inferSchema=True)

	#result0=mpg1.groupBy("user id").agg({'val':'max'})
	#print('unique users: ', result0.count())
	#result1=mpg1.groupBy("movie id").agg({'val':'max'})
	#print('unique movies: ', result1.count())

	result=mpg1.groupBy("user id","movie id").agg({'val':'max','time':'max'}).withColumnRenamed('max(val)','timing').withColumnRenamed('max(time)', 'time')
	#print(result.collect()[0])
	#print('type: ',type(result))
	#print('unique row: ', result.count())
	
	result2=result.withColumn("rating",
	when(col("timing")<=20,1)
	.when(col("timing")<=50,2)
	.when(col("timing")<=100,3)
	.when(col("timing")<=150,4)
	.otherwise(5))

	df=result2.select("time","user id","movie id","rating")
	with open('Data/spark_rating_from_mpg.csv','w') as fp:
		writer = csv.DictWriter(fp,fieldnames=df.columns)
		writer.writerow(dict(zip(df.columns,df.columns)))
		for row in df.toLocalIterator():
			writer.writerow(row.asDict())
	etime = time.time()
	print("Read without chunks: ", (etime-stime), "seconds")


