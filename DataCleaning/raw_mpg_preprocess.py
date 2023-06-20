import pandas as pd
import time

def getmpgDf():
	s_time = time.time()
	#10000 rows is 0.09 seconds, 163911 rows in 1.25 seconds
	#335 seconds for 45 million rows
	mpg=pd.read_csv("Data/kafka_mpg.csv", chunksize=1000000)
	header = True
	length = 0
	for chunk in mpg:
		length+=len(chunk)
		print("Read columns:", length)
		chunk[['movie id','val']] = chunk['movie id'].str.split('/',expand=True)[[3,4]]
		chunk["val"] = chunk["val"].str[:-4].astype('int16')
		chunk["user id"] = chunk["user id"].astype('int32')
		if(header):
			chunk.to_csv('Data/mpg_from_raw_mpg.csv',header=header,index=False)
			header = False
		else:
			chunk.to_csv('Data/mpg_from_raw_mpg.csv',header=header,mode='a',index=False)

	e_time = time.time()
	print("Read without chunks: ", (e_time-s_time), "seconds") 
