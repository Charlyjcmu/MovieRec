from movies.model_selection import models_train
from DataCleaning.rate_preprocess import getRatingDf
from DataStreaming.kafkaPipeRate import getRateData
from subprocess import Popen, PIPE
import pandas as pd
import os
import timeit
import Keys

p1 = Popen(['lsof', f'-ti:9092'], stdout=PIPE)
output = p1.communicate()[0]
if not output.decode():
	os.system(f"sshpass -p {Keys.pd} ssh -o ServerAliveInterval=60 -L 9092:localhost:9092 tunnel@{Keys.ip} -NTf")
 
print("###########Getting Rate Data...########")
starttime = timeit.default_timer()
#getRateData(20000,20000)
#getRateData(1)
print("get rate data took: ", timeit.default_timer()-starttime)

print("Cleaning Rate data.....")
starttime = timeit.default_timer()
#df = getRatingDf()
df = pd.read_csv("movies/rating_from_raw_rating.csv")
print("cleaning data took: ", timeit.default_timer()-starttime)

print("Training model....")
starttime = timeit.default_timer()
models_train(df)
print("Training model took: ", timeit.default_timer()-starttime)
print("Model trained!!")

