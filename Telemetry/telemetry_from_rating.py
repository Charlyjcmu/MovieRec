import pandas as pd
rating=pd.read_csv("/home/team14/Downloads/kafkaPipe/Data/rating_from_raw_rating.csv")
length=len(rating)

rating['time'] = pd.to_datetime(rating['time'])

# Calculate the time difference between each row and the first row
time_diff = rating['time'] - rating['time'][0]

# Find the index of the row that is 12 hours after the first row
index_2hrs = (time_diff >= pd.Timedelta(hours=12)).idxmax()
if index_2hrs!=0:
# Retrieve the row that is 2 hours after the first row
    row_2hrs = rating.iloc[index_2hrs]

    # Print the row
    start_time=rating['time'][0]
    print("start time",start_time)
    #print(row_2hrs)
    
    average_rating=rating['rating'][index_2hrs:].mean()
    #print(rating['time'][index_2hrs:])
    print(average_rating)
else:
    print("None")
    
with open('telemetry_collection.txt','a') as f:
	f.write("The average rating of rows with timestamps 12 hrs after \n 1st rating timestamp pulled, which is  %s \n" %start_time)
	f.write("%s \n" %average_rating)
f.close()
  
#print(df)
#print(df.loc[3])
#compression_opts = dict(method='zip',archive_name='rating_from_raw_rating.csv')  
#df.to_csv('rating_from_raw_rating.zip', index=False,
          #compression=compression_opts)  

