import pandas as pd
import os
import statistics

offset = os.environ.get('OFFSET', default=25)-1
threshold = os.environ.get("threshold",3.6615207392926363 )
df = pd.read_csv('meow.csv', skiprows= offset, names=["time", "userId", "movieId", "year", "rating"])
df['day'] = df['time'].str[:10]
daywise_rating = df[['day','rating']].groupby(['day']).mean()
#Calculation of threshold from current data
# threshold = statistics.mean(list(df[['day','rating']].groupby(['day']).mean()['rating']))
if len(daywise_rating[abs(daywise_rating['rating'] - threshold) > 0.2]) > 0: print("Warning! Threshold has crossed a minimum value")
offset += len(df)
os.environ['OFFSET'] = offset
os.environ['threshold'] = threshold