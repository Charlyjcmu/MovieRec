import pandas as pd
#import time
def getRatingDf():
    #s_time = time.time()
    rate=pd.read_csv("Data/kafka_rate.csv")
    print("Read columns:", len(rate))
    rate["rating"] = rate["movie id"].str[-1]
    rate["movie id"] = rate["movie id"].str[10:-2]
    rate["rating"] = rate["rating"].astype('int8')
    rate["user id"] = rate["user id"].astype('int32')
    
    df1 = rate.groupby("user id",as_index=False).count()
    df2 = df1.loc[df1['rating'] >= 6][['user id']]
    rate2 = pd.merge(rate, df2, on=['user id'], how='left', indicator='exists')
    rate2= rate2[rate2['exists'] == 'both'].drop('exists',axis=1)
    
    df3 = rate2.groupby("movie id",as_index=False).count()
    df4 = df3.loc[df3['rating'] >= 10][['movie id']]
    rate3 = pd.merge(rate2, df4, on=['movie id'], how='left', indicator='exists')
    rate3= rate3[rate3['exists'] == 'both'].drop('exists',axis=1)
    rate3.to_csv('movies/rating_from_raw_rating.csv',index=False)
    
    #e_time = time.time()
    #print("Read without rates: ", (e_time-s_time), "seconds")
    return rate
