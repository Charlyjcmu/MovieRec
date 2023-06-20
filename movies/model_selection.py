import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict  # data colector
from movies import app
import os
import pickle
from datetime import datetime
import surprise
from surprise.reader import Reader
from surprise import Dataset, dump
import shutil

##CrossValidation
from surprise.model_selection import cross_validate

##Matrix Factorization Algorithms
from surprise import SVD
from surprise import NMF

from surprise import KNNWithMeans, accuracy, SlopeOne, NMF, NormalPredictor, KNNBaseline, KNNBasic, KNNWithMeans, \
    KNNWithZScore, BaselineOnly, CoClustering
from surprise.model_selection import train_test_split, cross_validate

import pickle
import sys
import time

# Data Loading
def data_load():
#    resource_path = os.path.join()
    ratings_df = pd.read_csv(app.root_path+'/rating_from_raw_rating.csv')
    print("Ratings df len: ", len(ratings_df))

    # print("Number of unique movies in total data: ", len(ratings_df['movieId'].unique()))
    # print("Number of unique users in total data: ", len(ratings_df['userId'].unique()))
    return ratings_df

# Get list of watched ids for a user and remove rated movies
def get_watchedid_movies(df,unique_ids, userId):
    # get the list of the ids that the userid 13618 has watched
    iids1001 = df.loc[df['user id'] == userId, 'movie id']
    # remove the rated movies for the recommendations
    movies_to_predict = list(set(unique_ids) - set(iids1001)) #5 ms
    #movies_to_predict = np.setdiff1d(unique_ids, iids1001).tolist() #30 ms
    print("This user has not rated ", len(movies_to_predict), " movies")
    return movies_to_predict

# ger recommendation
def get_recommendations_for_users_new(df, uniqueMovies, userId,  algo, n=20,):
    t1 = time.time()
    movies_to_predict= get_watchedid_movies(df, uniqueMovies, userId)
    #my_recs =[]
    t2 = time.time()
    print("moviesPredict", t2-t1)
    my_recs =[(iid, algo.predict(uid=userId, iid=iid).est) for iid in movies_to_predict]
    
    #for iid in movies_to_predict: # 200 ms
    #    my_recs.append((iid, algo.predict(uid=userId, iid=iid).est))
    #print("Recommendations are as follows: ")
    t3 = time.time()
    print("my_recs", t3-t2)
    recosdf = pd.DataFrame(my_recs, columns=['iid', 'predictions']).sort_values('predictions', ascending=False).head(n)
    recos = recosdf.iid.values.tolist()[0:n]
    t4 = time.time()
    print("recos", t4-t3)
    #print(recos)
    return recos


# Model evaluation - offline metrics
def model_evaluation(ratings_df):
    reader = Reader(rating_scale=(1, 5))
    data = Dataset.load_from_df(ratings_df[["user id", "movie id", "rating"]], reader=reader)
    benchmark = []
    # Iterate over all algorithms
    # algorithms=[SVD(), SlopeOne(), NMF(), NormalPredictor(), KNNBaseline(), KNNBasic(), KNNWithMeans(),
    #                   KNNWithZScore(), BaselineOnly(), CoClustering()]
    #original code
    # for algorithm in [SVD()]:
    #     # Perform cross validation
    #     results = cross_validate(algorithm, data, measures=['RMSE'], cv=3, verbose=False)
    #     p = pickle.dumps(algorithm.fit(data.build_full_trainset()))
    #     # Get results & append algorithm name
    #     tmp = pd.DataFrame.from_dict(results).mean(axis=0)
    #     tmp = tmp.append(pd.Series([str(algorithm).split(' ')[0].split('.')[-1]], index=['Algorithm']))
    #     tmp['model_size'] = sys.getsizeof(p)
    #     benchmark.append(tmp)
    # value = '=====================================\n'
    # value+='Generating metrics for SVD model\n'
    # value+=datetime.now().strftime("%Y%m%d-%H%M%S")
    # value+=pd.DataFrame(benchmark).set_index('Algorithm').sort_values('test_rmse').to_string()
    # value+='\n=====================================\n'
    # print(value)
    # with open("Metrics.txt", 'a+') as f:
    #     f.write(value)
    count=0
    for algorithm in [SVD(), NMF()]:
        # Perform cross validation
        results = cross_validate(algorithm, data, measures=['RMSE'], cv=3, verbose=False)
        p = pickle.dumps(algorithm.fit(data.build_full_trainset()))
        # Get results & append algorithm name
        tmp = pd.DataFrame.from_dict(results).mean(axis=0)
        tmp = tmp.append(pd.Series([str(algorithm).split(' ')[0].split('.')[-1]], index=['Algorithm']))
        tmp['model_size'] = sys.getsizeof(p)
        benchmark.append(tmp)
        if count==0:
            value = '=====================================\n'
            value+='Generating metrics for SVD model\n'
            value+=datetime.now().strftime("%Y%m%d-%H%M%S")
            value+=pd.DataFrame(benchmark).set_index('Algorithm').sort_values('test_rmse').to_string()
            value+='\n=====================================\n'
            count+=1
        else:
            value = '=====================================\n'
            value+='Generating metrics for NMF model\n'
            value+=datetime.now().strftime("%Y%m%d-%H%M%S")
            value+=pd.DataFrame(benchmark).set_index('Algorithm').sort_values('test_rmse').to_string()
            value+='\n=====================================\n'
    print(value)
    with open("Metrics.txt", 'a+') as f:
        f.write(value)


#FUTURE_TODO: Save the dictionary during training itself
#FUTURE TODO: Write better code for checking for new user
def get_recommendations_for_users(predictions, userId,   n=20):
    movies = {}
    for uid, iid, _, est, _ in predictions:
        if uid==userId:
            movies[iid] = est
    if len(movies)==0:
        for uid, iid, _, est, _ in predictions:
           pass
        #    if uid == 303997:
        #       movies[iid] = est

    movies = dict(sorted(movies.items(), key=lambda x: x[1], reverse=True))
    return list(movies.keys())[:n]





def models_train(df):
    version_file = open(app.root_path+"/models/version.txt", "r")
    version = version_file.read()
    shutil.move(app.root_path +"/models/latest_model/SVDmodel","modelsOld/SVD_Models/v"+str(version))
    shutil.move(app.root_path + "/models/latest_model/NMFmodel", "modelsOld/NMF_Models/v" + str(version))
    reader = Reader(rating_scale=(1, 5))
    data = Dataset.load_from_df(df[["user id", "movie id", "rating"]], reader=reader)
    trainset = data.build_full_trainset()
    print("number of ratings in trainset: ", trainset.n_ratings)
    print("Number of users in trainset: ", trainset.n_users)
    print("Number of movies in trainset: ", trainset.n_items)

    algorithm_svd = SVD()
    algorithm_nmf = NMF()

    # Iterate over all algorithms
    
    # results = cross_validate(algorithm, data, measures=['RMSE'], cv=3, verbose=False)
    algorithm_svd.fit(trainset)
    algorithm_nmf.fit(trainset)
    
    dump.dump(app.root_path+"/models/latest_model/SVDmodel", algo=algorithm_svd)
    dump.dump(app.root_path + "/models/latest_model/NMFmodel", algo=algorithm_nmf)
    
    version_file.close()
    version_file = open(app.root_path+"/models/version.txt", "w")
    version = round(float(version) + 0.1,2)
    version_file.write(str(version))
    
    version_file.close()

