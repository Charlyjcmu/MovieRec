import pandas as pd

recs = pd.read_csv('kafka_recs.csv')
mpg = pd.read_csv('mpg_from_raw_mpg.csv')

user_groups = recs.groupby('user id')['recs'].apply(list)
cross_df = pd.merge(recs, mpg, on="user id")
cross_df['UserMatch'] = cross_df.apply(lambda x: x['movie id'] in x['recs'], axis=1)
cross_df = cross_df.loc[cross_df['UserMatch'] == True]
cross_df.to_csv("Users_vs_Recommendations.csv")