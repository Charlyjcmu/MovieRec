import pandas as pd
df = pd.read_csv("rating_from_raw_rating.csv", nrows=10000)
df.to_csv("rating_from_raw_rating.csv")
