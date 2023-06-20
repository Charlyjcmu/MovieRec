from movies.model_selection import data_load, get_watchedid_movies, get_recommendations_for_users
from surprise import dump
import pandas as pd
from movies import app


def test_data_load():
    out = data_load()
    assert isinstance(out, pd.DataFrame)
    assert (len(out) != 0)


def test_csv_fields():
    df = data_load()
    check_list = ["time","userId","movieId","year","rating"]

    df_cols_list = df.columns.tolist()

    assert df_cols_list == check_list, "Columns are misaligned: {0} vs {1}".format(df_cols_list, check_list)

    assert (len(df.columns), 5, "CSV file must have 5 columns")
    assert (isinstance(df['time'], pd.core.series.Series))
    assert (df['year'].dtype == 'int64')
    assert (df['rating'].dtype == 'int64')
    assert (pd.api.types.is_integer_dtype(df['rating']))  # double check to ensure no float
    assert (df['userId'].dtype == 'int64')
    assert (pd.api.types.is_integer_dtype(df['userId']))  # double check to ensure no float
    assert (df['movieId'].dtype == 'object')


def test_movie_id():
    df = data_load()
    # check for years in column
    assert (df['movieId'].str[-4:].str.isnumeric().all())
    # check if there are any whitespace or space characters in the 'column' column
    pattern = r'\s'
    whitespace = df['movieId'].str.contains(pattern).any()
    assert (not whitespace)


def test_rating():
    df = data_load()
    assert (min(df['rating']) >= 1)
    assert (max(df['rating']) <= 5)


def test_rated_movie_removal():
    df = pd.DataFrame(
        {'time': ['2023-01-23T19:59:42', '2023-01-23T20:00:08', '2023-01-23T20:02:43', '2023-01-23T20:09:44'],
         'userId': [143499, 704712, 298756, 303997],
         'movieId': ['theres+something+about+mary+1998', 'free+willy+2+-+the+adventure+home+1998',
                     'the+godfather+part+ii+1998', 'the+brady+bunch+movie+1998'],
         'year': ['1998', '1995', '1974',
                  '1995'],
         'rating': [4, 3, 5, 2]})

    out = get_watchedid_movies(df, 704712)
    assert (out == ['the+brady+bunch+movie+1998', 'the+godfather+part+ii+1998', 'theres+something+about+mary+1998'
                      ])
    assert (len(out) == 3)


def test_recommendation_existinguser():
    df = data_load()
    test_set_predictions, _ = dump.load(app.root_path + "/models/latest_model")
    res = get_recommendations_for_users(test_set_predictions, 143499, 5)  # Existing user
    res1 = get_recommendations_for_users(test_set_predictions, 303997)  # Without n, default=20

    results = [res, res1]
    # recommendations are not blank
    assert isinstance(res, list)
    assert (all(len(x) > 0 for x in results))
    # recommendations don't include rated movies
    assert ('theres+something+about+mary+1998' not in res)
    assert ('the+brady+bunch+movie+1998' not in res1)
    # recommended movies are present in db
    assert (set(res).issubset(set(df['movieId'].values)))
    assert (set(res).issubset(set(df['movieId'].values)))

def test_recommendation_newuser():
    df = data_load()
    test_set_predictions, _ = dump.load(app.root_path + "/models/latest_model")

    res = get_recommendations_for_users(test_set_predictions, 54321, 10)  # New user
    res1 = get_recommendations_for_users(test_set_predictions, 12345)  # Without n, default=20
    results = [res, res1]

    # recommendations are not blank
    assert isinstance(res, list)
    assert (all(len(x) > 0 for x in results))
    # recommended movies are present in db
    assert (set(res).issubset(set(df['movieId'].values)))
    assert (set(res).issubset(set(df['movieId'].values)))

test_recommendation_existinguser()
test_recommendation_newuser()