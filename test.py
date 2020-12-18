import pandas as pd


genres = pd.read_csv('data/movies.csv', usecols=['genres'])
genres['genres'] = genres['genres'].str.split('|', expand=True)
#genre_list = pd.unique(genres['genres'])
gl = genres['genres'].unique().tolist()

print(gl)

