"""
Reads data from data set csv to pandas dataframes then from the dataframes to sqlite database
Gets data from TMDB API and inserts to database

Issues:
1. Remove cast_id from movies, what you need is to have movie_id in the cast table instead.
2. Remove crew_id from movies, what you need is to have movie_id in the crew table instead.
3. There is no tag table so I can't see where you would store the actual tag data field
4. I don't understand why tagger_id is the primary ket of the tags table, should it be tag_id? Since you don't need
who tagged it then you don't need the userId field from the data set
3. and 4. might be resolved if you have a tag table and a tag_movie table.
5. Working out the genres isn't trivial, you first need to create a list that separates the string using |,
you then need to remove duplicates, then you need to insert the de-duplicated genres into the genres table (this has
been done). Then when you insert the genres for a given movie you will have to split the genres field into each
separate value, lookup the id for the genre in the genre table, then insert the movie id and the genre id into your
movie_genre table. I don't have the time to do that but hopefully you can work it out from this explanation.
6. Some of the data seems to fail the not null and unique constraints in the movie table so you might need to either
check the data or remove the constraints
"""
import pandas as pd
import sqlite3


def add_movies():
    """
    Reads data from the movies and links csv files, merges the data and inserts into the movie table
    Release_year is created by extracting (YYYY) from the end of the title field, the brackets are then removed
    Title column is stripped of (YYYY)
    movies.csv: movieId,title
    links.csv: movieId,imdbId,tmdbId
    """
    movies = pd.read_csv('data/movies.csv', skiprows=1, usecols=[0, 1], names=['movie_id', 'title'])

    links = pd.read_csv('data/links.csv', skiprows=1, names=['movie_id', 'imdb_id', 'tmdb_id'], dtype={
        'tmdb_id':
            'Int64'
    })  # tmdb_id results in float otherwise

    # create a new column for year by extracting the year using a regular expression to match (YYYY)
    movies['release_year'] = movies['title'].str.extract(pat=r'(\([0-9][0-9][0-9][0-9]\))', expand=True)
    movies['release_year'] = movies['release_year'].str.strip('()')

    # remove the (YYYY) from the title using a regular expression
    movies['title'] = movies['title'].str.replace(r'(\([0-9][0-9][0-9][0-9]\))', '')

    # merge the movies and links data frames so that we get all of the necessary fields for the movie
    movies_data = pd.merge(movies, links, on='movie_id')

    # Execute sql to save the movies_data dataframe the movie table
    movies_data.to_sql('movie', index=False, con=con, if_exists='replace')


def add_ratings():
    """
    Inserts into the genre table
    ratings.csv: userId,movieId,rating,timestamp (only use the last 3 of these)
    tablename: ratings  fields: rating_id, movie_id, rating, rating_timestamp
    """
    ratings = pd.read_csv('data/ratings.csv', skiprows=1, usecols=[1, 2, 3], names=['rating_id', 'movie_id', 'rating',
                                                                                    'rating_timestamp'])
    # Executes sql to save the ratings dataframe the ratings table
    ratings.to_sql('ratings', index=False, con=con, if_exists='replace')


def add_tags():
    """
    You need to sort out the tags table issue before this can be implemented correctly.
    tags.csv: userId,movieId,tag,timestamp
    tablename tags: tagger_id, movie_id, tag_id
    """
    # tags = pd.read_csv('data/tags.csv', skiprows=1, usercols=['movieId','tag'], names=['movie_id', '??'])


def add_genres():
    """ Creates a list of unique genres and inserts into the genre table"""
    genres = pd.read_csv('data/movies.csv', usecols=['genres'])
    genres['genres'] = genres['genres'].str.split('|', expand=True)
    genre_list = genres['genres'].unique().tolist()
    # genre_list is a python list and not a pandas dataframe
    df = pd.DataFrame({'col': genre_list})
    df.to_sql('genre', index=False, con=con, if_exists='replace')


def add_tmbd_data():
    """
    You will do something like this:
    - Get an API key for tmdb (I don't have one so can't try this out)
    - Either use a library in Python that makes it easy to query the TMDB API eg https://github.com/celiao/tmdbsimple/
    There is a list of libraries on the TMDB site https://www.themoviedb.org/documentation/api/wrappers-libraries?language=en-GB
    - OR use python requests library e.g. https://dev.to/m0nica/how-to-use-the-tmdb-api-to-find-films-with-the-highest-revenue-82p
    - If you use the second of those methods the linked article shows you how to get the data in a pandas dataframe,
    you should then be able to use the df.to_sql() method to add to your database table
    - If you use library then you just need to follow the documentation in that library for how to use it. You might
    need to use sqlite3 to save to database which is something like:
    con = sqlite3.connect('data/movies_database.sqlite')
    cur = con.cursor()
    sql = ''' INSERT INTO tablename(colname1,colname2,etc...)
              VALUES(?,?,...) '''
    data_to_insert = [whatever data you got from the api]
    cur.execute(sql, data_to_insert)
    conn.commit()
    """
    pass


if __name__ == '__main__':
    # Create a connection object to work with the sqlite database
    con = sqlite3.connect('data/movies_database.sqlite')

    # add the data using the functions above
    add_movies()
    add_ratings()
    add_genres()
    # add_tags()
    # add_tmbd_data()

    # Close connection to database
    con.close()
