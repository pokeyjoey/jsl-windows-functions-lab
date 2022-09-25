import pandas as pd
#url = "https://raw.githubusercontent.com/data-eng-10-21/window-functions/main/data/"
#movies_df = pd.read_csv(f'{url}/movies.csv')
#names_df = pd.read_csv(f'{url}/names.csv')
#ratings_df = pd.read_csv(f'{url}/ratings.csv')
#title_principals_df = pd.read_csv(f'{url}/title_principals.csv')
#names_df = pd.read_csv(f'{url}/names.csv')

import sqlite3
conn = sqlite3.connect('imdb.db')

# create tables
#movies_df.to_sql('movies', conn, index = False, if_exists = 'replace')
#names_df.to_sql('names', conn, index = False, if_exists = 'replace')
#ratings_df.to_sql('ratings', conn, index = False, if_exists = 'replace')
#title_principals_df.to_sql('movie_roles', conn, index = False, if_exists = 'replace')

print('select a record from the movies table')
print(pd.read_sql('SELECT * FROM movies LIMIT 1;', conn))
print()
print()


print("Let's begin by selecting movies after the year 2000, finding the average length of the movie for that year, and also including columns for the title of the movie, and each movie's runtime. Order the movies by year and duration.")
query = """
    SELECT title, duration, AVG(duration) OVER () AS avg_duration
    FROM movies
    WHERE year > 2000
    ORDER BY year, duration
    LIMIT 5"""
print(query)
print(pd.read_sql(query, conn))
print()
print()

print("select each movie's year, title, duration, and calculate the average_duration for that year, and movies of the same genre.")
print("Then calculate how each movie's runtime length deviates from the average.")
print('Select only those movies made after 2018, whose genre is Drama or Comedy and order the results by year, genre, and duration.')
query = """
    SELECT year, title, duration,
        AVG(duration) OVER (partition by year, genre) AS avg_length,
        duration - AVG(duration) OVER (partition by year, genre) AS length_minus_avg
    FROM movies
    WHERE year > 2018
    AND genre in ('Drama', 'Comedy')
    ORDER BY year, genre, duration DESC
    LIMIT 5"""
print(query)
print(pd.read_sql(query, conn))
print()
print()

print("select each movie's year, title, duration, and calculate the average_duration for that year, and movies of the same genre.")
print("Then calculate how each movie's runtime length deviates from the average.")
print('Select only those movies made after 2015, whose genre is Drama or Comedy and order the results by year, genre, and duration.')
query = """
    SELECT year, genre, count(*) as num_movies,
        AVG(duration) AS avg_length_per_genre,
        AVG(duration) OVER (partition by year) AS avg_length_per_year
    FROM movies
    WHERE year > 2015
    GROUP BY year, genre
    ORDER BY year, num_movies DESC"""
print(query)
print(pd.read_sql(query, conn))
print()
print()
