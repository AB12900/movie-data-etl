import pandas as pd
import sqlite3

df = pd.read_csv('netflix_titles.csv')

df_cleaned = df.dropna()
df_cleaned.columns = [col.strip().replace(' ', '_').lower() for col in df_cleaned.columns]  # Clean column names

# filter movies only, remove TV Shows
df_movies = df_cleaned[df_cleaned['type'] == 'Movie']

connection = sqlite3.connect('netflix_movies.db')  # Creates database file
df_movies.to_sql('movies', connection, if_exists='replace', index=False)

print("ETL process completed successfully!")

# (Optional) Test: Read data back from database
df_from_db = pd.read_sql_query("SELECT * FROM movies LIMIT 5", connection)
print(df_from_db)

connection.close()
