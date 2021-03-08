# DROP TABLES added 'IF EXISTS'
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time_table"
user_table_drop = "DROP TABLE IF EXISTSusers"
song_play_table_drop = "DROP TABLE IF EXISTS song_plays"

# CREATE TABLES
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id VARCHAR PRIMARY KEY, 
title VARCHAR, 
artist_id VARCHAR NOT NULL, 
year INT,
duration DECIMAL)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id VARCHAR PRIMARY KEY, 
artist_name VARCHAR , 
artist_location VARCHAR , 
artist_latitude DECIMAL, 
artist_longitude DECIMAL)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table(
ts TIMESTAMP PRIMARY KEY, 
hour INT, 
day INT, 
week INT, 
month INT, 
year INT, 
weekday INT)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id INT PRIMARY KEY, 
firstName VARCHAR, 
lastName VARCHAR, 
gender CHAR, 
level VARCHAR NOT NULL)
""")

# Include serial auto increment id for song play
# timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent
# Fact Table
song_play_table_create = ("""
CREATE TABLE IF NOT EXISTS song_plays(
song_play_id SERIAL PRIMARY KEY, 
ts TIMESTAMP NOT NULL,
user_id INT NOT NULL, 
level VARCHAR NOT NULL,
song_id VARCHAR,
artist_id VARCHAR, 
session_id INT, 
location VARCHAR,
user_agent VARCHAR )
""")

# INSERT RECORDS
artist_table_insert = ("""
INSERT INTO artists(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(artist_id)
DO NOTHING;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(song_id)
DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time_table(ts, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(ts)
DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users(user_id, firstName, lastName, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(user_id)
DO UPDATE 
SET level = EXCLUDED.level;
""")


song_play_table_insert = ("""
INSERT INTO song_plays(ts, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")


# FIND SONGS
song_select = ("""
SELECT song_id, artists.artist_id
FROM songs JOIN artists 
ON songs.artist_id = artists.artist_id
WHERE title= %s AND artists.artist_name = %s AND songs.duration = %s
""")
#
# QUERY LISTS
create_table_queries = [song_table_create, song_play_table_create, artist_table_create, time_table_create, user_table_create]
drop_table_queries = [song_table_drop, artist_table_drop, time_table_drop, user_table_drop, song_play_table_drop]
