import os
import glob
from datetime import datetime

import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Function processes a single file of songs logs and loads into postgres table using insert sql query
    for song and artist data.

    :param cur: psycop2 cursor
    :param filepath: path to main folder of songs
    :return: None
    '''
    # open song file
    song_df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = song_df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = song_df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Function processes a single file of play logs and loads into postgres table using insert sql query
    for time, user and song played data.

    :param cur:
    :param filepath:
    :return: None
    '''
    # open log file
    log_df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    log_df = log_df[log_df['page'] == 'NextSong']

    # convert timestamp column to datetime
    log_df['ts'] = log_df['ts'].apply(lambda x: datetime.fromtimestamp(x / 1000.0))
    
    # insert time data records
    log_df['hour'] = log_df['ts'].apply(lambda x: x.hour)
    log_df['day'] = log_df['ts'].apply(lambda x: x.day)
    log_df['week'] = log_df['ts'].apply(lambda x: x.isocalendar()[1])
    log_df['month'] = log_df['ts'].apply(lambda x: x.month)
    log_df['year'] = log_df['ts'].apply(lambda x: x.year)
    log_df['weekday'] = log_df['ts'].apply(lambda x: x.weekday())

    column_labels = ['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = log_df[column_labels]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = log_df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))

    # insert songplay records
    for index, row in log_df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, [row.song, row.artist, row.length])
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(song_play_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Gets all files within filepath directory and iterates over each for specific log files
    scripts functions to commit into database.

    :param cur: psycop cursor
    :param conn: connection to database
    :param filepath: single log filepath
    :param func: SQL script to use for inserting into database
    :return: None
    '''
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    Connects to postgres db and processes collection of log files in directories and processing function.
    Closes connection to database
    :return: None
    '''
    conn = psycopg2.connect(database='sparkifydb', user='cta_admin', password="chicago", \
                        host="127.0.0.1", port="5432", sslmode="disable", gssencmode="disable")
    cur = conn.cursor()

    song_files_path = "data\\song_data"
    process_data(cur, conn, filepath=song_files_path, func=process_song_file)

    log_files_path = "data\\log_data"
    process_data(cur, conn, filepath=log_files_path, func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()