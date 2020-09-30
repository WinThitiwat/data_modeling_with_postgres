import os
import glob
import psycopg2
import pandas as pd
import config
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process songs files and load records into database.
    
    Parameters:
        cur: psycopg2.extensions.cursor
            Database cursor reference
        filepath: str
            complete full path of a file to be loaded
    Returns:
        None
    """
    # open song file
    # read as Series type as each json contains single record
    df = pd.DataFrame([pd.read_json(filepath, typ='series',convert_dates=False)])
    
    for _, row in df.iterrows():   
        # num_songs, artist_id, artist_latitude, artist_longitude,artist_location, artist_name, song_id, title, duration, year = data
        # insert artist record
        artist_data = (row.artist_id, row.artist_name, row.artist_location, row.artist_latitude, row.artist_longitude)
        cur.execute(artist_table_insert, artist_data)

        # insert song record
        song_data = (row.song_id, row.title, row.artist_id, row.year, row.duration)
        cur.execute(song_table_insert, song_data)
    
        


def process_log_file(cur, filepath):
    """
    Process on log data to create `time` and `users` dimensional tables and `songplays` fact table.

    Parameters:
        cur: psycopg2.extensions.cursor
            Database cursor reference
        filepath: str
            Complete full path of a file to be loaded
    Returns:
        None
    """
    # open log file
    df = pd.DataFrame(pd.read_json(filepath, lines=True))

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = df['ts'].apply(pd.to_datetime, unit='ms')

    # insert time data records
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.day_name())

    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for _, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for _, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        start_time = pd.to_datetime(row.ts, unit='ms')
        # insert songplay record
        songplay_data = (start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process all data located in the given filepath and load to database based on given function.

    Parameters:
        cur: psycopg2.extensions.cursor
            Database cursor reference
        conn: psycopg2.extensions.connection
            Database connection object
        filepath: str
            Complete full path of a file to be loaded
        func: function
            A function to process data
    Returns:
        None
    """
    # get all files matching extension from directory
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
    conn = psycopg2.connect(f"host=127.0.0.1 dbname=sparkifydb user={config.DB_USERNAME} password={config.DB_PASSWORD}")
    cur = conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()