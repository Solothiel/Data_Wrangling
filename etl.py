"""
ETL pipeline for Sparkify Database

This script take data from song data and log data and then transforms them into the proper format.
The information is then loaded into a PostgreSQL database.

"""

import os
import glob
import psycopg2
import pandas as pd
import json
from sql_queries import *


def process_song_file(cur, filepath):
    """
        Takes a song file and insert data into songs and artist tables.

    Parameters
        cur      : psycopg2.cursor  (cursor of the database connection.)
        filepath : str  (filepath of the JSON song file.)

    Actions:
       Reads   : Song data is read from the JSON file.
       Inserts : Records are inserted into 'songs' and 'artists' tables.
     """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location",
                      "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        Takes a song file and insert data into songs and data tables.
    Params
        cur      : psycopg2.cursor  (cursor of the database connection)
        filepath : str  (filepath of the JSON log file.)

    Actions:
       Reads    : Log file is read from a JSON.
       Filters  : DataFrame is filtered by the "page" column that has the value "NextSong".
       Converts : Timestamps are converted from Unix Timestamp to DateTime and inserted into "time" table.
       Extracts : User information  is extracted and inserts into 'users' tables.
       Gathers  : Takes song_id, artist_id and inserts records in the 'songplays' table.

     """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, row.tolist())

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]].drop_duplicates(subset=["userId"])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row.tolist())

    # insert songplay records
    for index, row in df.iterrows():
        start_time = pd.to_datetime(row.ts, unit="ms")
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = (
            start_time,
            row['userId'],
            row['level'],
            song_id,
            artist_id,
            row['sessionId'],
            row['location'],
            row['userAgent']
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        Process all JSON files.

    Parameters
        cur      : psycopg2.cursor  (cursor of the database connection.)
        filepath : str  (filepath of the JSON log file.)
        conn     : psycopg2.connection (Active connection to the database.)
        func     : function (function that is used to process each single file.)

    Actions:
       Iterates : Iterates or looks through all the JSON files in the given filepath.
       Applies  : Applies the specified function process.
       Commits  : Commits or saves each transaction to the database.

     """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    """
        This is the main, which is the starting point.

    Actions:
       Connects  : Connects to the Sparkify database (PostgreSQL).
       Processes : Processes all song and log files.
       Closes    : Ends the database connection.

     """

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
