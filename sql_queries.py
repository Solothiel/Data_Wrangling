# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id SERIAL PRIMARY KEY,
        start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
        userId INTEGER NOT NULL REFERENCES users(userId),
        level VARCHAR,
        song_id VARCHAR REFERENCES songs(song_id),
        artist_id VARCHAR REFERENCES artists(artist_id),
        sessionId INTEGER,
        location VARCHAR,
        userAgent TEXT
     );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        userId INTEGER PRIMARY KEY,
        firstName VARCHAR,
        lastName VARCHAR,
        gender CHAR(1),
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INTEGER,
        duration FLOAT NOT NULL
     );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR PRIMARY KEY,
        artist_name VARCHAR NOT NULL,
        artist_location VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (
         start_time, userId, level, song_id, artist_id, sessionId, location, userAgent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);

""")

user_table_insert = ("""
    INSERT INTO users (userId, firstName, lastName, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (userId)
    DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (song_id) DO NOTHING;

""")

artist_table_insert = ("""        
     INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
     VALUES (%s, %s, %s, %s, %s)
     ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT songs.song_id, artists.artist_id
    FROM songs
    JOIN artists ON songs.artist_id = artists.artist_id
    WHERE songs.title = %s
    AND artists.artist_name = %s
    AND songs.duration = %s;
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,
                        songplay_table_create, ]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]