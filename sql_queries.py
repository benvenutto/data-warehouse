import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE staging_events;"
staging_songs_table_drop = "DROP TABLE staging_songs;"
songplay_table_drop = "DROP TABLE songplay;"
user_table_drop = "DROP TABLE user;"
song_table_drop = "DROP TABLE song;"
artist_table_drop = "DROP TABLE artist;"
time_table_drop = "DROP TABLE time;"

# CREATE TABLES

staging_songs_table_create = ("""
DROP TABLE IF EXISTS staging_songs;
CREATE TABLE staging_songs (
    num_songs  BIGINT NOT NULL,
    artist_id  VARCHAR NOT NULL,
    artist_latitude  BIGINT NULL,
    artist_longitude  BIGINT NULL,
    artist_location  VARCHAR NOT NULL,
    artist_name  VARCHAR NOT NULL,
    song_id  VARCHAR NOT NULL,
    title  VARCHAR NOT NULL,
    duration  REAL NOT NULL,
    year  BIGINT NOT NULL
);
""")


staging_events_table_create= ("""
DROP TABLE IF EXISTS staging_events;
CREATE TABLE staging_events (
    artist  VARCHAR NOT NULL,
    auth  VARCHAR NOT NULL,
    first_name  VARCHAR NOT NULL,
    gender  VARCHAR NOT NULL,
    item_in_session  BIGINT NOT NULL,
    last_name  VARCHAR NOT NULL,
    length  REAL NULL,
    level  VARCHAR NOT NULL,
    location  REAL NOT NULL,
    method  VARCHAR NOT NULL,
    page  VARCHAR NOT NULL,
    registration  REAL NOT NULL,
    session_id  BIGINT NOT NULL,
    song  VARCHAR NULL,
    status  BIGINT NOT NULL,
    ts  BIGINT NOT NULL,
    user_agent  VARCHAR NOT NULL,
    user_id  BIGINT NOT NULL
);
""")

songplay_table_create = ("""
DROP TABLE IF EXISTS songplay;
CREATE TABLE songplay (
);
""")

user_table_create = ("""
DROP TABLE IF EXISTS user;
CREATE TABLE user (
);
""")

song_table_create = ("""
DROP TABLE IF EXISTS song;
CREATE TABLE song (
);
""")

artist_table_create = ("""
DROP TABLE IF EXISTS artist;
CREATE TABLE artist (
);
""")

time_table_create = ("""
DROP TABLE IF EXISTS time;
CREATE TABLE time (
);
""")

# STAGING TABLES

staging_songs_copy = ("""
""")


staging_events_copy = ("""
COPY staging_events 
FROM '{}'
CREDENTIALS '{}'
DELEMITER '{}'
TIMEFORMAT '{}'
REGION '{}';
""")

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]