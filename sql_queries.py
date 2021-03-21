import configparser
import boto3

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_songs_table_drop = 'DROP TABLE IF EXISTS staging_songs;'
staging_events_table_drop = 'DROP TABLE IF EXISTS staging_events;'
songplay_table_drop = 'DROP TABLE IF EXISTS songplay;'
user_table_drop = 'DROP TABLE IF EXISTS "user";'
song_table_drop = 'DROP TABLE IF EXISTS song;'
artist_table_drop = 'DROP TABLE IF EXISTS artist;'
time_table_drop = 'DROP TABLE IF EXISTS time;'

# CREATE TABLES

staging_songs_table_create = ('''
CREATE TABLE staging_songs (
    song_id             VARCHAR NOT NULL,
    num_songs           INT     NOT NULL,
    artist_id           VARCHAR NOT NULL,
    artist_latitude     REAL    NULL,
    artist_longitude    REAL    NULL,
    artist_location     VARCHAR NULL,
    artist_name         VARCHAR NOT NULL,
    title               VARCHAR NOT NULL,
    duration            REAL    NOT NULL,
    year                INT     NOT NULL
);
''')

staging_events_table_create= ('''
CREATE TABLE staging_events (
    artist              VARCHAR NULL,
    auth                VARCHAR NOT NULL,
    first_name          VARCHAR NOT NULL,
    gender              VARCHAR NOT NULL,
    item_in_session     INT     NOT NULL,
    last_name           VARCHAR NOT NULL,
    length              REAL    NULL,
    level               VARCHAR NOT NULL,
    location            VARCHAR NOT NULL,
    method              VARCHAR NOT NULL,
    page                VARCHAR NOT NULL,
    registration        REAL    NOT NULL,
    session_id          BIGINT  NOT NULL,
    song                VARCHAR NULL,
    status              INT     NOT NULL,
    ts                  BIGINT  NOT NULL,
    user_agent          VARCHAR NOT NULL,
    user_id             BIGINT  NOT NULL
);
''')

songplay_table_create = ('''
CREATE TABLE songplay (
    songplay_id         BIGINT  NOT NULL IDENTITY(0, 1), 
    start_time          BIGINT  NOT NULL, 
    user_id             BIGINT  NOT NULL, 
    level               VARCHAR NOT NULL, 
    song_id             BIGINT  NOT NULL, 
    artist_id           BIGINT  NOT NULL, 
    session_id          BIGINT  NOT NULL, 
    location            VARCHAR NOT NULL, 
    user_agent          VARCHAR NOT NULL
);
''')

user_table_create = ('''
CREATE TABLE "user" (
    user_id             BIGINT  NOT NULL, 
    first_name          VARCHAR NOT NULL, 
    last_name           VARCHAR NOT NULL, 
    gender              VARCHAR NOT NULL, 
    level               VARCHAR NOT NULL
);
''')

song_table_create = ('''
CREATE TABLE song (
    song_id             BIGINT  NOT NULL, 
    title               VARCHAR NOT NULL, 
    artist_id           BIGINT  NOT NULL, 
    year                INT     NOT NULL, 
    duration            REAL    NOT NULL
);
''')

artist_table_create = ('''
CREATE TABLE artist (
    artist_id           BIGINT  NOT NULL, 
    name                VARCHAR NOT NULL, 
    location            VARCHAR NOT NULL, 
    latitude            BIGINT  NULL, 
    longitude           BIGINT  NULL
);
''')

time_table_create = ('''
CREATE TABLE time (
    start_time          TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    hour                INT     NOT NULL,
    day                 INT     NOT NULL,
    week                INT     NOT NULL,
    month               INT     NOT NULL,
    year                INT     NOT NULL,
    weekday             INT     NOT NULL
);
''')

# STAGING TABLES

staging_songs_copy = "COPY staging_songs FROM '{}' IAM_ROLE '{}' JSON 'auto' ;"
staging_events_copy = "COPY staging_events FROM '{}' IAM_ROLE '{}' JSON '{}' ;"

# FINAL TABLES

songplay_table_insert = ('''
INSERT INTO songplays (
    start_time, 
    user_id, 
    level, 
    session_id, 
    location, 
    user_agent
    )
SELECT
    ts, 
    user_id, 
    level, 
    session_id, 
    location, 
    user_agent
;
''')

user_table_insert = ('''
INSERT INTO users (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
) VALUES(
    %s, %s, %s, %s, %s
)
ON CONFLICT (user_id) DO UPDATE 
SET level = excluded.level
;
''')

song_table_insert = ('''
INSERT INTO songs (
    song_id, title, artist_id, year, duration
) VALUES (
    %s, %s, %s, %s, %s
)
ON CONFLICT (song_id) DO NOTHING
;
''')

artist_table_insert = ('''
INSERT INTO artists (
    artist_id, name, location, latitude, longitude
) VALUES (
    %s, %s, %s, %s, %s
)
ON CONFLICT (artist_id) DO NOTHING
;
''')

time_table_insert = ('''
''')

# QUERY LISTS

create_table_queries = [staging_songs_table_create, staging_events_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_songs_table_drop, staging_events_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
