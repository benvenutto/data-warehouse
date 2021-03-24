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
)
DISTSTYLE EVEN
;''')

staging_events_table_create= ('''
CREATE TABLE staging_events (
    artist              VARCHAR NULL,
    auth                VARCHAR NOT NULL,
    first_name          VARCHAR NULL,
    gender              VARCHAR NULL,
    item_in_session     INT     NOT NULL,
    last_name           VARCHAR NULL,
    length              REAL    NULL,
    level               VARCHAR NOT NULL,
    location            VARCHAR NULL,
    method              VARCHAR NOT NULL,
    page                VARCHAR NOT NULL,
    registration        REAL    NULL,
    session_id          BIGINT  NOT NULL,
    song                VARCHAR NULL,
    status              INT     NOT NULL,
    ts                  BIGINT  NOT NULL,
    user_agent          VARCHAR NULL,
    user_id             BIGINT  NULL
)
DISTSTYLE EVEN
;''')

songplay_table_create = ('''
CREATE TABLE songplay (
    songplay_id         BIGINT  NOT NULL IDENTITY(0, 1) SORTKEY, 
    start_time          TIMESTAMP WITHOUT TIME ZONE, 
    user_id             BIGINT  NOT NULL, 
    level               VARCHAR NOT NULL, 
    song_id             VARCHAR NOT NULL, 
    artist_id           VARCHAR NOT NULL, 
    session_id          BIGINT  NOT NULL, 
    location            VARCHAR NOT NULL, 
    user_agent          VARCHAR NOT NULL
)
DISTSTYLE EVEN
;''')

user_table_create = ('''
CREATE TABLE "user" (
    user_id             BIGINT  NOT NULL    SORTKEY, 
    first_name          VARCHAR NOT NULL, 
    last_name           VARCHAR NOT NULL, 
    gender              VARCHAR NOT NULL, 
    level               VARCHAR NOT NULL
)
DISTSTYLE EVEN
;''')

song_table_create = ('''
CREATE TABLE song (
    song_id             VARCHAR NOT NULL    SORTKEY, 
    title               VARCHAR NOT NULL, 
    artist_id           VARCHAR NOT NULL, 
    year                INT     NOT NULL, 
    duration            REAL    NOT NULL
)
DISTSTYLE EVEN
;''')

artist_table_create = ('''
CREATE TABLE artist (
    artist_id           VARCHAR NOT NULL    SORTKEY, 
    name                VARCHAR NOT NULL, 
    location            VARCHAR NULL, 
    latitude            BIGINT  NULL, 
    longitude           BIGINT  NULL
)
DISTSTYLE EVEN
;''')

time_table_create = ('''
CREATE TABLE time (
    start_time          TIMESTAMP WITHOUT TIME ZONE NOT NULL    SORTKEY,
    hour                INT     NOT NULL,
    day                 INT     NOT NULL,
    week                INT     NOT NULL,
    month               INT     NOT NULL,
    year                INT     NOT NULL,
    weekday             INT     NOT NULL
)
DISTSTYLE EVEN
;''')

# STAGING TABLES

staging_songs_copy = "COPY staging_songs FROM '{}' IAM_ROLE '{}' JSON 'auto' ;"
staging_events_copy = "COPY staging_events FROM '{}' IAM_ROLE '{}' JSON '{}' ;"

# FINAL TABLES

song_table_insert = ('''
INSERT INTO song (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
)
SELECT DISTINCT
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
FROM    staging_songs
;
''')

artist_table_insert = ('''
INSERT INTO artist (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
)
SELECT DISTINCT
    artist_id, 
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM    staging_songs
;
''')

time_table_insert = ('''
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT
    dateadd(milliseconds, ts, '1970-01-01 00:00:00'),
    date_part(hour, dateadd(milliseconds, ts, '1970-01-01 00:00:00')),
    date_part(day, dateadd(milliseconds, ts, '1970-01-01 00:00:00')),
    date_part(week, dateadd(milliseconds, ts, '1970-01-01 00:00:00')),
    date_part(month, dateadd(milliseconds, ts, '1970-01-01 00:00:00')),
    date_part(year, dateadd(milliseconds, ts, '1970-01-01 00:00:00')),
    date_part(dow, dateadd(milliseconds, ts, '1970-01-01 00:00:00'))    
FROM    staging_events
''')

user_table_insert = ('''
INSERT INTO "user" (
    user_id, 
    first_name, 
    last_name, 
    gender,
    level
)
SELECT DISTINCT
    user_id,
    first_name, 
    last_name,
    gender,
    'free' AS level
FROM    staging_events
WHERE   user_id IS NOT NULL
;
''')

user_table_update = ('''
UPDATE  "user" AS u
SET     level = 'paid'
FROM    staging_events se
WHERE   u.user_id = se.user_id
AND     se.level = 'paid'
;
''')

songplay_table_insert = ('''
INSERT INTO songplay (
    start_time, 
    user_id, 
    level, 
    session_id,
    location, 
    user_agent,
    song_id,
    artist_id
    )
SELECT DISTINCT
    dateadd(milliseconds, ts, '1970-01-01 00:00:00'), 
    user_id, 
    level, 
    session_id,
    se.location, 
    user_agent,
    s.song_id,
    a.artist_id
FROM    staging_events se,
        song AS s,
        artist AS a
WHERE   se.status = 200
AND     se.page = 'NextSong'
AND     se.song = s.title
AND     se.artist = a.name
;
''')

# QUERY LISTS

create_table_queries = [staging_songs_table_create, staging_events_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_songs_table_drop, staging_events_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [song_table_insert, artist_table_insert, time_table_insert, user_table_insert, songplay_table_insert]
update_table_queries = [user_table_update]