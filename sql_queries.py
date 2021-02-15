import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1) PRIMARY KEY,
    artist varchar,
    auth varchar,
    first_name varchar,
    gender varchar,
    item_in_session integer,
    last_name varchar,
    length double precision,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    session_id bigint,
    song varchar,
    status integer,
    ts varchar,
    user_agent text,
    user_id varchar)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
     song_id varchar PRIMARY KEY,
     num_songs integer,
     title varchar,
     artist_name varchar,
     artist_latitude double precision,
     year int,
     duration double precision,
     artist_id varchar,
     artist_longitude double precision,
     artist_location varchar))
""")

songplay_table_create = ("""
CREATE TABLE songplay (
     songplay_id   int IDENTITY(0,1) PRIMARY KEY,
     start_time    timestamp,
     user_id       varchar,
     level         varchar,
     song_id       varchar,
     artist_id     varchar,
     session_id    bigint,
     location      varchar,
     user_agent    text)
""")

user_table_create = ("""
CREATE TABLE user (
     user_id     varchar PRIMARY KEY,
     first_name  varchar,
     last_name   varchar,
     gender      varchar,
     level       varchar )
""")

song_table_create = ("""
CREATE TABLE song(
     song_id varchar PRIMARY KEY,
     title varchar,
     artist_id varchar,
     year integer,
     duration double precision)
""")

artist_table_create = ("""
CREATE TABLE artist (
     artist_id varchar PRIMARY KEY,
     name varchar,
     location varchar,
     lattitude double precision,
     longitude double precision)
""")

time_table_create = ("""
CREATE TABLE time (
     start_time timestamp PRIMARY KEY,
     hour integer,
     day integer,
     week  integer,
     month integer,
     year integer,
     weekday integer)
""")

# STAGING TABLES

staging_events_copy = (""" COPY staging_events 
                           FROM '{}' credentials
                           'aws_iam_role={}'
                           region 'us-west-2' 
                           COMPUPDATE OFF STATUPDATE OFF
                           JSON '{}'
                        """).format(config.get('S3','LOG_DATA'),
                                    config.get('IAM_ROLE', 'ARN'),
                                    config.get('S3','LOG_JSONPATH'))


staging_songs_copy = (""" COPY staging_songs
                            FROM {} credentials
                            \'aws_iam_role={}\' JSON 'auto' 
                            truncatecolumns compupdate off region \'us-west-2\';
                            """).format(config.get("S3","SONG_DATA"),
                                        config.get("IAM_ROLE", "ARN"))
# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT   
    TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, 
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
FROM staging_events e
JOIN staging_songs s
     ON (e.artist = s.artist_name)
     AND (e.song = s.title)
     AND (e.length = s.duration)
     WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO user (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT
    artist_id,
    name,
    location,
    lattitude,
    longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    start_time,
    EXTRACT(hour FROM start_time) as hour ,
    EXTRACT(day FROM start_time) as day,
    EXTRACT(week FROM start_time) as week,
    EXTRACT(month FROM start_time) as ,month,
    EXTRACT(year FROM start_time) as year,
    EXTRACT(dayofweek FROM start_time) as weekday
FROM songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
