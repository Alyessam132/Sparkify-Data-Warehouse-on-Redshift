import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = ("""DROP TABLE IF EXISTS staging_events""")
staging_songs_table_drop = ("""DROP TABLE IF EXISTS staging_songs""")
songplay_table_drop = ("""DROP TABLE IF EXISTS songplay""")
user_table_drop = ("""DROP TABLE  IF EXISTS users""")
song_table_drop = ("""DROP TABLE  IF EXISTS songs""")
artist_table_drop = ("""DROP TABLE  IF EXISTS artist""")
time_table_drop = ("""DROP TABLE  IF EXISTS time""")

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
    artist  varchar,
    auth    varchar,
firstName   varchar,
gender       varchar,
itemInSession      int,
lastName    varchar,
length           float4,
level             varchar,
location          varchar,
method            varchar,
page              varchar,
registration       float8,
sessionId          int8,
song              varchar,
status          int,
ts                 int8,
userAgent         varchar,
userId             int
    )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs             int,
artist_id            varchar,
artist_latitude     float4,
artist_longitude    float4,
artist_location     varchar,
artist_name         varchar,
song_id              varchar,
title                varchar,
duration            float4,
year                  int
)
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays 
    (songplay_id BIGINT IDENTITY(1, 1) PRIMARY KEY, 
     start_time timestamp NOT NULL sortkey, 
     user_id int NOT NULL distkey, 
     level varchar, 
     song_id varchar, 
     artist_id varchar, 
     session_id int, 
     location varchar,
     user_agent varchar)diststyle key;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(user_id int PRIMARY KEY sortkey,
 first_name varchar NOT NULL,
 last_name varchar NOT NULL,
 gender varchar NOT NULL,
 level varchar) diststyle all;
 """)

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs 
    (song_id varchar PRIMARY KEY sortkey, 
     title varchar NOT NULL, 
     artist_id varchar NOT NULL distkey, 
     year int, 
     duration float4 NOT NULL)diststyle key;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(artist_id varchar PRIMARY KEY sortkey,
name varchar NOT NULL,
location varchar ,
longitude float4,
latitude float4 )diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(start_time timestamp PRIMARY KEY sortkey,
 hour int,
 day int,
 week int,
 month int,
 year int distkey,
 weekday varchar )diststyle key;
 """)

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {bucket}
iam_role {role}
    region      'us-west-2'
    format       as JSON {path}
    timeformat   as 'epochmillisecs'
""").format(bucket=config.get('S3','LOG_DATA'), path=config.get('S3','LOG_JSONPATH'),role = config.get('IAM_ROLE','ARN'))

staging_songs_copy = ("""
copy staging_songs from {bucket}
iam_role {role}
    region      'us-west-2'
    format       as JSON 'auto'
""").format(bucket=config.get('S3','SONG_DATA'),role = config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + (se.ts/1000 * INTERVAL '1 second'),
se.userId, 
se.level,
ss.song_id,
ss.artist_id,
se.sessionId,
se.location,
se.userAgent
FROM staging_events se
JOIN  staging_songs ss
ON se.artist = ss.artist_name 
and se.song = ss.title
and se.length = ss.duration
and se.page = 'NextSong'
""")

user_table_insert = ("""
insert into users
SELECT DISTINCT(userId) as user_id
,firstName,lastName,gender,level
FROM staging_events
WHERE userId is not NULL
""")

song_table_insert = ("""
insert into songs
SELECT DISTINCT (song_id) as song_id
,title,artist_id,year,duration
FROM staging_songs
""")

artist_table_insert = ("""
insert into artists
SELECT DISTINCT (artist_id)
artist_id,artist_name,artist_location,artist_longitude, artist_latitude
FROM staging_songs
""")

time_table_insert = ("""  
insert into time
SELECT DISTINCT (ts_1),
        extract(hour from ts_1),
        extract(day from ts_1),
        extract(week from ts_1),
        extract(month from ts_1),
        extract(year from ts_1),
        extract(weekday from ts_1)
        FROM (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts_1 FROM staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
