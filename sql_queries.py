import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events(
    artist_id VARCHAR ,
    auth VARCHAR ,
    first_name VARCHAR ,
    gender VARCHAR ,
    item_in_session SMALLINT ,
    last_name VARCHAR ,
    length FLOAT ,
    level VARCHAR ,
    location VARCHAR ,
    method VARCHAR ,
    page VARCHAR ,
    registration VARCHAR ,
    session_id VARCHAR ,
    song_title VARCHAR ,
    status VARCHAR ,
    ts BIGINT ,
    user_agent VARCHAR ,
    user_id VARCHAR 
    )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
     num_songs INT NOT NULL,
     artist_id VARCHAR NOT NULL,
     artist_latitude decimal,
     artist_longitude decimal ,
     artist_location VARCHAR ,
     artist_name VARCHAR NOT NULL,
     song_id VARCHAR,
     title VARCHAR NOT NULL,
     duration NUMERIC,
     year INT NOT NULL

)

""")

songplay_table_create = ("""CREATE TABLE songplay(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY ,
    start_time TIMESTAMP NOT NULL SORTKEY ,
    user_id VARCHAR NOT NULL DISTKEY,
    level VARCHAR ,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id VARCHAR ,
    location VARCHAR,
    user_agent VARCHAR  
)
""")

user_table_create = ("""CREATE TABLE users(
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR  ,
    last_name VARCHAR  ,
    gender VARCHAR ,
    level VARCHAR  
)
DISTSTYLE AUTO
""")

song_table_create = ("""CREATE TABLE song (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL DISTKEY,
    year INT4,
    duration FLOAT  
)
""")

artist_table_create = ("""CREATE TABLE artist (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT  
)
DISTSTYLE AUTO;
""")

time_table_create = ("""CREATE TABLE time (
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT  
)
DISTSTYLE AUTO;
""")

# STAGING TABLES

staging_events_copy = (""" COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON{} REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = (""" COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto' REGION 'us-west-2' TRUNCATECOLUMNS;
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplay (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent )
SELECT
  TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time,
  e.user_id,
  e.level,
  s.song_id,
  s.artist_id,
  e.session_id,
  e.location,
  e.user_agent  
FROM staging_events e
LEFT JOIN staging_songs s 
ON (e.song_title = s.title)
AND (e.artist_id = s.artist_id)
WHERE page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users(user_id,first_name ,last_name,gender,level)
WITH unique_user AS (
SELECT
  user_id,
  first_name,
  last_name,
  gender, 
  level,
  ROW_NUMBER() over(partition by user_id order by ts desc) as index
  FROM staging_events 
  )
 SELECT
  user_id,
  first_name,
  lastname,
  gender, 
  level
  FROM unique_user
 
""")

song_table_insert = ("""INSERT INTO song(song_id,title,artist_id,year,duration)
SELECT
s.song_id,s.title,s.artist_id,s.year,s.duration
FROM staging_songs s
""")

artist_table_insert = (""" INSERT INTO artist (artist_id,name,location,latitude,longitude)
SELECT
s.artist_id,s.artist_name,s.artist_location,s.lattitude,s.longitude
FROM staging_songs s
""")

time_table_insert = (""" INSERT INTO time (start_time,hour, day, week, month, year, weekday)
WITH time_parse AS (
SELECT
 DISTINCT TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time
  FROM staging_events
  )
SELECT 
  EXTRACT(hour FROM start_time ) AS hour,
   EXTRACT(day FROM start_time ) AS day,
    EXTRACT(week FROM start_time ) AS week,
     EXTRACT(month FROM start_time ) AS month,
      EXTRACT(year FROM start_time ) AS year,
       EXTRACT(dow FROM start_time ) AS weekday
 FROM time_parse
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
