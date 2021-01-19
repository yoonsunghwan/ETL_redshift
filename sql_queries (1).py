import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_songs_table_create= ("""
                                  CREATE TABLE  staging_songs (
                                      num_songs int,
                                      artist_id varchar, 
                                      artist_latitude DECIMAL(12, 5), 
                                      artist_longitude DECIMAL(12, 5),
                                      artist_location varchar, 
                                      artist_name varchar,
                                      song_id varchar, 
                                      title varchar, 
                                      duration float,
                                      year int
                                  )


""")

staging_events_table_create = ("""
                                   CREATE TABLE staging_events (
                                      artist varchar,
                                      auth varchar,
                                      firstName varchar,
                                      gender varchar,
                                      itemInSession int,
                                      lastName varchar,
                                      length  DECIMAL(12, 5),
                                      level varchar,
                                      location varchar,
                                      method varchar,
                                      page varchar,
                                      registration float,
                                      sessionId int,
                                      song varchar,
                                      status int,
                                      ts varchar,
                                      userAgent varchar,
                                      userId int
                                      )
""")

user_table_create = ("""
                                CREATE TABLE users (
                                        user_id int NOT NULL PRIMARY KEY sortkey,
                                        first_name varchar,
                                        last_name varchar,
                                        gender varchar,
                                        level varchar
                                        )
""")


song_table_create = ("""
                                CREATE TABLE songs (
                                        song_id varchar PRIMARY KEY  NOT NULL sortkey,
                                        title varchar,
                                        artist_id varchar,
                                        year int,
                                        duration float 
                                        )
""")



artist_table_create = ("""
                                CREATE TABLE  artists (
                                        artist_id varchar  NOT NULL PRIMARY KEY  sortkey,
                                        name varchar,
                                        location varchar,
                                        latitude float,
                                        longitude float
                                        )
""")

time_table_create = ("""
                                CREATE TABLE time (
                                        start_time timestamp PRIMARY KEY  NOT NULL sortkey distkey,
                                        hour int,
                                        day int,
                                        week int,
                                        month int,
                                        year int,
                                        weekday int
                                        )
""")
songplay_table_create = (""" 
                                  CREATE TABLE songplays (
                                        songplay_id int IDENTITY(0,1) PRIMARY KEY NOT NULL,
                                        start_time timestamp sortkey distkey,
                                        user_id int,
                                        level varchar,
                                        song_id varchar,
                                        artist_id varchar,
                                        session_id int,
                                        location varchar,
                                        user_agent varchar,
                                        FOREIGN KEY (user_id) REFERENCES users (user_id),
                                        FOREIGN KEY (song_id) REFERENCES songs (song_id),
                                        FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
                                        FOREIGN KEY (start_time) REFERENCES time (start_time)
                                        )
""" )

# STAGING TABLES
staging_events_copy = ("""
                                COPY staging_events 
                                    FROM {} 
                                    iam_role '{}'
                                    region 'us-west-2'
                                    compupdate off statupdate off
                                    FORMAT AS JSON {} 
                                    timeformat 'epochmillisecs'
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
                                COPY staging_events 
                                    FROM {} 
                                    iam_role '{}'
                                    region 'us-west-2'
                                    compupdate off statupdate off
                                    FORMAT AS JSON 'auto'
                                    timeformat 'epochmillisecs'

""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = (""" 
                                INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                                        SELECT DISTINCT se.ts, 
                                            se.userId, 
                                            se.level, 
                                            ss.song_id, 
                                            ss.artist_id, 
                                            se.sessionId, 
                                            se.location, 
                                            se.userAgent
                                        FROM staging_events se 
                                        INNER JOIN staging_songs ss 
                                            ON se.song = ss.title AND se.artist = ss.artist_name
                                        WHERE se.page = 'NextSong'


            
""")

user_table_insert = ("""
                                INSERT INTO users (user_id, first_name, last_name, gender,level)
                                    SELECT  DISTINCT(userId)    AS user_id,
                                            firstName           AS first_name,
                                            lastName            AS last_name,
                                            gender,
                                            level
                                    FROM staging_events
                                    WHERE user_id IS NOT NULL
                                    AND page  =  'NextSong'
                                """)

song_table_insert = ("""
                                INSERT INTO songs (song_id, title, artist_id, year, duration)
                                    SELECT DISTINCT(song_id) as song_id,
                                           title,
                                           artist_id,
                                           year,
                                           duration
                                    FROM staging_songs
                                    WHERE song_id IS NOT NULL

""")

artist_table_insert = ("""
                                INSERT INTO artists (artist_id, name, location, latitude, longitude)
                                    SELECT DISTINCT(artist_id) as artist_id,
                                           artist_name as name,
                                           artist_location as location,
                                           artist_latitude as latitude,
                                           artist_longitude as longitude
                                    FROM staging_songs
                                    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
                                INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                                    SELECT DISTINCT(ts) as start_time
                                            EXTRACT(hour FROM start_time)       AS hour,
                                            EXTRACT(day FROM start_time)        AS day,
                                            EXTRACT(week FROM start_time)       AS week,
                                            EXTRACT(month FROM start_time)      AS month,
                                            EXTRACT(year FROM start_time)       AS year,
                                            EXTRACT(dayofweek FROM start_time)  as weekday
                                    FROM staging_events
                                    WHERE page = 'NextSong'

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
