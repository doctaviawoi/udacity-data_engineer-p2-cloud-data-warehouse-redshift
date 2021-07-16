import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Get parameters
IAM_ROLE = config.get('IAM_ROLE', 'ARN')
S3_LOG_DATA = config.get('S3','LOG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA = config.get('S3','SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                    artist         VARCHAR,
                                    auth           VARCHAR,
                                    firstName      VARCHAR,
                                    gender         VARCHAR,
                                    itemInSession  BIGINT,
                                    lastName       VARCHAR,
                                    length         NUMERIC(10,5),
                                    level          VARCHAR,
                                    location       VARCHAR,
                                    method         VARCHAR,
                                    page           VARCHAR,
                                    registration   BIGINT,
                                    sessionId      BIGINT,
                                    song           VARCHAR,
                                    status         INT,
                                    ts             TIMESTAMP,
                                    userAgent      VARCHAR,
                                    userId         BIGINT
                                    );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                    artist_id        VARCHAR,
                                    artist_latitude  NUMERIC(8,5),
                                    artist_longitude NUMERIC(8,5),
                                    artist_location  VARCHAR,
                                    artist_name      VARCHAR,
                                    song_id          VARCHAR,
                                    title            VARCHAR,
                                    duration         NUMERIC(10,5),
                                    year             INT
                                    );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id      BIGINT IDENTITY(0,1),
                                start_timestamp  TIMESTAMP NOT NULL REFERENCES time(timestamp),
                                user_id          BIGINT NOT NULL REFERENCES users(user_id),
                                level            VARCHAR(4) NOT NULL,
                                song_id          VARCHAR NOT NULL REFERENCES songs(song_id),
                                artist_id        VARCHAR NOT NULL REFERENCES artists(artist_id) DISTKEY,
                                session_id       BIGINT NOT NULL,
                                location         VARCHAR NOT NULL,
                                user_agent       VARCHAR(100) NOT NULL,
                                PRIMARY KEY (songplay_id)
                                )
                            SORTKEY(start_timestamp, user_id);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                user_id     BIGINT PRIMARY KEY SORTKEY,
                                first_name  VARCHAR NOT NULL,
                                last_name   VARCHAR NOT NULL,
                                gender      CHAR(1) NOT NULL,
                                level       VARCHAR(4) NOT NULL
                                ) DISTSTYLE ALL;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                song_id    VARCHAR PRIMARY KEY,
                                title      VARCHAR NOT NULL,
                                artist_id  VARCHAR NOT NULL REFERENCES artists,
                                year       INT NOT NULL,
                                duration   NUMERIC(10,5) NOT NULL
                                ) DISTSTYLE ALL
                                SORTKEY(artist_id, song_id);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                artist_id  VARCHAR PRIMARY KEY DISTKEY SORTKEY,
                                name       VARCHAR NOT NULL,
                                location   VARCHAR,
                                latitude   NUMERIC(8,5),
                                longitude  NUMERIC(8,5)
                                );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                timestamp   TIMESTAMP PRIMARY KEY SORTKEY,
                                start_time  TIME NOT NULL,
                                hour        INT NOT NULL,
                                day         INT NOT NULL,
                                week        INT NOT NULL,
                                month       INT NOT NULL,
                                year        INT NOT NULL,
                                weekday     INT NOT NULL
                                );
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events 
                            FROM {}
                            credentials 'aws_iam_role={}'
                            region 'us-west-2'
                            FORMAT AS JSON {}
                            TIMEFORMAT 'epochmillisecs';                          
""").format(S3_LOG_DATA, IAM_ROLE, S3_LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs
                            FROM {}
                            credentials 'aws_iam_role={}'
                            region 'us-west-2'
                            FORMAT AS JSON 'auto';    
""").format(S3_SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_timestamp, user_id, level, song_id, artist_id, session_id, location, user_agent)
                             SELECT TO_CHAR(L.ts, 'YYYY-MM-DD HH:MI:SS')::TIMESTAMP      AS start_timestamp,
                                    L.userId                                             AS user_id, 
                                    L.level                                              AS level,
                                    S.song_id                                            AS song_id,
                                    S.artist_id                                          AS artist_id,
                                    L.sessionId                                          AS session_id,
                                    L.location                                           AS location,
                                    L.userAgent::VARCHAR(100)                            AS user_agent
                             FROM (SELECT artist, length, level, location, page, sessionId, song, ts, userAgent, userId FROM staging_events WHERE page='NextSong') AS L
                             LEFT JOIN staging_songs S 
                                 ON L.artist = S.artist_name AND L.song = S.title AND L.length = S.duration
                             WHERE S.song_id IS NOT NULL 
                             AND S.artist_id IS NOT NULL
                             ORDER BY start_timestamp;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT A.userId    AS user_id, 
                               L.firstName AS first_name, 
                               L.lastName  AS last_name, 
                               L.gender, 
                               L.level
                        FROM (SELECT userId, MAX(ts) AS recent FROM staging_events GROUP BY userId HAVING userId IS NOT NULL) A 
                        LEFT JOIN staging_events L 
                            ON A.userId = L.userId 
                            AND A.recent = L.ts
                        ORDER BY user_id;
""")


song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT artist_id, 
                                 MIN(artist_name)      AS name, 
                                 MIN(artist_location)  AS location, 
                                 MIN(artist_latitude)  AS latitude, 
                                 MIN(artist_longitude) AS longitude
                          FROM staging_songs
                          GROUP BY artist_id
                          HAVING artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO time (timestamp, start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT start_timestamp              AS timestamp, 
                               start_timestamp::TIME                 AS start_time, 
                               EXTRACT(HOUR FROM start_timestamp)    AS hour,
                               EXTRACT(DAY FROM start_timestamp)     AS day,
                               EXTRACT(WEEK FROM start_timestamp)    AS week,
                               EXTRACT(MONTH FROM start_timestamp)   AS month,
                               EXTRACT(YEAR FROM start_timestamp)    AS year,
                               TO_CHAR(start_timestamp, 'ID')::INT   AS weekday
                        FROM (SELECT start_timestamp FROM songplays) t
                        ORDER BY timestamp;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,  time_table_insert]