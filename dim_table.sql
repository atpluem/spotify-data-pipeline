-- CREATE DIMENSIONAL TABLE
CREATE OR REPLACE TABLE SPOTIFY.SPOTIFY_TRACK.dim_album 
AS (SELECT
        album_id,
        album_name,
        release_date,
        total_track,
        url
    FROM SPOTIFY.SPOTIFY_TRACK.ALBUM);

CREATE OR REPLACE TABLE SPOTIFY.SPOTIFY_TRACK.dim_artist
AS (SELECT
        artist_id,
        artist_name,
        external_url
    FROM SPOTIFY.SPOTIFY_TRACK.ARTIST);

CREATE OR REPLACE TABLE SPOTIFY.SPOTIFY_TRACK.dim_song
AS (SELECT
        song_id,
        song_name,
        duration_ms,
        url,
        song_added
    FROM SPOTIFY.SPOTIFY_TRACK.SONG);

-- CREATE FACT TABLE
CREATE OR REPLACE TABLE SPOTIFY.SPOTIFY_TRACK.fact_track
AS (SELECT
        ab.album_id,
        at.artist_id,
        s.song_id,
        s.popularity
    FROM SPOTIFY.SPOTIFY_TRACK.SONG s
    JOIN SPOTIFY.SPOTIFY_TRACK.ALBUM ab ON s.album_id = ab.album_id
    JOIN SPOTIFY.SPOTIFY_TRACK.ARTIST at ON s.artist_id = at.artist_id);