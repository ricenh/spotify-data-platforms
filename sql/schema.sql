CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    name TEXT,
    popularity INT,
    genres TEXT[]
);

CREATE TABLE IF NOT EXISTS tracks (
    track_id TEXT PRIMARY KEY,
    name TEXT,
    duration_ms INT,
    popularity INT,
    explicit BOOLEAN,
    artist_id TEXT REFERENCES artists(artist_id)
);

CREATE TABLE IF NOT EXISTS plays (
    played_at TIMESTAMP,
    track_id TEXT REFERENCES tracks(track_id),
    PRIMARY KEY (played_at, track_id)
);

CREATE TABLE IF NOT EXISTS audio_features (
    track_id TEXT PRIMARY KEY,
    danceability FLOAT,
    energy FLOAT,
    tempo FLOAT,
    valence FLOAT,
    loudness FLOAT
);
