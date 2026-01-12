with plays as (
    select * from {{ ref('stg_recent_plays') }}
),

tracks as (
    select * from {{ ref('stg_tracks') }}
),

artists as (
    select * from {{ ref('stg_artists') }}
),

audio_features as (
    select * from {{ ref('stg_audio_features') }}
)

select
    -- Play info
    plays.played_at,
    plays.play_date,
    plays.play_hour,
    plays.day_of_week,
    
    -- Track info
    plays.track_id,
    tracks.track_name,
    tracks.duration_minutes,
    tracks.popularity as track_popularity,
    tracks.is_explicit,
    
    -- Artist info
    tracks.primary_artist_id as artist_id,
    artists.artist_name,
    artists.popularity as artist_popularity,
    
    -- Audio features
    audio_features.danceability,
    audio_features.energy,
    audio_features.energy_level,
    audio_features.tempo,
    audio_features.valence,
    audio_features.mood,
    audio_features.loudness

from plays
left join tracks on plays.track_id = tracks.track_id
left join artists on tracks.primary_artist_id = artists.artist_id
left join audio_features on plays.track_id = audio_features.track_id