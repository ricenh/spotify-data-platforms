with tracks as (
    select * from {{ ref('stg_tracks') }}
),

audio_features as (
    select * from {{ ref('stg_audio_features') }}
),

artists as (
    select * from {{ ref('stg_artists') }}
)

select
    tracks.track_id,
    tracks.track_name,
    tracks.duration_minutes,
    tracks.popularity as track_popularity,
    tracks.is_explicit,
    
    -- Primary artist
    tracks.primary_artist_id,
    artists.artist_name as primary_artist_name,
    
    -- Audio features
    audio_features.danceability,
    audio_features.energy,
    audio_features.energy_level,
    audio_features.tempo,
    audio_features.valence,
    audio_features.mood,
    audio_features.loudness

from tracks
left join artists on tracks.primary_artist_id = artists.artist_id
left join audio_features on tracks.track_id = audio_features.track_id