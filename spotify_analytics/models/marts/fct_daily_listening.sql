with plays as (
    select * from {{ ref('fct_plays') }}
)

select
    play_date,
    
    -- Listening volume
    count(*) as total_plays,
    count(distinct track_id) as unique_tracks,
    count(distinct artist_id) as unique_artists,
    sum(duration_minutes) as total_minutes,
    round(avg(duration_minutes)::numeric, 2) as avg_track_length,
    
    -- Music characteristics
    round(avg(energy)::numeric, 3) as avg_energy,
    round(avg(danceability)::numeric, 3) as avg_danceability,
    round(avg(valence)::numeric, 3) as avg_valence,
    round(avg(tempo)::numeric, 1) as avg_tempo,
    
    -- Popularity
    round(avg(track_popularity)::numeric, 1) as avg_track_popularity,
    round(avg(artist_popularity)::numeric, 1) as avg_artist_popularity,
    
    -- Content flags
    sum(case when is_explicit then 1 else 0 end) as explicit_tracks

from plays
group by play_date
order by play_date desc