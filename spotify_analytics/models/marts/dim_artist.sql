with artists as (
    select * from {{ ref('stg_artists') }}
),

artist_stats as (
    select
        artist_id,
        count(*) as total_plays,
        count(distinct track_id) as unique_tracks_played,
        sum(duration_minutes) as total_minutes_played
    from {{ ref('fct_plays') }}
    group by artist_id
)

select
    artists.artist_id,
    artists.artist_name,
    artists.popularity,
    artists.genres,
    artists.has_genres,
    
    -- Play stats
    coalesce(artist_stats.total_plays, 0) as total_plays,
    coalesce(artist_stats.unique_tracks_played, 0) as unique_tracks_played,
    coalesce(artist_stats.total_minutes_played, 0) as total_minutes_played

from artists
left join artist_stats on artists.artist_id = artist_stats.artist_id