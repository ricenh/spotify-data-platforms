-- Verify artist dimension stats match actual play counts
-- Should return 0 rows

with actual_stats as (
    select
        artist_id,
        count(*) as actual_plays,
        count(distinct track_id) as actual_tracks
    from {{ ref('fct_plays') }}
    where artist_id is not null
    group by artist_id
)

select
    da.artist_id,
    da.artist_name,
    da.total_plays as recorded_plays,
    a.actual_plays,
    da.total_plays - a.actual_plays as play_diff,
    da.unique_tracks_played as recorded_tracks,
    a.actual_tracks,
    da.unique_tracks_played - a.actual_tracks as track_diff
from {{ ref('dim_artist') }} da
join actual_stats a on da.artist_id = a.artist_id
where da.total_plays != a.actual_plays
   or da.unique_tracks_played != a.actual_tracks