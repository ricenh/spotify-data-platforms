-- Verify all plays successfully join to dim_track
-- Should return 0 rows

select
    fp.track_id,
    count(*) as play_count
from {{ ref('fct_plays') }} fp
left join {{ ref('dim_track') }} dt on fp.track_id = dt.track_id
where dt.track_id is null
group by fp.track_id