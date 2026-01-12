-- Verify no duplicate plays (same track at same timestamp)
-- Should return 0 rows

select
    played_at,
    track_id,
    count(*) as duplicate_count
from {{ ref('fct_plays') }}
group by played_at, track_id
having count(*) > 1