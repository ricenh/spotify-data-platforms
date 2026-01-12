-- Verify that daily aggregations match the detail-level play counts
-- Should return 0 rows if aggregations are correct

with daily_from_plays as (
    select
        play_date,
        count(*) as play_count,
        sum(duration_minutes) as total_minutes
    from {{ ref('fct_plays') }}
    group by play_date
),

daily_from_mart as (
    select
        play_date,
        total_plays,
        total_minutes
    from {{ ref('fct_daily_listening') }}
)

select
    p.play_date,
    p.play_count as expected_plays,
    m.total_plays as actual_plays,
    p.play_count - m.total_plays as play_count_diff,
    round((p.total_minutes - m.total_minutes)::numeric, 2) as minutes_diff
from daily_from_plays p
join daily_from_mart m on p.play_date = m.play_date
where p.play_count != m.total_plays
   or abs(p.total_minutes - m.total_minutes) > 0.1