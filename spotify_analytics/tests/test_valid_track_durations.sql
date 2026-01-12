-- Verify all tracks have reasonable durations (0.5 to 60 minutes)
-- Should return 0 rows

select
    track_id,
    track_name,
    duration_minutes
from {{ ref('dim_track') }}
where duration_minutes < 0.5
   or duration_minutes > 60
   or duration_minutes is null