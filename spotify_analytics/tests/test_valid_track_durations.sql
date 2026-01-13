{{ config(severity='warn') }}

select
    track_id,
    track_name,
    duration_minutes
from {{ ref('dim_track') }}
where duration_minutes <= 0 
   or duration_minutes is null
   or duration_minutes > 60  