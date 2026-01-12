with source as (
    select * from {{ source('raw', 'plays') }}
),

renamed as (
    select
        played_at,
        track_id,
        
        -- Add derived columns
        played_at::date as play_date,
        extract(hour from played_at) as play_hour,
        extract(dow from played_at) as day_of_week  -- 0=Sunday, 6=Saturday
        
    from source
)

select * from renamed