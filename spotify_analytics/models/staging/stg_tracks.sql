with source as (
    select * from {{ source('raw', 'tracks') }}
),

renamed as (
    select
        track_id,
        name as track_name,
        duration_ms,
        popularity,
        explicit as is_explicit,
        artist_id as primary_artist_id,
        
        -- Convert duration to minutes
        round(duration_ms / 60000.0, 2) as duration_minutes
        
    from source
)

select * from renamed