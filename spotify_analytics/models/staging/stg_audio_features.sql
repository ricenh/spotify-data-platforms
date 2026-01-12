with source as (
    select * from {{ source('raw', 'audio_features') }}
),

renamed as (
    select
        track_id,
        danceability,
        energy,
        tempo,
        valence,
        loudness,
        
        -- Create categorical energy level
        case
            when energy >= 0.8 then 'high'
            when energy >= 0.5 then 'medium'
            else 'low'
        end as energy_level,
        
        -- Create categorical mood based on valence
        case
            when valence >= 0.7 then 'happy'
            when valence >= 0.4 then 'neutral'
            else 'sad'
        end as mood
        
    from source
)

select * from renamed