with source as (
    select * from {{ source('raw', 'artists') }}
),

renamed as (
    select
        artist_id,
        name as artist_name,
        popularity,
        genres,
        
        -- Create flag for genre availability
        case 
            when genres is not null and array_length(genres, 1) > 0 
            then true 
            else false 
        end as has_genres
        
    from source
)

select * from renamed