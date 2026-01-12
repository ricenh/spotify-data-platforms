-- Verify mood categorization logic
-- Should return 0 rows if all categorizations are correct

select
    track_id,
    valence,
    mood
from {{ ref('fct_plays') }}
where 
    (valence < 0.4 and mood != 'sad')
    or (valence >= 0.4 and valence < 0.7 and mood != 'neutral')
    or (valence >= 0.7 and mood != 'happy')