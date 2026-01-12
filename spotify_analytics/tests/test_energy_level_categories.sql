-- Verify energy_level categorization logic
-- Should return 0 rows if all categorizations are correct

select
    track_id,
    energy,
    energy_level
from {{ ref('fct_plays') }}
where 
    (energy < 0.5 and energy_level != 'low')
    or (energy >= 0.5 and energy < 0.8 and energy_level != 'medium')
    or (energy >= 0.8 and energy_level != 'high')