-- Test: ReccoBeats audio features are within valid ranges
--
-- Validates that audio features from ReccoBeats API have expected values:
--   - danceability: 0.0 to 1.0
--   - energy: 0.0 to 1.0
--   - valence: 0.0 to 1.0
--   - tempo: 40 to 250 BPM (reasonable music range)
--   - loudness: -60 to 0 dB (typical range)
--
-- This test FAILS if any ReccoBeats features are outside expected ranges,
-- which could indicate API issues or data corruption
--
-- Expected result: 0 rows

select
    track_id,
    track_name,
    danceability,
    energy,
    valence,
    tempo,
    loudness,
    case
        when danceability < 0 or danceability > 1 then 'Invalid danceability'
        when energy < 0 or energy > 1 then 'Invalid energy'
        when valence < 0 or valence > 1 then 'Invalid valence'
        when tempo < 40 or tempo > 250 then 'Invalid tempo'
        when loudness < -60 or loudness > 1 then 'Invalid loudness'
    end as failure_reason
from {{ ref('fct_plays') }}
where 
    danceability < 0 or danceability > 1
    or energy < 0 or energy > 1
    or valence < 0 or valence > 1
    or tempo < 40 or tempo > 250
    or loudness < -60 or loudness > 1