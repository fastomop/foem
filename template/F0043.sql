-- Number of patients grouped by race and residence state location.

WITH
race_concepts AS (
    SELECT
        concept_id,
        concept_name AS race
    FROM concept
    WHERE domain_id = 'Race'
    AND standard_concept = 'S'
),

location_states AS (
    SELECT
        location_id,
        state
    FROM location
)

SELECT
    COALESCE(rt.race, 'Unknown') AS race,
    COALESCE(st.state, 'Unknown') AS state,
    COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
LEFT JOIN race_concepts rt ON pe1.race_concept_id = rt.concept_id
LEFT JOIN location_states st ON pe1.location_id = st.location_id
GROUP BY rt.race, st.state;
