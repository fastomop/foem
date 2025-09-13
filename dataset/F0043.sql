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
    rt.race,
    st.state,
    COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
INNER JOIN race_concepts rt ON pe1.race_concept_id = rt.concept_id
INNER JOIN location_states st ON pe1.location_id = st.location_id
GROUP BY rt.race, st.state;
