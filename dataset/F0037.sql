WITH 
ethnicity_concepts AS (
    SELECT
        concept_id,
        concept_name AS ethnicity
    FROM concept
    WHERE domain_id = 'Ethnicity' 
    AND standard_concept = 'S'
),

location_states AS (
    SELECT
        location_id,
        state
    FROM location
)

SELECT
    et.ethnicity,
    st.state,
    COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
INNER JOIN ethnicity_concepts et ON pe1.ethnicity_concept_id = et.concept_id
INNER JOIN location_states st ON pe1.location_id = st.location_id
GROUP BY et.ethnicity, st.state;
