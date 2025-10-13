-- Number of patients by gender and state.

WITH gt AS (
    SELECT
        concept_id,
        concept_name AS gender
    FROM concept
    WHERE domain_id = 'Gender' AND standard_concept = 'S'
)

SELECT
    gt.gender,
    loc1.state,
    COUNT(DISTINCT pe1.person_id) AS number_of_patients
FROM person AS pe1
INNER JOIN gt
    ON pe1.gender_concept_id = gt.concept_id
INNER JOIN location AS loc1 ON pe1.location_id = loc1.location_id
GROUP BY gt.gender, loc1.state;
