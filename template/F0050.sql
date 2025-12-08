-- Number of patients of race <ARG-RACE><0>.

WITH race_concept AS (
    SELECT concept_id
    FROM concept
    WHERE concept_name = %(race)s 
    AND domain_id = 'Race' 
    AND standard_concept = 'S'
)

SELECT COUNT(DISTINCT pe1.person_id) AS number_of_patients
FROM person pe1
JOIN race_concept rc ON pe1.race_concept_id = rc.concept_id;