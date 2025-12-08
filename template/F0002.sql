-- Count of patients grouped by race.

WITH alias1 AS (
    SELECT
        concept_id,
        concept_name AS race
    FROM concept
    WHERE domain_id = 'Race' AND standard_concept = 'S'
)

SELECT
    race,
    COUNT(DISTINCT pe1.person_id) AS number_of_patients
FROM person AS pe1
LEFT JOIN alias1
    ON pe1.race_concept_id = concept_id
GROUP BY race;
