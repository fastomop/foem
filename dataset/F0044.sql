WITH rt AS (
    SELECT
        concept_id,
        concept_name AS race
    FROM concept
    WHERE domain_id = 'Race' AND standard_concept = 'S'
)

SELECT
    rt.race,
    pe1.year_of_birth,
    COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
INNER JOIN rt
    ON pe1.race_concept_id = rt.concept_id
GROUP BY rt.race, pe1.year_of_birth;
