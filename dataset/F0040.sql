WITH gender_concepts AS (
    SELECT
        concept_id,
        concept_name AS gender
    FROM concept
    WHERE domain_id = 'Gender' AND standard_concept = 'S'
)

SELECT
    gender,
    COUNT(DISTINCT pe1.person_id)
FROM person pe1
INNER JOIN gender_concepts gc ON pe1.gender_concept_id = gc.concept_id
GROUP BY gender;
