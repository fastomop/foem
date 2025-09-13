SELECT
    ethnicity,
    COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
JOIN (
    SELECT
        concept_id,
        concept_name AS ethnicity
    FROM concept
    WHERE domain_id = 'Ethnicity' AND standard_concept = 'S'
) AS alias1
    ON pe1.ethnicity_concept_id = concept_id
GROUP BY ethnicity;
