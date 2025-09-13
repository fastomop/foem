SELECT
    pe1.year_of_birth,
    gt.gender,
    COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
JOIN (
    SELECT
        concept_id,
        concept_name AS gender
    FROM concept
    WHERE domain_id = 'Gender' AND standard_concept = 'S'
) AS gt
    ON pe1.gender_concept_id = gt.concept_id
GROUP BY pe1.year_of_birth, gt.gender;
