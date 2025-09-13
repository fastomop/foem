SELECT
    rt.race,
    et.ethnicity,
    COUNT(DISTINCT pe1.person_id)
FROM ((
    person AS pe1 JOIN (
        SELECT
            concept_id,
            concept_name AS race
        FROM concept
        WHERE domain_id = 'Race' AND standard_concept = 'S'
    ) AS rt
        ON pe1.race_concept_id = rt.concept_id
) JOIN
    (SELECT
        concept_id,
        concept_name AS ethnicity
    FROM concept
    WHERE domain_id = 'Ethnicity' AND standard_concept = 'S') AS et
    ON pe1.ethnicity_concept_id = et.concept_id
)
GROUP BY rt.race, et.ethnicity;
