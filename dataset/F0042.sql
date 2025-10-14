-- Number of patients grouped by race and gender.

SELECT
    COALESCE(rt.race, 'Unknown') AS race,
    COALESCE(gen_temp1.gender, 'Unknown') AS gender,
    COUNT(DISTINCT pe1.person_id)
FROM ((
    person AS pe1 LEFT JOIN (
        SELECT
            concept_id,
            concept_name AS race
        FROM concept
        WHERE domain_id = 'Race' AND standard_concept = 'S'
    ) AS rt
        ON pe1.race_concept_id = rt.concept_id
) LEFT JOIN
    (SELECT
        concept_id,
        concept_name AS gender
    FROM concept
    WHERE domain_id = 'Gender' AND standard_concept = 'S') AS gen_temp1
    ON pe1.gender_concept_id = gen_temp1.concept_id
)
GROUP BY rt.race, gen_temp1.gender;
