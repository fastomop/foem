-- Number of patients grouped by gender and ethnicity.

WITH gen_temp1 AS (
    SELECT
        concept_id,
        concept_name AS gender
    FROM concept
    WHERE domain_id = 'Gender' AND standard_concept = 'S'
),

eth_temp1 AS (
    SELECT
        concept_id,
        concept_name AS ethnicity
    FROM concept
    WHERE domain_id = 'Ethnicity' AND standard_concept = 'S'
)

SELECT
    gen_temp1.gender,
    eth_temp1.ethnicity,
    COUNT(DISTINCT pe1.person_id) AS number_of_patients
FROM person AS pe1
INNER JOIN gen_temp1
    ON pe1.gender_concept_id = gen_temp1.concept_id
INNER JOIN eth_temp1
    ON pe1.ethnicity_concept_id = eth_temp1.concept_id
GROUP BY gen_temp1.gender, eth_temp1.ethnicity;
