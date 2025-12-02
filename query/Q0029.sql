-- How many people took drug {drug_name} after being diagnosed with condition {condition_name}?

WITH valid_conditions AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Condition'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
valid_drugs AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Drug'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
pairs AS (
    SELECT
        co.condition_concept_id,
        de.drug_concept_id,
        COUNT(DISTINCT co.person_id) AS person_count
    FROM condition_occurrence co
    INNER JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
    INNER JOIN drug_exposure de
        ON de.person_id = co.person_id
       AND de.drug_exposure_start_date > co.condition_start_date
    INNER JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
    GROUP BY co.condition_concept_id, de.drug_concept_id
    ORDER BY person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS condition_name,
    c2.concept_name AS drug_name
    -- ,p.person_count AS patient_count
FROM pairs p
INNER JOIN concept c1 ON c1.concept_id = p.condition_concept_id
INNER JOIN concept c2 ON c2.concept_id = p.drug_concept_id;
