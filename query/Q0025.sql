-- How many people have treated by drug {drug_a_name} after more than 30 days of starting with drug {drug_b_name}?

WITH valid_drugs AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Drug'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
pairs AS (
    SELECT
        e1.drug_concept_id AS drug_a,
        e2.drug_concept_id AS drug_b,
        COUNT(DISTINCT e1.person_id) AS person_count
    FROM drug_exposure e1
    INNER JOIN valid_drugs vd1 ON e1.drug_concept_id = vd1.concept_id
    INNER JOIN drug_exposure e2
        ON e2.person_id = e1.person_id
       AND e2.drug_concept_id <> e1.drug_concept_id
       AND (e2.drug_exposure_start_date - e1.drug_exposure_start_date) >= 30
    INNER JOIN valid_drugs vd2 ON e2.drug_concept_id = vd2.concept_id
    GROUP BY e1.drug_concept_id, e2.drug_concept_id
    ORDER BY person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c2.concept_name AS drug_b_name,
    c1.concept_name AS drug_a_name
    -- ,p.person_count AS patient_count
FROM pairs p
INNER JOIN concept c1 ON c1.concept_id = p.drug_a
INNER JOIN concept c2 ON c2.concept_id = p.drug_b;
