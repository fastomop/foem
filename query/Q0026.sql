-- How many people have treated by drug {drug_a_name} followed by drug {drug_b_name}?

WITH valid_drugs AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Drug'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
ordered AS (
    SELECT
        person_id,
        drug_concept_id AS drug_a,
        LEAD(drug_concept_id) OVER (
            PARTITION BY person_id
            ORDER BY drug_exposure_start_date, drug_exposure_id
        ) AS drug_b
    FROM drug_exposure
    INNER JOIN valid_drugs vd ON drug_concept_id = vd.concept_id
),
pairs AS (
    SELECT
        drug_a,
        drug_b,
        COUNT(DISTINCT person_id) AS person_count
    FROM ordered
    WHERE drug_b IS NOT NULL
      AND drug_a <> drug_b
    GROUP BY drug_a, drug_b
    ORDER BY person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS drug_a_name,
    c2.concept_name AS drug_b_name
    -- ,p.person_count AS patient_count
FROM pairs p
INNER JOIN concept c1 ON c1.concept_id = p.drug_a
INNER JOIN concept c2 ON c2.concept_id = p.drug_b;
