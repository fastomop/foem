-- Counts of patients taking drug {drug1_name}, {drug2_name} and {drug3_name} within 30 days.

WITH valid_drugs AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Drug'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
triples AS (
    SELECT
        e1.drug_concept_id AS drug1_concept_id,
        e2.drug_concept_id AS drug2_concept_id,
        e3.drug_concept_id AS drug3_concept_id,
        COUNT(DISTINCT e1.person_id) AS person_count
    FROM drug_exposure e1
    INNER JOIN valid_drugs vd1 ON e1.drug_concept_id = vd1.concept_id
    INNER JOIN drug_exposure e2
        ON e2.person_id = e1.person_id
       AND e2.drug_concept_id > e1.drug_concept_id
    INNER JOIN valid_drugs vd2 ON e2.drug_concept_id = vd2.concept_id
    INNER JOIN drug_exposure e3
        ON e3.person_id = e1.person_id
       AND e3.drug_concept_id > e2.drug_concept_id
    INNER JOIN valid_drugs vd3 ON e3.drug_concept_id = vd3.concept_id
    WHERE (GREATEST(e1.drug_exposure_start_date, e2.drug_exposure_start_date, e3.drug_exposure_start_date)
         - LEAST(e1.drug_exposure_start_date, e2.drug_exposure_start_date, e3.drug_exposure_start_date)) <= 30
    GROUP BY e1.drug_concept_id, e2.drug_concept_id, e3.drug_concept_id
    ORDER BY person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS drug1_name,
    c2.concept_name AS drug2_name,
    c3.concept_name AS drug3_name
    -- ,t.person_count AS patients
FROM triples t
INNER JOIN concept c1 ON c1.concept_id = t.drug1_concept_id
INNER JOIN concept c2 ON c2.concept_id = t.drug2_concept_id
INNER JOIN concept c3 ON c3.concept_id = t.drug3_concept_id;
