WITH valid_drugs AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Drug'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
-- Optimization: Get unique patient-drug pairs first. 
-- This drastically reduces the row count before the self-joins.
unique_patient_drugs AS (
    SELECT DISTINCT 
        de.person_id, 
        de.drug_concept_id
    FROM drug_exposure de
    INNER JOIN valid_drugs vd 
        ON de.drug_concept_id = vd.concept_id
),
triples AS (
    SELECT
        t1.drug_concept_id AS drug1_concept_id,
        t2.drug_concept_id AS drug2_concept_id,
        t3.drug_concept_id AS drug3_concept_id,
        COUNT(*) AS person_count -- Faster than COUNT(DISTINCT)
    FROM unique_patient_drugs t1
    INNER JOIN unique_patient_drugs t2 
        ON t1.person_id = t2.person_id 
        AND t2.drug_concept_id > t1.drug_concept_id
    INNER JOIN unique_patient_drugs t3 
        ON t1.person_id = t3.person_id 
        AND t3.drug_concept_id > t2.drug_concept_id
    GROUP BY 
        t1.drug_concept_id, 
        t2.drug_concept_id, 
        t3.drug_concept_id
    ORDER BY 
        person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS drug1_name,
    c2.concept_name AS drug2_name,
    c3.concept_name AS drug3_name,
    t.person_count
FROM triples t
INNER JOIN concept c1 ON c1.concept_id = t.drug1_concept_id
INNER JOIN concept c2 ON c2.concept_id = t.drug2_concept_id
INNER JOIN concept c3 ON c3.concept_id = t.drug3_concept_id;