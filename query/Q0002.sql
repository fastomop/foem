WITH valid_drug_ids AS (
    -- 1. Cache valid drugs (Integer List)
    -- Avoid joining the heavy concept table repeatedly.
    SELECT concept_id 
    FROM concept 
    WHERE domain_id = 'Drug' 
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
unique_patient_drugs AS (
    -- 2. MAJOR OPTIMIZATION: Deduplicate first!
    -- We reduce the table from "all prescriptions" to "unique patient-drug pairs".
    -- This eliminates the Cartesian product of refills.
    SELECT DISTINCT 
        de.person_id, 
        de.drug_concept_id
    FROM drug_exposure de
    WHERE de.drug_concept_id IN (SELECT concept_id FROM valid_drug_ids)
),
candidates AS (
    -- 3. Optimization: Pruning
    -- Only keep patients who have taken at least 2 distinct drugs.
    SELECT person_id
    FROM unique_patient_drugs
    GROUP BY person_id
    HAVING COUNT(*) >= 2
),
drug_pairs AS (
    -- 4. The Self-Join (Now running on a much smaller dataset)
    SELECT
        t1.drug_concept_id AS drug1_id,
        t2.drug_concept_id AS drug2_id,
        COUNT(*) AS co_prescription_count -- FAST: No need for COUNT(DISTINCT) anymore
    FROM unique_patient_drugs t1
    INNER JOIN candidates c ON t1.person_id = c.person_id
    INNER JOIN unique_patient_drugs t2 
        ON t1.person_id = t2.person_id 
        AND t1.drug_concept_id < t2.drug_concept_id -- Force Order (A-B)
    GROUP BY 
        t1.drug_concept_id, 
        t2.drug_concept_id
    ORDER BY 
        co_prescription_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS drug1_name,
    c2.concept_name AS drug2_name
    -- ,dp.co_prescription_count -- Uncomment if you need the count
FROM drug_pairs dp
INNER JOIN concept c1 ON dp.drug1_id = c1.concept_id
INNER JOIN concept c2 ON dp.drug2_id = c2.concept_id;