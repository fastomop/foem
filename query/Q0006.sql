-- 1. APRIORI FILTER: Only consider drugs that appear frequently.
--    This drastically reduces the search space (the 'N' in N^4).
WITH frequent_drugs AS (
    SELECT drug_concept_id
    FROM drug_era
    GROUP BY drug_concept_id
    ORDER BY count(DISTINCT person_id) DESC
    LIMIT 500 -- Keep this number as low as your business logic allows
),

-- 2. VALID DRUGS + OVERLAP PRE-CALCULATION
--    We filter early and ensure we only look at drugs in the 'frequent' list.
--    We (optionally) can ignore the specific dates here if 'Ever Took' is enough,
--    but to match your original logic, we just prepare the raw data.
valid_eras AS (
    SELECT 
        de.person_id, 
        de.drug_concept_id
    FROM drug_era de
    JOIN frequent_drugs fd ON de.drug_concept_id = fd.drug_concept_id
    JOIN concept c ON c.concept_id = de.drug_concept_id
    WHERE c.domain_id = 'Drug' 
      AND c.standard_concept = 'S'
),

-- 3. COLLECT & EXPLODE (The "Medium Article" Strategy)
--    Instead of joining table-to-table, we collapse drugs into an ARRAY per patient.
patient_baskets AS (
    SELECT 
        person_id, 
        -- Create a sorted array of unique drugs this patient took
        ARRAY_AGG(DISTINCT drug_concept_id ORDER BY drug_concept_id) AS drug_list
    FROM valid_eras
    GROUP BY person_id
    -- Optimization: Discard patients who didn't take at least 4 top drugs
    HAVING cardinality(array_agg(DISTINCT drug_concept_id)) >= 4
),

-- 4. GENERATE COMBINATIONS IN MEMORY
--    We unnest the array against itself. Because the array size is small 
--    (controlled by the frequent_drugs limit), this is very fast.
combinations AS (
    SELECT 
        d1 AS drug1,
        d2 AS drug2,
        d3 AS drug3,
        d4 AS drug4,
        person_id
    FROM patient_baskets,
         UNNEST(drug_list) d1,
         UNNEST(drug_list) d2,
         UNNEST(drug_list) d3,
         UNNEST(drug_list) d4
    WHERE d2 > d1 
      AND d3 > d2 
      AND d4 > d3
)

-- 5. FINAL COUNT & LABELING
SELECT 
    c1.concept_name AS drug1_name,
    c2.concept_name AS drug2_name,
    c3.concept_name AS drug3_name,
    c4.concept_name AS drug4_name,
    COUNT(DISTINCT comb.person_id) AS patient_count
FROM combinations comb
-- Join back to get names only at the very end
JOIN concept c1 ON comb.drug1 = c1.concept_id
JOIN concept c2 ON comb.drug2 = c2.concept_id
JOIN concept c3 ON comb.drug3 = c3.concept_id
JOIN concept c4 ON comb.drug4 = c4.concept_id
GROUP BY 1, 2, 3, 4
ORDER BY patient_count DESC
LIMIT {self.result_limit}