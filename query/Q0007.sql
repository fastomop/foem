WITH valid_drug_ids AS (
    -- 1. Cache Valid Drugs (Integer List)
    -- Prevents joining the text-heavy Concept table 3 times inside the loop.
    SELECT concept_id 
    FROM concept 
    WHERE domain_id = 'Drug' 
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
candidates AS (
    -- 2. Optimization: Pruning (Fast Fail)
    -- Filter out patients with fewer than 3 drugs immediately.
    SELECT person_id 
    FROM drug_exposure
    WHERE drug_concept_id IN (SELECT concept_id FROM valid_drug_ids)
    GROUP BY person_id 
    HAVING COUNT(DISTINCT drug_concept_id) >= 3
),
clean_exposures AS (
    -- 3. Flatten and Standardize
    -- Only fetch necessary columns for the candidates.
    SELECT 
        de.person_id,
        de.drug_concept_id,
        de.drug_exposure_start_date AS s_date
    FROM drug_exposure de
    INNER JOIN candidates c ON de.person_id = c.person_id
    WHERE de.drug_concept_id IN (SELECT concept_id FROM valid_drug_ids)
      AND de.drug_exposure_start_date IS NOT NULL
),
triples AS (
    -- 4. The Optimized Join
    SELECT
        d1.drug_concept_id AS drug_1,
        d2.drug_concept_id AS drug_2,
        d3.drug_concept_id AS drug_3,
        COUNT(DISTINCT d1.person_id) AS person_count
    FROM clean_exposures d1
    INNER JOIN clean_exposures d2 
        ON d1.person_id = d2.person_id 
        AND d1.drug_concept_id < d2.drug_concept_id -- Force Concept Order
        -- OPTIMIZATION: Index Range Scan
        -- Instead of calculating math, we check if D2 is roughly near D1.
        AND d2.s_date BETWEEN (d1.s_date - 30) AND (d1.s_date + 30)
    INNER JOIN clean_exposures d3 
        ON d1.person_id = d3.person_id 
        AND d2.drug_concept_id < d3.drug_concept_id -- Force Concept Order
        -- OPTIMIZATION: Index Range Scan
        -- Check if D3 is near D1...
        AND d3.s_date BETWEEN (d1.s_date - 30) AND (d1.s_date + 30)
        -- ...AND check if D3 is near D2
        AND d3.s_date BETWEEN (d2.s_date - 30) AND (d2.s_date + 30)
    WHERE 
        -- 5. The Final Strict Check
        -- We apply your exact logic (Max - Min <= 30) only on the 
        -- tiny subset of rows that passed the index scans above.
        (GREATEST(d1.s_date, d2.s_date, d3.s_date) - LEAST(d1.s_date, d2.s_date, d3.s_date)) <= 30
    GROUP BY 
        d1.drug_concept_id, 
        d2.drug_concept_id, 
        d3.drug_concept_id
    ORDER BY 
        person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS drug1_name,
    c2.concept_name AS drug2_name,
    c3.concept_name AS drug3_name
    -- ,t.person_count
FROM triples t
INNER JOIN concept c1 ON t.drug_1 = c1.concept_id
INNER JOIN concept c2 ON t.drug_2 = c2.concept_id
INNER JOIN concept c3 ON t.drug_3 = c3.concept_id;