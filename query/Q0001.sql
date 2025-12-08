WITH valid_drug_ids AS (
    -- 1. Cache valid drugs (Hash Map)
    -- We do this once so we don't join to the concept table repeatedly.
    SELECT concept_id 
    FROM concept 
    WHERE domain_id = 'Drug' 
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
candidates AS (
    -- 2. Optimization: Pruning (The "Fast Fail")
    -- Ignore patients with < 2 records. They cannot have a pair.
    SELECT person_id 
    FROM drug_exposure
    GROUP BY person_id 
    HAVING COUNT(*) >= 2
),
clean_exposures AS (
    -- 3. Flatten and Filter
    -- We get the subset of data needed for the self-join.
    SELECT 
        de.person_id,
        de.drug_concept_id,
        de.drug_exposure_start_date AS start_date
    FROM drug_exposure de
    INNER JOIN candidates c ON de.person_id = c.person_id
    WHERE de.drug_concept_id IN (SELECT concept_id FROM valid_drug_ids)
      AND de.drug_exposure_start_date IS NOT NULL
),
pairs AS (
    -- 4. The Optimized Self-Join
    SELECT
        d1.drug_concept_id AS drug_1,
        d2.drug_concept_id AS drug_2,
        COUNT(DISTINCT d1.person_id) AS co_prescription_count
    FROM clean_exposures d1
    INNER JOIN clean_exposures d2 
        ON d1.person_id = d2.person_id 
        AND d1.drug_concept_id < d2.drug_concept_id -- Force A-B order (Avoids A-B and B-A duplicates)
        -- CRITICAL OPTIMIZATION: 
        -- Replacing ABS() with BETWEEN allows the database to use the Date Index.
        AND d2.start_date BETWEEN (d1.start_date - 30) AND (d1.start_date + 30)
    GROUP BY 
        d1.drug_concept_id, 
        d2.drug_concept_id
    ORDER BY 
        co_prescription_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS drug1_name,
    c2.concept_name AS drug2_name,
    p.co_prescription_count
FROM pairs p
-- Only join to concept table at the very end for the labels
INNER JOIN concept c1 ON p.drug_1 = c1.concept_id
INNER JOIN concept c2 ON p.drug_2 = c2.concept_id;