WITH valid_drug_ids AS (
    -- 1. Cache Valid Drugs
    -- Create a simple list of IDs to avoid joining the heavy Concept table repeatedly.
    SELECT concept_id 
    FROM concept 
    WHERE domain_id = 'Drug' 
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
candidates AS (
    -- 2. Optimization: Aggressive Pruning
    -- Only look at patients who have taken at least 4 distinct valid drugs.
    -- This eliminates 80-90% of the population immediately.
    SELECT de.person_id 
    FROM drug_exposure de
    WHERE de.drug_concept_id IN (SELECT concept_id FROM valid_drug_ids)
    GROUP BY de.person_id 
    HAVING COUNT(DISTINCT de.drug_concept_id) >= 4
),
clean_exposures AS (
    -- 3. Flatten and Standardize
    -- Pull only the necessary data for the candidates.
    SELECT 
        de.person_id,
        de.drug_concept_id,
        de.drug_exposure_start_date AS s_date,
        -- Handle missing end dates by defaulting to start date (1-day script)
        COALESCE(de.drug_exposure_end_date, de.drug_exposure_start_date) AS e_date
    FROM drug_exposure de
    INNER JOIN candidates c ON de.person_id = c.person_id
    WHERE de.drug_concept_id IN (SELECT concept_id FROM valid_drug_ids)
      AND de.drug_exposure_start_date IS NOT NULL
),
quads AS (
    -- 4. The 4-Way Intersection Join
    SELECT
        d1.drug_concept_id AS drug_1,
        d2.drug_concept_id AS drug_2,
        d3.drug_concept_id AS drug_3,
        d4.drug_concept_id AS drug_4,
        COUNT(DISTINCT d1.person_id) AS person_count
    FROM clean_exposures d1
    INNER JOIN clean_exposures d2 
        ON d1.person_id = d2.person_id 
        AND d1.drug_concept_id < d2.drug_concept_id -- Force Order
        -- Optimization: Quick pairwise check (A touches B)
        AND d1.s_date <= d2.e_date AND d1.e_date >= d2.s_date
    INNER JOIN clean_exposures d3 
        ON d1.person_id = d3.person_id 
        AND d2.drug_concept_id < d3.drug_concept_id -- Force Order
        -- Optimization: Quick pairwise check (B touches C)
        AND d2.s_date <= d3.e_date AND d2.e_date >= d3.s_date
    INNER JOIN clean_exposures d4 
        ON d1.person_id = d4.person_id 
        AND d3.drug_concept_id < d4.drug_concept_id -- Force Order
        -- Optimization: Quick pairwise check (C touches D)
        AND d3.s_date <= d4.e_date AND d3.e_date >= d4.s_date
    WHERE d2.s_date BETWEEN d1.s_date AND (d1.s_date + 30)
        AND d3.s_date BETWEEN d1.s_date AND (d1.s_date + 30)
        AND d4.s_date BETWEEN d1.s_date AND (d1.s_date + 30)
    GROUP BY 
        d1.drug_concept_id, d2.drug_concept_id, d3.drug_concept_id, d4.drug_concept_id
    ORDER BY 
        person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS drug1_name,
    c2.concept_name AS drug2_name,
    c3.concept_name AS drug3_name,
    c4.concept_name AS drug4_name
    -- ,q.person_count -- Uncomment to see counts
FROM quads q
INNER JOIN concept c1 ON q.drug_1 = c1.concept_id
INNER JOIN concept c2 ON q.drug_2 = c2.concept_id
INNER JOIN concept c3 ON q.drug_3 = c3.concept_id
INNER JOIN concept c4 ON q.drug_4 = c4.concept_id;