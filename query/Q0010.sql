WITH valid_condition_ids AS (
    -- 1. Cache Valid Conditions (Integer List)
    -- Keeps the main query join logic clean and numeric.
    SELECT concept_id 
    FROM concept 
    WHERE domain_id = 'Condition' 
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
candidates AS (
    -- 2. Pruning (Fast Fail)
    -- Only check patients with at least 2 distinct conditions.
    -- This removes a significant chunk of the population (e.g., single-visit patients).
    SELECT person_id 
    FROM condition_occurrence
    WHERE condition_concept_id IN (SELECT concept_id FROM valid_condition_ids)
    GROUP BY person_id 
    HAVING COUNT(DISTINCT condition_concept_id) >= 2
),
clean_conditions AS (
    -- 3. Flatten and Standardize
    -- Only fetch necessary columns for the candidates.
    SELECT 
        co.person_id,
        co.condition_concept_id,
        co.condition_start_date
    FROM condition_occurrence co
    INNER JOIN candidates c ON co.person_id = c.person_id
    WHERE co.condition_concept_id IN (SELECT concept_id FROM valid_condition_ids)
      AND co.condition_start_date IS NOT NULL
),
pair_counts AS (
    -- 4. The Optimized Self-Join
    SELECT
        c1.condition_concept_id AS cond1_id,
        c2.condition_concept_id AS cond2_id,
        COUNT(DISTINCT c1.person_id) AS patient_count
    FROM clean_conditions c1
    INNER JOIN clean_conditions c2 
        ON c1.person_id = c2.person_id 
        AND c1.condition_concept_id < c2.condition_concept_id -- Force Order (Removes duplicates & need for LEAST/GREATEST)
        -- CRITICAL OPTIMIZATION: Index Range Scan
        -- Replaces ABS(date - date) <= 30
        AND c2.condition_start_date BETWEEN (c1.condition_start_date - 30) AND (c1.condition_start_date + 30)
    GROUP BY 
        c1.condition_concept_id, 
        c2.condition_concept_id
    ORDER BY 
        patient_count DESC
    LIMIT {self.result_limit}
)
SELECT
    concept1.concept_name AS condition1_name,
    concept2.concept_name AS condition2_name
    -- ,pc.patient_count
FROM pair_counts pc
INNER JOIN concept concept1 ON pc.cond1_id = concept1.concept_id
INNER JOIN concept concept2 ON pc.cond2_id = concept2.concept_id;