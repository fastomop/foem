WITH valid_condition_ids AS (
    -- 1. Cache Valid Conditions (Integer List)
    -- Avoid joining the heavy concept table repeatedly.
    SELECT concept_id 
    FROM concept 
    WHERE domain_id = 'Condition' 
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
unique_patient_conditions AS (
    -- 2. MAJOR OPTIMIZATION: Deduplicate First!
    -- We convert "All Condition Occurrences" to "Distinct Conditions per Person".
    -- This eliminates the Cartesian product of repeat visits.
    SELECT DISTINCT 
        co.person_id, 
        co.condition_concept_id
    FROM condition_occurrence co
    WHERE co.condition_concept_id IN (SELECT concept_id FROM valid_condition_ids)
),
candidates AS (
    -- 3. Optimization: Pruning
    -- A patient needs at least 2 distinct conditions to form a pair.
    -- Filter out the "Single Diagnosis" population immediately.
    SELECT person_id
    FROM unique_patient_conditions
    GROUP BY person_id
    HAVING COUNT(*) >= 2
),
pair_counts AS (
    -- 4. The Efficient Self-Join
    SELECT
        t1.condition_concept_id AS cond1_id,
        t2.condition_concept_id AS cond2_id,
        COUNT(*) AS patient_count -- FAST: No need for COUNT(DISTINCT) anymore
    FROM unique_patient_conditions t1
    INNER JOIN candidates c ON t1.person_id = c.person_id
    INNER JOIN unique_patient_conditions t2 
        ON t1.person_id = t2.person_id 
        AND t1.condition_concept_id < t2.condition_concept_id -- Force Order (Removes duplicates)
    GROUP BY 
        t1.condition_concept_id, 
        t2.condition_concept_id
    ORDER BY 
        patient_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS condition1_name,
    c2.concept_name AS condition2_name
    -- ,pc.patient_count
FROM pair_counts pc
INNER JOIN concept c1 ON pc.cond1_id = c1.concept_id
INNER JOIN concept c2 ON pc.cond2_id = c2.concept_id;