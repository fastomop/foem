WITH valid_condition_ids AS (
    -- 1. Cache Valid Conditions (Integer List)
    -- Avoid joining the heavy concept table inside the loop.
    SELECT concept_id 
    FROM concept 
    WHERE domain_id = 'Condition' 
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
unique_patient_conditions AS (
    -- 2. MAJOR OPTIMIZATION: Deduplicate First!
    -- We convert "All Visits" to "Distinct Conditions per Person".
    -- This stops the Cartesian explosion of repeat visits.
    SELECT DISTINCT 
        co.person_id, 
        co.condition_concept_id
    FROM condition_occurrence co
    WHERE co.condition_concept_id IN (SELECT concept_id FROM valid_condition_ids)
),
candidates AS (
    -- 3. Optimization: Pruning (Fast Fail)
    -- A patient needs at least 3 distinct conditions to form a triad.
    -- Filter out the "Simple Case" population immediately.
    SELECT person_id
    FROM unique_patient_conditions
    GROUP BY person_id
    HAVING COUNT(*) >= 3
),
triads AS (
    -- 4. The Efficient Self-Join
    -- Now running on a minimized dataset (Candidates * Unique Conditions).
    SELECT
        t1.condition_concept_id AS cond1_id,
        t2.condition_concept_id AS cond2_id,
        t3.condition_concept_id AS cond3_id,
        COUNT(*) AS person_count -- FAST: No need for COUNT(DISTINCT) anymore
    FROM unique_patient_conditions t1
    INNER JOIN candidates c ON t1.person_id = c.person_id
    INNER JOIN unique_patient_conditions t2 
        ON t1.person_id = t2.person_id 
        AND t1.condition_concept_id < t2.condition_concept_id -- Force Order (A < B)
    INNER JOIN unique_patient_conditions t3 
        ON t1.person_id = t3.person_id 
        AND t2.condition_concept_id < t3.condition_concept_id -- Force Order (B < C)
    GROUP BY 
        t1.condition_concept_id, 
        t2.condition_concept_id, 
        t3.condition_concept_id
    ORDER BY 
        person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS condition1_name,
    c2.concept_name AS condition2_name,
    c3.concept_name AS condition3_name
    -- ,t.person_count
FROM triads t
INNER JOIN concept c1 ON t.cond1_id = c1.concept_id
INNER JOIN concept c2 ON t.cond2_id = c2.concept_id
INNER JOIN concept c3 ON t.cond3_id = c3.concept_id;