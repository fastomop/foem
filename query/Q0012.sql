WITH valid_condition_ids AS (
    -- 1. Cache Valid Conditions (Integer List)
    SELECT concept_id 
    FROM concept 
    WHERE domain_id = 'Condition' 
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
candidates AS (
    -- 2. Optimization: Pruning (Fast Fail)
    -- A patient needs at least 2 distinct condition eras to form a sequential pair.
    SELECT person_id 
    FROM condition_era
    WHERE condition_concept_id IN (SELECT concept_id FROM valid_condition_ids)
    GROUP BY person_id 
    HAVING COUNT(DISTINCT condition_concept_id) >= 2
),
relevant_eras AS (
    -- 3. Flatten Data for Candidates
    SELECT 
        ce.person_id,
        ce.condition_concept_id,
        ce.condition_era_start_date AS s_date,
        ce.condition_era_end_date AS e_date
    FROM condition_era ce
    INNER JOIN candidates c ON ce.person_id = c.person_id
    WHERE ce.condition_concept_id IN (SELECT concept_id FROM valid_condition_ids)
),
patient_pair_status AS (
    -- 4. The Single Pass Logic
    -- We join the history once. 
    -- Instead of filtering rows, we calculate a "Flag" for the patient.
    SELECT 
        e1.condition_concept_id AS cond1,
        e2.condition_concept_id AS cond2,
        -- Boolean Aggregation:
        -- If ANY instance of Cond A overlaps with Cond B, flag becomes 1.
        -- If they never overlap, flag stays 0.
        MAX(CASE 
            WHEN e1.s_date <= e2.e_date AND e1.e_date >= e2.s_date THEN 1 
            ELSE 0 
        END) AS has_overlap
    FROM relevant_eras e1
    INNER JOIN relevant_eras e2 
        ON e1.person_id = e2.person_id
        AND e1.condition_concept_id < e2.condition_concept_id -- Force Order
    GROUP BY 
        e1.person_id, 
        e1.condition_concept_id, 
        e2.condition_concept_id
    HAVING 
        -- 5. The "Strictly Separate" Filter
        -- Only keep patients where the overlap flag is 0 (Never Overlapped).
        MAX(CASE 
            WHEN e1.s_date <= e2.e_date AND e1.e_date >= e2.s_date THEN 1 
            ELSE 0 
        END) = 0
),
sequential_pairs AS (
    -- 6. Final Count
    SELECT 
        cond1,
        cond2,
        COUNT(*) AS patient_count
    FROM patient_pair_status
    GROUP BY cond1, cond2
    ORDER BY patient_count DESC
    LIMIT {self.result_limit}
)
SELECT 
    c1.concept_name AS condition1_name,
    c2.concept_name AS condition2_name
    -- ,sp.patient_count
FROM sequential_pairs sp
INNER JOIN concept c1 ON sp.cond1 = c1.concept_id
INNER JOIN concept c2 ON sp.cond2 = c2.concept_id;