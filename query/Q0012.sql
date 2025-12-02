-- Counts of patients with condition {condition1_name} or {condition2_name}.

WITH valid_conditions AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Condition'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
eras AS (
    SELECT
        ce.person_id,
        ce.condition_concept_id,
        ce.condition_era_start_date AS start_date,
        ce.condition_era_end_date AS end_date
    FROM condition_era ce
    INNER JOIN valid_conditions vc ON vc.concept_id = ce.condition_concept_id
),
cooccur_pairs AS (
    SELECT
        e1.person_id,
        LEAST(e1.condition_concept_id, e2.condition_concept_id) AS cond1,
        GREATEST(e1.condition_concept_id, e2.condition_concept_id) AS cond2
    FROM eras e1
    INNER JOIN eras e2
        ON e1.person_id = e2.person_id
       AND e1.condition_concept_id < e2.condition_concept_id
    GROUP BY e1.person_id,
            LEAST(e1.condition_concept_id, e2.condition_concept_id),
            GREATEST(e1.condition_concept_id, e2.condition_concept_id)
),
overlapping_pairs AS (
    SELECT DISTINCT
        a.person_id,
        LEAST(a.condition_concept_id, b.condition_concept_id) AS cond1,
        GREATEST(a.condition_concept_id, b.condition_concept_id) AS cond2
    FROM eras a
    INNER JOIN eras b
        ON a.person_id = b.person_id
       AND a.condition_concept_id < b.condition_concept_id
       AND a.start_date <= b.end_date
       AND b.start_date <= a.end_date
),
separate_pairs AS (
    SELECT c.cond1, c.cond2
    FROM cooccur_pairs c
    LEFT JOIN overlapping_pairs o
        ON o.person_id = c.person_id
       AND o.cond1 = c.cond1
       AND o.cond2 = c.cond2
    WHERE o.person_id IS NULL
    GROUP BY c.cond1, c.cond2
    ORDER BY COUNT(DISTINCT c.person_id) DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS condition1_name,
    c2.concept_name AS condition2_name
FROM separate_pairs sp
INNER JOIN concept c1 ON c1.concept_id = sp.cond1
INNER JOIN concept c2 ON c2.concept_id = sp.cond2;
