-- Counts of patients with condition {condition1_name}, {condition2_name}, {condition3_name} or {condition4_name}.

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
cooccur_quads AS (
    SELECT
        e1.person_id,
        e1.condition_concept_id AS cond1,
        e2.condition_concept_id AS cond2,
        e3.condition_concept_id AS cond3,
        e4.condition_concept_id AS cond4
    FROM eras e1
    INNER JOIN eras e2
        ON e2.person_id = e1.person_id
       AND e2.condition_concept_id > e1.condition_concept_id
    INNER JOIN eras e3
        ON e3.person_id = e1.person_id
       AND e3.condition_concept_id > e2.condition_concept_id
    INNER JOIN eras e4
        ON e4.person_id = e1.person_id
       AND e4.condition_concept_id > e3.condition_concept_id
    GROUP BY e1.person_id, e1.condition_concept_id, e2.condition_concept_id, e3.condition_concept_id, e4.condition_concept_id
),
overlapping_quads AS (
    SELECT DISTINCT
        a.person_id,
        a.condition_concept_id AS cond1,
        b.condition_concept_id AS cond2,
        c.condition_concept_id AS cond3,
        d.condition_concept_id AS cond4
    FROM eras a
    INNER JOIN eras b
        ON b.person_id = a.person_id
       AND b.condition_concept_id > a.condition_concept_id
    INNER JOIN eras c
        ON c.person_id = a.person_id
       AND c.condition_concept_id > b.condition_concept_id
    INNER JOIN eras d
        ON d.person_id = a.person_id
       AND d.condition_concept_id > c.condition_concept_id
    WHERE GREATEST(a.start_date, b.start_date, c.start_date, d.start_date)
       <= LEAST(a.end_date, b.end_date, c.end_date, d.end_date)
),
separate_quads AS (
    SELECT cq.cond1, cq.cond2, cq.cond3, cq.cond4
    FROM cooccur_quads cq
    LEFT JOIN overlapping_quads oq
        ON oq.person_id = cq.person_id
       AND oq.cond1 = cq.cond1
       AND oq.cond2 = cq.cond2
       AND oq.cond3 = cq.cond3
       AND oq.cond4 = cq.cond4
    WHERE oq.person_id IS NULL
    GROUP BY cq.cond1, cq.cond2, cq.cond3, cq.cond4
    ORDER BY COUNT(DISTINCT cq.person_id) DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS condition1_name,
    c2.concept_name AS condition2_name,
    c3.concept_name AS condition3_name,
    c4.concept_name AS condition4_name
FROM separate_quads sq
INNER JOIN concept c1 ON c1.concept_id = sq.cond1
INNER JOIN concept c2 ON c2.concept_id = sq.cond2
INNER JOIN concept c3 ON c3.concept_id = sq.cond3
INNER JOIN concept c4 ON c4.concept_id = sq.cond4;
