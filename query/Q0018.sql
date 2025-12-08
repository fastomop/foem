-- Counts of patients with condition {condition1_name}, {condition2_name} or {condition3_name}.

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
cooccur_triples AS (
    SELECT
        e1.person_id,
        e1.condition_concept_id AS cond1,
        e2.condition_concept_id AS cond2,
        e3.condition_concept_id AS cond3
    FROM eras e1
    INNER JOIN eras e2
        ON e2.person_id = e1.person_id
       AND e2.condition_concept_id > e1.condition_concept_id
    INNER JOIN eras e3
        ON e3.person_id = e1.person_id
       AND e3.condition_concept_id > e2.condition_concept_id
    GROUP BY e1.person_id, e1.condition_concept_id, e2.condition_concept_id, e3.condition_concept_id
),
overlapping_triples AS (
    SELECT DISTINCT
        a.person_id,
        a.condition_concept_id AS cond1,
        b.condition_concept_id AS cond2,
        c.condition_concept_id AS cond3
    FROM eras a
    INNER JOIN eras b
        ON b.person_id = a.person_id
       AND b.condition_concept_id > a.condition_concept_id
    INNER JOIN eras c
        ON c.person_id = a.person_id
       AND c.condition_concept_id > b.condition_concept_id
    WHERE GREATEST(a.start_date, b.start_date, c.start_date)
       <= LEAST(a.end_date, b.end_date, c.end_date)
),
separate_triples AS (
    SELECT ct.cond1, ct.cond2, ct.cond3
    FROM cooccur_triples ct
    LEFT JOIN overlapping_triples ot
        ON ot.person_id = ct.person_id
       AND ot.cond1 = ct.cond1
       AND ot.cond2 = ct.cond2
       AND ot.cond3 = ct.cond3
    WHERE ot.person_id IS NULL
    GROUP BY ct.cond1, ct.cond2, ct.cond3
    ORDER BY COUNT(DISTINCT ct.person_id) DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS condition1_name,
    c2.concept_name AS condition2_name,
    c3.concept_name AS condition3_name
FROM separate_triples s
INNER JOIN concept c1 ON c1.concept_id = s.cond1
INNER JOIN concept c2 ON c2.concept_id = s.cond2
INNER JOIN concept c3 ON c3.concept_id = s.cond3;
