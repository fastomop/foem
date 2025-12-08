-- Q0016
-- Counts of patients with condition {condition1_name}, {condition2_name} and {condition3_name} within 300 days.

WITH valid_conditions AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Condition'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
triads AS (
    SELECT
        e1.condition_concept_id AS cond1_concept_id,
        e2.condition_concept_id AS cond2_concept_id,
        e3.condition_concept_id AS cond3_concept_id,
        COUNT(DISTINCT e1.person_id) AS person_count
    FROM condition_occurrence e1
    INNER JOIN valid_conditions vc1 ON e1.condition_concept_id = vc1.concept_id
    INNER JOIN condition_occurrence e2
        ON e2.person_id = e1.person_id
       AND e2.condition_concept_id > e1.condition_concept_id
    INNER JOIN valid_conditions vc2 ON e2.condition_concept_id = vc2.concept_id
    INNER JOIN condition_occurrence e3
        ON e3.person_id = e1.person_id
       AND e3.condition_concept_id > e2.condition_concept_id
    INNER JOIN valid_conditions vc3 ON e3.condition_concept_id = vc3.concept_id
    WHERE (GREATEST(e1.condition_start_date, e2.condition_start_date, e3.condition_start_date)
         - LEAST(e1.condition_start_date, e2.condition_start_date, e3.condition_start_date)) <= 300
    GROUP BY e1.condition_concept_id, e2.condition_concept_id, e3.condition_concept_id
    ORDER BY person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS condition1_name,
    c2.concept_name AS condition2_name,
    c3.concept_name AS condition3_name
FROM triads t
INNER JOIN concept c1 ON c1.concept_id = t.cond1_concept_id
INNER JOIN concept c2 ON c2.concept_id = t.cond2_concept_id
INNER JOIN concept c3 ON c3.concept_id = t.cond3_concept_id;
