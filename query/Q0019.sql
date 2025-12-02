-- How many people have condition {first_condition} followed by condition {second_condition}?

WITH valid_conditions AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Condition'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
condition_pairs AS (
    SELECT
        a.condition_concept_id AS cond1_id,
        b.condition_concept_id AS cond2_id,
        COUNT(DISTINCT a.person_id) AS person_count
    FROM condition_occurrence a
    INNER JOIN valid_conditions vc1 ON a.condition_concept_id = vc1.concept_id
    INNER JOIN condition_occurrence b
        ON b.person_id = a.person_id
       AND b.start_date > a.condition_start_date
       AND a.condition_concept_id <> b.condition_concept_id
    INNER JOIN valid_conditions vc2 ON b.condition_concept_id = vc2.concept_id
    GROUP BY a.condition_concept_id, b.condition_concept_id
    ORDER BY person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS first_condition,
    c2.concept_name AS second_condition
    -- ,cp.person_count
FROM condition_pairs cp
INNER JOIN concept c1 ON c1.concept_id = cp.cond1_id
INNER JOIN concept c2 ON c2.concept_id = cp.cond2_id;
