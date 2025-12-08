-- How many people have condition {condition_b_name} more than 30 days after diagnosed by condition {condition_a_name}?

WITH valid_conditions AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Condition'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
pairs AS (
    SELECT
        o1.condition_concept_id AS cond_a,
        o2.condition_concept_id AS cond_b,
        COUNT(DISTINCT o1.person_id) AS person_count
    FROM condition_occurrence o1
    INNER JOIN valid_conditions vc1 ON o1.condition_concept_id = vc1.concept_id
    INNER JOIN condition_occurrence o2
        ON o2.person_id = o1.person_id
       AND o2.condition_concept_id <> o1.condition_concept_id
       AND (o2.condition_start_date - o1.condition_start_date) >= 30
    INNER JOIN valid_conditions vc2 ON o2.condition_concept_id = vc2.concept_id
    GROUP BY o1.condition_concept_id, o2.condition_concept_id
    ORDER BY person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS condition_a_name,
    c2.concept_name AS condition_b_name
FROM pairs p
INNER JOIN concept c1 ON c1.concept_id = p.cond_a
INNER JOIN concept c2 ON c2.concept_id = p.cond_b;
