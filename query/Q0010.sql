-- Counts of patients with condition {condition1_name} and {condition2_name} within 30 days.

WITH valid_conditions AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Condition'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
pair_counts AS (
    SELECT
        LEAST(o1.condition_concept_id, o2.condition_concept_id) AS cond1_id,
        GREATEST(o1.condition_concept_id, o2.condition_concept_id) AS cond2_id,
        COUNT(DISTINCT o1.person_id) AS patient_count
    FROM condition_occurrence o1
    INNER JOIN valid_conditions vc1 ON o1.condition_concept_id = vc1.concept_id
    INNER JOIN condition_occurrence o2
        ON o1.person_id = o2.person_id
       AND o1.condition_concept_id <> o2.condition_concept_id
       AND ABS(o1.condition_start_date - o2.condition_start_date) <= 30
    INNER JOIN valid_conditions vc2 ON o2.condition_concept_id = vc2.concept_id
    GROUP BY LEAST(o1.condition_concept_id, o2.condition_concept_id),
             GREATEST(o1.condition_concept_id, o2.condition_concept_id)
    ORDER BY patient_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS condition1_name,
    c2.concept_name AS condition2_name
FROM pair_counts pc
INNER JOIN concept c1 ON c1.concept_id = pc.cond1_id
INNER JOIN concept c2 ON c2.concept_id = pc.cond2_id;
