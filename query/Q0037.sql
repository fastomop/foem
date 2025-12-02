-- Number of patients with {condition_name}.

WITH valid_conditions AS (
    SELECT concept_id, concept_name
    FROM concept
    WHERE domain_id = 'Condition'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
)
SELECT
    vc.concept_name AS condition_name
    -- ,COUNT(DISTINCT co.person_id) AS patient_count
FROM condition_occurrence co
INNER JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
GROUP BY vc.concept_name
ORDER BY COUNT(DISTINCT co.person_id) DESC
LIMIT {self.result_limit};
