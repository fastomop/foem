-- Number of {most_common_gender} patients with {condition_name}.

WITH valid_conditions AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Condition'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
counts AS (
    SELECT
        co.condition_concept_id,
        COALESCE(gc.concept_name, 'Unknown') AS gender_name,
        COUNT(DISTINCT co.person_id) AS n
    FROM condition_occurrence co
    INNER JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
    INNER JOIN person p ON p.person_id = co.person_id
    LEFT JOIN concept gc
        ON gc.concept_id = p.gender_concept_id
       AND gc.domain_id = 'Gender'
       AND gc.standard_concept = 'S'
    GROUP BY co.condition_concept_id, COALESCE(gc.concept_name, 'Unknown')
),
ranked AS (
    SELECT
        c1.concept_name AS condition_name,
        gender_name,
        n,
        RANK() OVER (PARTITION BY condition_concept_id ORDER BY n DESC, gender_name) AS rnk,
        SUM(n) OVER (PARTITION BY condition_concept_id) AS total_patients
    FROM counts
    INNER JOIN concept c1 ON c1.concept_id = condition_concept_id
)
SELECT
    gender_name AS most_common_gender,
    condition_name
    -- ,n AS gender_patient_count
    -- ,total_patients
FROM ranked
WHERE rnk = 1
ORDER BY total_patients DESC
LIMIT {self.result_limit};
