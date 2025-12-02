-- How many people have condition {condition_name} in the state {most_common_state}?

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
        l.state AS state_name,
        COUNT(DISTINCT co.person_id) AS n
    FROM condition_occurrence co
    INNER JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
    INNER JOIN person p ON p.person_id = co.person_id
    INNER JOIN location l ON l.location_id = p.location_id
    WHERE l.state IS NOT NULL
    GROUP BY co.condition_concept_id, l.state
),
ranked AS (
    SELECT
        c1.concept_name AS condition_name,
        state_name,
        n,
        RANK() OVER (PARTITION BY condition_concept_id ORDER BY n DESC, state_name) AS rnk,
        SUM(n) OVER (PARTITION BY condition_concept_id) AS total_patients
    FROM counts
    INNER JOIN concept c1 ON c1.concept_id = condition_concept_id
    ORDER BY total_patients DESC
    LIMIT {self.result_limit}
)
SELECT
    condition_name,
    state_name AS most_common_state
    -- ,n AS state_patient_count
    -- ,total_patients
FROM ranked
WHERE rnk = 1;
