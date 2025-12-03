-- How many people have condition {condition_name} in the cohort of race {most_common_race}?

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
        COALESCE(rc.concept_name, 'Unknown') AS race_name,
        COUNT(DISTINCT co.person_id) AS n
    FROM condition_occurrence co
    INNER JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
    INNER JOIN person p ON p.person_id = co.person_id
    LEFT JOIN concept rc ON rc.concept_id = p.race_concept_id
    GROUP BY co.condition_concept_id, COALESCE(rc.concept_name, 'Unknown')
),
ranked AS (
    SELECT
        c1.concept_name AS condition_name,
        race_name,
        n,
        RANK() OVER (PARTITION BY condition_concept_id ORDER BY n DESC, race_name) AS rnk,
        SUM(n) OVER (PARTITION BY condition_concept_id) AS total_patients
    FROM counts
    INNER JOIN concept c1 ON c1.concept_id = condition_concept_id
)
SELECT
    condition_name,
    race_name AS most_common_race
    -- ,n AS race_patient_count
    -- ,total_patients
FROM ranked
WHERE rnk = 1
ORDER BY total_patients DESC
LIMIT {self.result_limit};
