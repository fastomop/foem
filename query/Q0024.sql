-- How many people were diagnosed with condition {condition_name} in year {most_common_year}?

WITH valid_conditions AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Condition'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
counts AS (
    SELECT
        condition_concept_id,
        EXTRACT(YEAR FROM condition_start_date)::int AS year,
        COUNT(DISTINCT person_id) AS n
    FROM condition_occurrence
    INNER JOIN valid_conditions vc ON vc.concept_id = condition_concept_id
    WHERE condition_start_date IS NOT NULL
    GROUP BY condition_concept_id, EXTRACT(YEAR FROM condition_start_date)::int
),
ranked AS (
    SELECT
        c1.concept_name AS condition_name,
        year,
        n,
        RANK() OVER (PARTITION BY condition_concept_id ORDER BY n DESC, year) AS rnk,
        SUM(n) OVER (PARTITION BY condition_concept_id) AS total_patients
    FROM counts
    INNER JOIN concept c1 ON c1.concept_id = condition_concept_id
)
SELECT
    condition_name,
    year AS most_common_year
    -- ,n AS year_patient_count
    -- ,total_patients
FROM ranked
WHERE rnk = 1
ORDER BY total_patients DESC
LIMIT {self.result_limit};
