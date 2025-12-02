-- Counts of patients with condition {condition_name} grouped by year of diagnosis.

WITH valid_conditions AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Condition'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
yearly_counts AS (
    SELECT
        EXTRACT(YEAR FROM condition_start_date)::int AS year,
        condition_concept_id,
        COUNT(DISTINCT person_id) AS patient_count
    FROM condition_occurrence
    INNER JOIN valid_conditions vc ON vc.concept_id = condition_concept_id
    GROUP BY EXTRACT(YEAR FROM condition_start_date)::int, condition_concept_id
),
ranked AS (
    SELECT
        yc.year,
        yc.condition_concept_id,
        yc.patient_count,
        ROW_NUMBER() OVER (
            PARTITION BY yc.year
            ORDER BY yc.patient_count DESC, yc.condition_concept_id
        ) AS rnum
    FROM yearly_counts yc
    WHERE rnum <= 20
    ORDER BY year, patient_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c.concept_name AS condition_name,
    r.year
    -- ,r.patient_count
FROM ranked r
INNER JOIN concept c ON c.concept_id = r.condition_concept_id;
