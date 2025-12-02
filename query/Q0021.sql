-- How many people have condition {condition_name} at age {most_common_age}?

WITH valid_conditions AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Condition'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
condition_ages AS (
    SELECT
        co.condition_concept_id,
        FLOOR(EXTRACT(YEAR FROM co.condition_start_date) - p.year_of_birth) AS age,
        co.person_id
    FROM condition_occurrence co
    INNER JOIN valid_conditions vc ON co.condition_concept_id = vc.concept_id
    INNER JOIN person p ON p.person_id = co.person_id
    WHERE FLOOR(EXTRACT(YEAR FROM co.condition_start_date) - p.year_of_birth) BETWEEN 0 AND 120
),
counts AS (
    SELECT
        condition_concept_id,
        age,
        COUNT(*) AS n
    FROM condition_ages
    GROUP BY condition_concept_id, age
),
ranked AS (
    SELECT
        c1.concept_name,
        ca.age,
        ca.n,
        RANK() OVER (PARTITION BY ca.condition_concept_id ORDER BY ca.n DESC) AS rnk,
        SUM(ca.n) OVER (PARTITION BY ca.condition_concept_id) AS total_condition_count
    FROM counts ca
    INNER JOIN concept c1 ON c1.concept_id = ca.condition_concept_id
    ORDER BY total_condition_count DESC
    LIMIT {self.result_limit}
)
SELECT
    concept_name AS condition_name,
    age AS most_common_age
    -- ,n AS age_count
    -- ,total_condition_count
FROM ranked
WHERE rnk = 1;
