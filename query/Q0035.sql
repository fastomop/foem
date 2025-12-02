-- Number of patients of specific gender {gender}.

SELECT
COALESCE(c.concept_name, 'Unknown') AS gender
-- ,COUNT(*) AS patient_count
FROM person p
LEFT JOIN concept c
ON c.concept_id = p.gender_concept_id
AND c.domain_id = 'Gender'
AND c.standard_concept = 'S'
AND c.invalid_reason IS NULL
GROUP BY COALESCE(c.concept_name, 'Unknown')
ORDER BY COUNT(*) DESC;
