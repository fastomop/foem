-- Number of patients of ethnicity {ethnicity}.

SELECT
COALESCE(c.concept_name, 'Unknown') AS ethnicity
--, COUNT(*) AS patient_count
FROM person p
LEFT JOIN concept c
ON c.concept_id = p.ethnicity_concept_id
GROUP BY c.concept_name
ORDER BY COUNT(*) DESC;
