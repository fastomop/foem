-- Number of patients of race {race}.

SELECT
c.concept_name AS race
--, COUNT(*) AS patient_count
FROM person p
JOIN concept c
ON c.concept_id = p.race_concept_id
WHERE c.domain_id = 'Race'
GROUP BY c.concept_name
ORDER BY COUNT(*) DESC;
