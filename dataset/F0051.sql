-- Number of patients of specific gender <ARG-GENDER><0>.

WITH 
gender_concepts AS (
    SELECT concept_id
    FROM concept
    WHERE concept_name = %(gender)s 
    AND domain_id = 'Gender' 
    AND standard_concept = 'S'
)

SELECT COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
JOIN gender_concepts gc ON pe1.gender_concept_id = gc.concept_id;