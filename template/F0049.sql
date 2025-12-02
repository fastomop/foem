-- Number of patients of ethnicity <ARG-ETHNICITY><0>.

WITH 
ethnicity_concepts AS (
    SELECT concept_id
    FROM concept
    WHERE concept_name = %(ethnicity)s
    AND domain_id = 'Ethnicity'
    AND standard_concept = 'S'
)

SELECT COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
JOIN ethnicity_concepts ec ON pe1.ethnicity_concept_id = ec.concept_id;