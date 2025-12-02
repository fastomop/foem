-- Number of patients taking {drug_name}.

WITH valid_drugs AS (
    SELECT concept_id, concept_name
    FROM concept
    WHERE domain_id = 'Drug'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
)
SELECT
    vd.concept_name AS drug_name
    -- ,COUNT(DISTINCT de.person_id) AS patient_count
FROM drug_exposure de
INNER JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
GROUP BY vd.concept_name
ORDER BY COUNT(DISTINCT de.person_id) DESC
LIMIT {self.result_limit};
