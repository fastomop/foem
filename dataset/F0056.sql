WITH 
-- Get source concept for drug
drug_source AS (
    SELECT concept_id
    FROM concept
    WHERE vocabulary_id = %(v_id1)s
    AND concept_code = %(d_id1)s
),

-- Map drug to standard concept
drug_mapped AS (
    SELECT concept_id_2 AS concept_id
    FROM drug_source ds
    JOIN concept_relationship cr ON ds.concept_id = cr.concept_id_1
    WHERE cr.relationship_id = 'Maps to'
),

-- Get all descendant concepts for drug
drug_concepts AS (
    SELECT DISTINCT ca.descendant_concept_id AS concept_id
    FROM drug_mapped dm
    JOIN concept c ON dm.concept_id = c.concept_id
    JOIN concept_ancestor ca ON c.concept_id = ca.ancestor_concept_id
)

SELECT EXTRACT(year FROM dr1.drug_exposure_start_date) AS year, COUNT(DISTINCT dr1.person_id)
FROM drug_exposure AS dr1
JOIN drug_concepts dc ON dr1.drug_concept_id = dc.concept_id
GROUP BY EXTRACT(year FROM dr1.drug_exposure_start_date);