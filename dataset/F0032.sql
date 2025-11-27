-- How many people were treated by drug <ARG-DRUG><0> after the diagnosis of Condition <ARG-CONDITION><0>?

WITH 
condition_source AS (
    SELECT concept_id 
    FROM concept 
    WHERE vocabulary_id = %(v_id1)s 
    AND concept_code = %(c_id1)s
),

condition_mapped AS (
    SELECT concept_id_2 AS concept_id
    FROM condition_source cs
    JOIN concept_relationship cr ON cs.concept_id = cr.concept_id_1
    WHERE cr.relationship_id = 'Maps to'
),

condition_concepts AS (
    SELECT DISTINCT ca.descendant_concept_id AS concept_id
    FROM condition_mapped cm
    JOIN concept c ON cm.concept_id = c.concept_id
    JOIN concept_ancestor ca ON c.concept_id = ca.ancestor_concept_id
),

drug_source AS (
    SELECT concept_id 
    FROM concept 
    WHERE vocabulary_id = %(v_id2)s 
    AND concept_code = %(d_id1)s
),

drug_mapped AS (
    SELECT concept_id_2 AS concept_id
    FROM drug_source ds
    JOIN concept_relationship cr ON ds.concept_id = cr.concept_id_1
    WHERE cr.relationship_id = 'Maps to'
),

-- Only use the mapped concept itself, not its descendants
drug_concepts AS (
    SELECT concept_id
    FROM drug_mapped
),

cond_occ AS (
    SELECT co.person_id, 
           co.condition_concept_id, 
           co.condition_start_date::date AS cond_date
    FROM condition_occurrence co
    JOIN condition_concepts cc ON co.condition_concept_id = cc.concept_id
),

drug_exp AS (
    SELECT de.person_id, 
           de.drug_concept_id, 
           de.drug_exposure_start_date::date AS drug_date
    FROM drug_exposure de
    JOIN drug_concepts dc ON de.drug_concept_id = dc.concept_id
),

pairs AS (
    SELECT DISTINCT co.person_id, 
                    co.condition_concept_id, 
                    de.drug_concept_id
    FROM cond_occ co
    JOIN drug_exp de ON de.person_id = co.person_id 
                    AND de.drug_date > co.cond_date
)

SELECT COUNT(DISTINCT person_id)
FROM pairs;