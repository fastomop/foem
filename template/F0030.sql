-- How many people of ethnicity <ARG-ETHNICITY><0> have condition <ARG-CONDITION><0>?

WITH 
-- Get ethnicity concept
ethnicity_concept AS (
    SELECT concept_id
    FROM concept
    WHERE concept_name = %(ethnicity)s
    AND domain_id = 'Ethnicity'
    AND standard_concept = 'S'
),

-- Get source concept for condition
condition_source AS (
    SELECT concept_id
    FROM concept
    WHERE vocabulary_id = %(v_id1)s
    AND concept_code = %(c_id1)s
),

-- Map condition to standard concept
condition_mapped AS (
    SELECT concept_id_2 AS concept_id
    FROM condition_source cs
    JOIN concept_relationship cr ON cs.concept_id = cr.concept_id_1
    WHERE cr.relationship_id = 'Maps to'
),

-- Get all descendant concepts for condition
condition_concepts AS (
    SELECT DISTINCT ca.descendant_concept_id AS concept_id
    FROM condition_mapped cm
    JOIN concept c ON cm.concept_id = c.concept_id
    JOIN concept_ancestor ca ON c.concept_id = ca.ancestor_concept_id
)

SELECT COUNT(DISTINCT pe1.person_id)
FROM person pe1
JOIN ethnicity_concept ec ON pe1.ethnicity_concept_id = ec.concept_id
JOIN condition_occurrence con1 ON pe1.person_id = con1.person_id
JOIN condition_concepts cc ON con1.condition_concept_id = cc.concept_id;