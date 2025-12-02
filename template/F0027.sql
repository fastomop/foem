-- How many people have condition <ARG-CONDITION><0> in the year <ARG-TIMEYEARS><0>?

WITH 
-- Get source concept for condition
condition_source AS (
    SELECT concept_id
    FROM concept
    WHERE vocabulary_id = %(v_id1)s
    AND concept_code = %(c_id1)s
    AND invalid_reason IS NULL
),

-- Map condition to standard concept
condition_mapped AS (
    SELECT DISTINCT COALESCE(cr.concept_id_2, cs.concept_id) AS concept_id
    FROM condition_source cs
    LEFT JOIN concept_relationship cr 
        ON cs.concept_id = cr.concept_id_1
        AND cr.relationship_id = 'Maps to'
        AND cr.invalid_reason IS NULL
),

-- Get the mapped concept itself plus all descendants
condition_hierarchy AS (
    SELECT concept_id FROM condition_mapped
    UNION
    SELECT DISTINCT ca.descendant_concept_id AS concept_id
    FROM condition_mapped cm
    JOIN concept_ancestor ca ON cm.concept_id = ca.ancestor_concept_id
),

-- Filter to only valid standard condition concepts
condition_concepts AS (
    SELECT ch.concept_id
    FROM condition_hierarchy ch
    JOIN concept c ON ch.concept_id = c.concept_id
    WHERE c.standard_concept = 'S'
    AND c.domain_id = 'Condition'
    AND c.invalid_reason IS NULL
)

SELECT COUNT(DISTINCT con1.person_id) AS number_of_patients
FROM condition_occurrence AS con1
JOIN condition_concepts cc ON con1.condition_concept_id = cc.concept_id
WHERE EXTRACT(year FROM con1.condition_start_date) = %(year)s
AND con1.condition_start_date IS NOT NULL;