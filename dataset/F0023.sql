-- How many people have Condition <ARG-CONDITION><0> more than <ARG-TIMEDAYS><0> days after diagnosed by Condition <ARG-CONDITION><1>?

WITH 
-- Get source concept for first condition
condition1_source AS (
    SELECT concept_id
    FROM concept
    WHERE vocabulary_id = %(v_id1)s
    AND concept_code = %(c_id1)s
),

-- Map first condition to standard concept
condition1_mapped AS (
    SELECT concept_id_2 AS concept_id
    FROM condition1_source cs
    JOIN concept_relationship cr ON cs.concept_id = cr.concept_id_1
    WHERE cr.relationship_id = 'Maps to'
),

-- Get all descendant concepts for first condition
condition1_concepts AS (
    SELECT DISTINCT ca.descendant_concept_id AS concept_id
    FROM condition1_mapped cm
    JOIN concept c ON cm.concept_id = c.concept_id
    JOIN concept_ancestor ca ON c.concept_id = ca.ancestor_concept_id
),

-- Get source concept for second condition
condition2_source AS (
    SELECT concept_id
    FROM concept
    WHERE vocabulary_id = %(v_id2)s
    AND concept_code = %(c_id2)s
),

-- Map second condition to standard concept
condition2_mapped AS (
    SELECT concept_id_2 AS concept_id
    FROM condition2_source cs
    JOIN concept_relationship cr ON cs.concept_id = cr.concept_id_1
    WHERE cr.relationship_id = 'Maps to'
),

-- Get all descendant concepts for second condition
condition2_concepts AS (
    SELECT DISTINCT ca.descendant_concept_id AS concept_id
    FROM condition2_mapped cm
    JOIN concept c ON cm.concept_id = c.concept_id
    JOIN concept_ancestor ca ON c.concept_id = ca.ancestor_concept_id
)

SELECT COUNT(DISTINCT con1.person_id)
FROM condition_occurrence AS con1
JOIN condition1_concepts cc1 ON con1.condition_concept_id = cc1.concept_id
JOIN condition_occurrence AS con2 ON con1.person_id = con2.person_id
JOIN condition2_concepts cc2 ON con2.condition_concept_id = cc2.concept_id
WHERE CAST(EXTRACT(epoch FROM
    CAST(con1.condition_start_date AS TIMESTAMP) -
    CAST(con2.condition_start_date AS TIMESTAMP)) / 86400 AS BIGINT) > %(days)s;