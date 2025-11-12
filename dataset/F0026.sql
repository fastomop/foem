-- How many people have condition <ARG-CONDITION><0> in the state <ARG-STATE><0>?

WITH
    seed AS (
        SELECT c.concept_id AS src_id
        FROM concept c
        WHERE c.vocabulary_id = %(v_id1)s
        AND c.concept_code = %(c_id1)s
        AND c.invalid_reason IS NULL
    ),
    std AS (
        SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
        FROM seed s
        LEFT JOIN concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
    ),
    concept_ids AS (
        SELECT standard_id AS concept_id FROM std
        UNION
        SELECT ca.descendant_concept_id
        FROM std
        JOIN concept_ancestor ca
            ON ca.ancestor_concept_id = std.standard_id
    ),
    cond_concepts AS (
        SELECT ci.concept_id
        FROM concept_ids ci
        JOIN concept c
            ON c.concept_id = ci.concept_id
        WHERE c.standard_concept = 'S'
        AND c.domain_id = 'Condition'
        AND c.invalid_reason IS NULL
    )
SELECT COUNT(DISTINCT p.person_id) AS number_of_patients
FROM person p
JOIN location l
    ON l.location_id = p.location_id
JOIN condition_occurrence co
    ON co.person_id = p.person_id
JOIN cond_concepts cc
    ON co.condition_concept_id = cc.concept_id
WHERE l.state IS NOT NULL
    AND TRIM(l.state) != ''
    AND UPPER(TRIM(l.state)) != 'UNKNOWN'
    AND UPPER(TRIM(l.state)) = UPPER(TRIM(%(state)s));