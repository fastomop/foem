-- Counts of patients with condition <ARG-CONDITION><0> or <ARG-CONDITION><1>.

WITH
            seeds AS (
            SELECT %(v_id1)s::text AS vocabulary_id, %(c_id1)s::text AS concept_code
            UNION ALL
            SELECT %(v_id2)s::text,       %(c_id2)s::text
            ),
            seed_concepts AS (
            SELECT c.concept_id AS src_id
            FROM concept c
            JOIN seeds s
            ON s.vocabulary_id = c.vocabulary_id
            AND s.concept_code  = c.concept_code
            WHERE c.invalid_reason IS NULL
            ),
            std AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, sc.src_id) AS standard_id
            FROM seed_concepts sc
            LEFT JOIN concept_relationship cr
            ON cr.concept_id_1 = sc.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            descendants AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std
            JOIN concept_ancestor ca
            ON ca.ancestor_concept_id = std.standard_id
            ),
            valid_condition_desc AS (
            SELECT d.concept_id
            FROM descendants d
            JOIN concept c
            ON c.concept_id = d.concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            )
            SELECT COUNT(DISTINCT co.person_id)
            FROM condition_occurrence co
            JOIN valid_condition_desc vcd
            ON co.condition_concept_id = vcd.concept_id;