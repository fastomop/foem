-- How many people have condition <ARG-CONDITION><0> in the cohort of race <ARG-RACE><0>?

WITH
            race AS (
            SELECT c.concept_id
            FROM concept c
            WHERE c.domain_id = 'Race'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
            AND (
                  lower(c.concept_name) = lower(%(race)s)
                  OR EXISTS (
                  SELECT 1
                  FROM concept_synonym cs
                  WHERE cs.concept_id = c.concept_id
                  AND lower(cs.concept_synonym_name) = lower(%(race)s)
                  )
            )
            ),
            seed AS (
            SELECT c.concept_id AS src_id
            FROM concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code  = %(c_id1)s
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
            cond_concepts AS (
            SELECT DISTINCT c.concept_id
            FROM (
            SELECT standard_id AS concept_id FROM std
            UNION
            SELECT ca.descendant_concept_id
            FROM std
            JOIN concept_ancestor ca
                  ON ca.ancestor_concept_id = std.standard_id
            ) x
            JOIN concept c ON c.concept_id = x.concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            )

            SELECT COUNT(DISTINCT p.person_id)
            FROM person p
            JOIN race r
            ON p.race_concept_id = r.concept_id
            JOIN condition_occurrence co
            ON co.person_id = p.person_id
            JOIN cond_concepts cc
            ON co.condition_concept_id = cc.concept_id;