WITH
            seed_a AS (
            SELECT c.concept_id AS src_id
            FROM concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code  = %(c_id1)s
            AND c.invalid_reason IS NULL
            ),
            std_a AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_a s
            LEFT JOIN concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_a AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_a sa
            JOIN concept_ancestor ca
            ON ca.ancestor_concept_id = sa.standard_id
            JOIN concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_b AS (
            SELECT c.concept_id AS src_id
            FROM concept c
            WHERE c.vocabulary_id = %(v_id2)s
            AND c.concept_code  = %(c_id2)s
            AND c.invalid_reason IS NULL
            ),
            std_b AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_b s
            LEFT JOIN concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_b AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_b sb
            JOIN concept_ancestor ca
            ON ca.ancestor_concept_id = sb.standard_id
            JOIN concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_c AS (
            SELECT c.concept_id AS src_id
            FROM concept c
            WHERE c.vocabulary_id = %(v_id3)s
            AND c.concept_code  = %(c_id3)s
            AND c.invalid_reason IS NULL
            ),
            std_c AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_c s
            LEFT JOIN concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_c AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_c sc
            JOIN concept_ancestor ca
            ON ca.ancestor_concept_id = sc.standard_id
            JOIN concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            )
            SELECT COUNT(*)
            FROM (
            SELECT DISTINCT co.person_id
            FROM condition_occurrence co
            JOIN desc_a da ON co.condition_concept_id = da.concept_id

            INTERSECT

            SELECT DISTINCT co.person_id
            FROM condition_occurrence co
            JOIN desc_b db ON co.condition_concept_id = db.concept_id

            INTERSECT

            SELECT DISTINCT co.person_id
            FROM condition_occurrence co
            JOIN desc_c dc ON co.condition_concept_id = dc.concept_id
            ) ppl;