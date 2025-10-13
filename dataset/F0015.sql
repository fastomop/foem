-- Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, <ARG-CONDITION><2> and <ARG-CONDITION><3> within <ARG-TIMEDAYS><0> days.

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
            AND c.standard_concept = 'S'
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
            AND c.standard_concept = 'S'
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
            AND c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_d AS (
            SELECT c.concept_id AS src_id
            FROM concept c
            WHERE c.vocabulary_id = %(v_id4)s
            AND c.concept_code  = %(c_id4)s
            AND c.invalid_reason IS NULL
            ),
            std_d AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_d s
            LEFT JOIN concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_d AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_d sd
            JOIN concept_ancestor ca
            ON ca.ancestor_concept_id = sd.standard_id
            JOIN concept c
            ON c.concept_id = ca.descendant_concept_id
            AND c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            occ AS (
            SELECT co.person_id, co.condition_start_date::date AS start_date, 'A'::text AS grp
            FROM condition_occurrence co
            JOIN desc_a da ON co.condition_concept_id = da.concept_id
            UNION ALL
            SELECT co.person_id, co.condition_start_date::date AS start_date, 'B'
            FROM condition_occurrence co
            JOIN desc_b db ON co.condition_concept_id = db.concept_id
            UNION ALL
            SELECT co.person_id, co.condition_start_date::date AS start_date, 'C'
            FROM condition_occurrence co
            JOIN desc_c dc ON co.condition_concept_id = dc.concept_id
            UNION ALL
            SELECT co.person_id, co.condition_start_date::date AS start_date, 'D'
            FROM condition_occurrence co
            JOIN desc_d dd ON co.condition_concept_id = dd.concept_id
            ),
            persons_with_all_4 AS (
            SELECT o1.person_id
            FROM occ o1
            JOIN occ o2
            ON o2.person_id = o1.person_id
            AND o2.start_date >= o1.start_date
            AND (o2.start_date - o1.start_date) <= %(days)s::int
            GROUP BY o1.person_id, o1.start_date
            HAVING COUNT(DISTINCT o2.grp) = 4
            )
            SELECT COUNT(DISTINCT person_id)
            FROM persons_with_all_4;