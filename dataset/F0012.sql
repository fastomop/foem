WITH seed_a AS
            (SELECT c.concept_id AS src_id
            FROM concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code = %(c_id1)s
            AND c.invalid_reason IS NULL),
            std_a AS
            (SELECT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_a s
            LEFT JOIN concept_relationship cr ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL),
            desc_a AS
            (SELECT ca.descendant_concept_id AS concept_id
            FROM std_a sa
            JOIN concept_ancestor ca ON ca.ancestor_concept_id = sa.standard_id),
            a AS
            (SELECT con1.person_id,
                  con1.condition_start_date::date AS start_date
            FROM condition_occurrence con1
            JOIN desc_a ON con1.condition_concept_id = desc_a.concept_id),
            seed_b AS
            (SELECT c.concept_id AS src_id
            FROM concept c
            WHERE c.vocabulary_id = %(v_id2)s
            AND c.concept_code = %(c_id2)s
            AND c.invalid_reason IS NULL),
            std_b AS
            (SELECT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_b s
            LEFT JOIN concept_relationship cr ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL),
            desc_b AS
            (SELECT ca.descendant_concept_id AS concept_id
            FROM std_b sb
            JOIN concept_ancestor ca ON ca.ancestor_concept_id = sb.standard_id),
            b AS
            (SELECT con2.person_id,
                  con2.condition_start_date::date AS start_date
            FROM condition_occurrence con2
            JOIN desc_b ON con2.condition_concept_id = desc_b.concept_id)
            SELECT COUNT(DISTINCT a.person_id)
            FROM a
            JOIN b ON a.person_id = b.person_id
            WHERE ABS(a.start_date - b.start_date) <= %(days)s;