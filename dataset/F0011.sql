SELECT COUNT(DISTINCT dr1.person_id)
            FROM (drug_exposure AS dr1 JOIN (((SELECT descendant_concept_id AS concept_id
                                                      FROM (SELECT *
                                                            FROM (SELECT concept_id_2
                                                                  FROM ((SELECT concept_id
                                                                        FROM concept
                                                                        WHERE vocabulary_id = %(v_id1)s
                                                                        AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias1
                                                                        ON concept_id = concept_id_1)) as c
                                                                  JOIN concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                            JOIN concept_ancestor ON concept_id = ancestor_concept_id)
                                                UNION
                                                (SELECT descendant_concept_id AS concept_id
                                                      FROM (SELECT *
                                                            FROM (SELECT concept_id_2
                                                                  FROM ((SELECT concept_id
                                                                        FROM concept
                                                                        WHERE vocabulary_id = %(v_id2)s
                                                                        AND (concept_code = %(d_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias2
                                                                        ON concept_id = concept_id_1)) as i
                                                                  JOIN concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                            JOIN concept_ancestor ON concept_id = ancestor_concept_id))
                                                UNION
                                                (SELECT descendant_concept_id AS concept_id
                                                FROM (SELECT *
                                                      FROM (SELECT concept_id_2
                                                            FROM ((SELECT concept_id
                                                                        FROM concept
                                                                        WHERE vocabulary_id = %(v_id3)s
                                                                        AND (concept_code = %(d_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias3
                                                                  ON concept_id = concept_id_1)) as ccia3ci2
                                                                  JOIN concept ON concept_id_2 = concept_id) as "ccia3ci2c*"
                                                            JOIN concept_ancestor ON concept_id = ancestor_concept_id)) as "ccia1ci2c*caciccia2ci2c*caciccia3ci2c*caci"
                  ON dr1.drug_concept_id = concept_id);