SELECT COUNT(DISTINCT pe1.person_id)
            FROM ((person AS pe1 JOIN (SELECT concept_id
                                          FROM concept
                                          WHERE concept_name = %(gender)s
                                                AND domain_id = 'Gender'
                                                AND standard_concept = 'S') AS alias1
                  ON pe1.gender_concept_id = concept_id) JOIN (condition_occurrence AS co JOIN (SELECT descendant_concept_id AS concept_id
                                                                                                      FROM (SELECT *
                                                                                                            FROM (SELECT concept_id_2
                                                                                                                  FROM ((SELECT concept_id
                                                                                                                        FROM concept
                                                                                                                        WHERE vocabulary_id = %(v_id1)s
                                                                                                                        AND (concept_code = %(c_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                            FROM concept_relationship
                                                                                                                                                            WHERE relationship_id = 'Maps to') AS alias2
                                                                                                                        ON concept_id = concept_id_1)) as c
                                                                                                                  JOIN concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                                                            JOIN concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                                                            ON co.condition_concept_id = concept_id) AS pe2
                  ON pe1.person_id = pe2.person_id);