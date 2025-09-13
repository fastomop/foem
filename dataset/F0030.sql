SELECT COUNT(DISTINCT pe1.person_id)
            FROM ((person AS pe1 JOIN (SELECT concept_id
                                          FROM concept
                                          WHERE concept_name = %(ethnicity)s
                                                AND domain_id = 'Ethnicity'
                                                AND standard_concept = 'S') AS alias1
                  ON pe1.ethnicity_concept_id = concept_id) JOIN ((SELECT descendant_concept_id AS concept_id
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
                                                                        JOIN concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci" JOIN condition_occurrence AS con1
                                                                  ON concept_id = con1.condition_concept_id)
                  ON pe1.person_id = con1.person_id);