SELECT COUNT(DISTINCT pe1.person_id)
            FROM person AS pe1
                  JOIN (SELECT concept_id
                        FROM concept
                        WHERE concept_name = %(gender)s AND domain_id = 'Gender' AND standard_concept = 'S') AS alias1
                        ON pe1.gender_concept_id = concept_id;