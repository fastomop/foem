SELECT COUNT(DISTINCT pe1.person_id) AS number_of_patients
            FROM person AS pe1
                  JOIN (SELECT concept_id
                        FROM concept
                        WHERE concept_name = %(race)s AND domain_id = 'Race' AND standard_concept = 'S') AS alias1
                        ON pe1.race_concept_id = concept_id;