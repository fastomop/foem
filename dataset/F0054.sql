SELECT COUNT(DISTINCT pe1.person_id)
            FROM person AS pe1
                  JOIN (SELECT location_id FROM location WHERE state = %(location)s) AS state_temp1
                        ON pe1.location_id = state_temp1.location_id;