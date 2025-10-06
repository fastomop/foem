SELECT COUNT(DISTINCT pe1.person_id)
            FROM person AS pe1
            WHERE year_of_birth = %(year)s;