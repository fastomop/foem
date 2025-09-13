SELECT
    pe1.year_of_birth,
    COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
GROUP BY pe1.year_of_birth;
