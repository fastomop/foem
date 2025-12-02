-- Number of patients born in year {year}.

SELECT
p.year_of_birth AS year
-- , COUNT(*)        AS patient_count
FROM person p
GROUP BY p.year_of_birth
ORDER BY COUNT(*) DESC, year
LIMIT {self.result_limit};
