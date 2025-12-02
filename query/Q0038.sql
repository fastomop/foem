-- Number of patients with residence state location at {location}.

SELECT
    l.state AS location,
    COUNT(DISTINCT p.person_id) AS patient_count
FROM person p
INNER JOIN location l ON l.location_id = p.location_id
WHERE l.state IS NOT NULL
GROUP BY l.state
ORDER BY patient_count DESC
LIMIT {self.result_limit};
