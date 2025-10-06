WITH 
state_locations AS (
    SELECT location_id 
    FROM location 
    WHERE state = %(location)s
)

SELECT COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
JOIN state_locations sl ON pe1.location_id = sl.location_id;