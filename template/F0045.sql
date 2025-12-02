-- Number of patients grouped by residence state location.

WITH st AS (
    SELECT
        location_id,
        state
    FROM location
)

SELECT
    COALESCE(st.state, 'Unknown') AS state,
    COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
LEFT JOIN
    st
    ON pe1.location_id = st.location_id
GROUP BY st.state;
