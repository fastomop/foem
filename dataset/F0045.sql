-- Number of patients grouped by residence state location.

WITH st AS (
    SELECT
        location_id,
        state
    FROM location
)

SELECT
    st.state,
    COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
INNER JOIN
    st
    ON pe1.location_id = st.location_id
GROUP BY st.state;
