SELECT
    pe1.year_of_birth,
    st.state,
    COUNT(DISTINCT pe1.person_id)
FROM person AS pe1
JOIN
    (SELECT
        location_id,
        state
    FROM location) AS st
    ON pe1.location_id = st.location_id
GROUP BY pe1.year_of_birth, st.state;
