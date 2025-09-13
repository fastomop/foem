SELECT
    rt.race,
    st.state,
    COUNT(DISTINCT pe1.person_id)
FROM ((
    person AS pe1 INNER JOIN (
        SELECT
            concept_id,
            concept_name AS race
        FROM concept
        WHERE domain_id = 'Race' AND standard_concept = 'S'
    ) AS rt
        ON pe1.race_concept_id = rt.concept_id
) INNER JOIN (SELECT
    location_id,
    state
FROM location) AS st
    ON pe1.location_id = st.location_id
)
GROUP BY rt.race, st.state;
