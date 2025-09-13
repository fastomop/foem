SELECT
    et.ethnicity,
    st.state,
    COUNT(DISTINCT pe1.person_id)
FROM ((
    person AS pe1 JOIN (
        SELECT
            concept_id,
            concept_name AS ethnicity
        FROM concept
        WHERE domain_id = 'Ethnicity' AND standard_concept = 'S'
    ) AS et
        ON pe1.ethnicity_concept_id = et.concept_id
) JOIN (SELECT
    location_id,
    state
FROM location) AS st
    ON pe1.location_id = st.location_id
)
GROUP BY et.ethnicity, st.state;
