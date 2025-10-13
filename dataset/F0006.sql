-- Counts of patients taking drug <ARG-DRUG><0>, <ARG-DRUG><1>, <ARG-DRUG><2> and <ARG-DRUG><3> within <ARG-TIMEDAYS><0> days.

WITH 
-- Get source concept for first drug
drug1_source AS (
    SELECT concept_id
    FROM concept
    WHERE vocabulary_id = %(v_id1)s
    AND concept_code = %(d_id1)s
),

-- Map first drug to standard concept
drug1_mapped AS (
    SELECT concept_id_2 AS concept_id
    FROM drug1_source ds
    JOIN concept_relationship cr ON ds.concept_id = cr.concept_id_1
    WHERE cr.relationship_id = 'Maps to'
),

-- Get all descendant concepts for first drug
drug1_concepts AS (
    SELECT DISTINCT ca.descendant_concept_id AS concept_id
    FROM drug1_mapped dm
    JOIN concept c ON dm.concept_id = c.concept_id
    JOIN concept_ancestor ca ON c.concept_id = ca.ancestor_concept_id
),

-- Get source concept for second drug
drug2_source AS (
    SELECT concept_id
    FROM concept
    WHERE vocabulary_id = %(v_id2)s
    AND concept_code = %(d_id2)s
),

-- Map second drug to standard concept
drug2_mapped AS (
    SELECT concept_id_2 AS concept_id
    FROM drug2_source ds
    JOIN concept_relationship cr ON ds.concept_id = cr.concept_id_1
    WHERE cr.relationship_id = 'Maps to'
),

-- Get all descendant concepts for second drug
drug2_concepts AS (
    SELECT DISTINCT ca.descendant_concept_id AS concept_id
    FROM drug2_mapped dm
    JOIN concept c ON dm.concept_id = c.concept_id
    JOIN concept_ancestor ca ON c.concept_id = ca.ancestor_concept_id
),

-- Get source concept for third drug
drug3_source AS (
    SELECT concept_id
    FROM concept
    WHERE vocabulary_id = %(v_id3)s
    AND concept_code = %(d_id3)s
),

-- Map third drug to standard concept
drug3_mapped AS (
    SELECT concept_id_2 AS concept_id
    FROM drug3_source ds
    JOIN concept_relationship cr ON ds.concept_id = cr.concept_id_1
    WHERE cr.relationship_id = 'Maps to'
),

-- Get all descendant concepts for third drug
drug3_concepts AS (
    SELECT DISTINCT ca.descendant_concept_id AS concept_id
    FROM drug3_mapped dm
    JOIN concept c ON dm.concept_id = c.concept_id
    JOIN concept_ancestor ca ON c.concept_id = ca.ancestor_concept_id
),

-- Get source concept for fourth drug
drug4_source AS (
    SELECT concept_id
    FROM concept
    WHERE vocabulary_id = %(v_id4)s
    AND concept_code = %(d_id4)s
),

-- Map fourth drug to standard concept
drug4_mapped AS (
    SELECT concept_id_2 AS concept_id
    FROM drug4_source ds
    JOIN concept_relationship cr ON ds.concept_id = cr.concept_id_1
    WHERE cr.relationship_id = 'Maps to'
),

-- Get all descendant concepts for fourth drug
drug4_concepts AS (
    SELECT DISTINCT ca.descendant_concept_id AS concept_id
    FROM drug4_mapped dm
    JOIN concept c ON dm.concept_id = c.concept_id
    JOIN concept_ancestor ca ON c.concept_id = ca.ancestor_concept_id
),

-- Get drug exposures for first drug
drug1_exposures AS (
    SELECT dr1.person_id, dr1.drug_exposure_start_date AS start_date
    FROM drug_exposure dr1
    JOIN drug1_concepts d1 ON dr1.drug_concept_id = d1.concept_id
),

-- Get drug exposures for second drug
drug2_exposures AS (
    SELECT dr2.person_id, dr2.drug_exposure_start_date AS start_date
    FROM drug_exposure dr2
    JOIN drug2_concepts d2 ON dr2.drug_concept_id = d2.concept_id
),

-- Get drug exposures for third drug
drug3_exposures AS (
    SELECT dr3.person_id, dr3.drug_exposure_start_date AS start_date
    FROM drug_exposure dr3
    JOIN drug3_concepts d3 ON dr3.drug_concept_id = d3.concept_id
),

-- Get drug exposures for fourth drug
drug4_exposures AS (
    SELECT dr4.person_id, dr4.drug_exposure_start_date AS start_date
    FROM drug_exposure dr4
    JOIN drug4_concepts d4 ON dr4.drug_concept_id = d4.concept_id
)

SELECT COUNT(DISTINCT a.person_id)
FROM drug1_exposures a
JOIN drug2_exposures b ON a.person_id = b.person_id
JOIN drug3_exposures c ON b.person_id = c.person_id
JOIN drug4_exposures d ON c.person_id = d.person_id
WHERE CAST(EXTRACT(epoch FROM CAST(GREATEST(a.start_date, b.start_date, c.start_date, d.start_date) AS TIMESTAMP) -
                              CAST(LEAST(a.start_date, b.start_date, c.start_date, d.start_date) AS TIMESTAMP)) /
          86400 AS BIGINT) <= %(days)s;