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

-- Get drug exposures for first drug with start and end dates
drug1_exposures AS (
    SELECT DISTINCT
        dr1.person_id, 
        dr1.drug_exposure_start_date::date AS start_date,
        COALESCE(dr1.drug_exposure_end_date::date, dr1.drug_exposure_start_date::date) AS end_date
    FROM drug_exposure dr1
    JOIN drug1_concepts d1 ON dr1.drug_concept_id = d1.concept_id
),

-- Get drug exposures for second drug with start and end dates
drug2_exposures AS (
    SELECT DISTINCT
        dr2.person_id, 
        dr2.drug_exposure_start_date::date AS start_date,
        COALESCE(dr2.drug_exposure_end_date::date, dr2.drug_exposure_start_date::date) AS end_date
    FROM drug_exposure dr2
    JOIN drug2_concepts d2 ON dr2.drug_concept_id = d2.concept_id
),

-- Get drug exposures for third drug with start and end dates
drug3_exposures AS (
    SELECT DISTINCT
        dr3.person_id, 
        dr3.drug_exposure_start_date::date AS start_date,
        COALESCE(dr3.drug_exposure_end_date::date, dr3.drug_exposure_start_date::date) AS end_date
    FROM drug_exposure dr3
    JOIN drug3_concepts d3 ON dr3.drug_concept_id = d3.concept_id
),

-- Get drug exposures for fourth drug with start and end dates
drug4_exposures AS (
    SELECT DISTINCT
        dr4.person_id, 
        dr4.drug_exposure_start_date::date AS start_date,
        COALESCE(dr4.drug_exposure_end_date::date, dr4.drug_exposure_start_date::date) AS end_date
    FROM drug_exposure dr4
    JOIN drug4_concepts d4 ON dr4.drug_concept_id = d4.concept_id
),

-- Find overlapping periods and calculate the span
overlapping_quads AS (
    SELECT DISTINCT
        a.person_id,
        GREATEST(a.start_date, b.start_date, c.start_date, d.start_date) AS overlap_start,
        LEAST(a.end_date, b.end_date, c.end_date, d.end_date) AS overlap_end
    FROM drug1_exposures a
    JOIN drug2_exposures b 
        ON a.person_id = b.person_id
        AND b.start_date <= a.end_date
        AND b.end_date >= a.start_date
    JOIN drug3_exposures c 
        ON a.person_id = c.person_id
        AND c.start_date <= a.end_date
        AND c.end_date >= a.start_date
        AND c.start_date <= b.end_date
        AND c.end_date >= b.start_date
    JOIN drug4_exposures d 
        ON a.person_id = d.person_id
        AND d.start_date <= a.end_date
        AND d.end_date >= a.start_date
        AND d.start_date <= b.end_date
        AND d.end_date >= b.start_date
        AND d.start_date <= c.end_date
        AND d.end_date >= c.start_date
)

-- Count patients where the overlapping period spans within the time constraint
SELECT COUNT(DISTINCT person_id)
FROM overlapping_quads
WHERE overlap_end >= overlap_start
  AND (overlap_end - overlap_start) <= %(days)s;