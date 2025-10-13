-- Counts of patients taking drug <ARG-DRUG><0>, <ARG-DRUG><1>, or <ARG-DRUG><2>.

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

-- Union all drug concept sets
all_drug_concepts AS (
    SELECT concept_id FROM drug1_concepts
    UNION
    SELECT concept_id FROM drug2_concepts
    UNION
    SELECT concept_id FROM drug3_concepts
)

SELECT COUNT(DISTINCT dr1.person_id)
FROM drug_exposure dr1
JOIN all_drug_concepts adc ON dr1.drug_concept_id = adc.concept_id;