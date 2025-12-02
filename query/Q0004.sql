-- Counts of patients taking drug {drug1_name}, {drug2_name}, {drug3_name} and {drug4_name} within 30 days.

WITH valid_drug_exposures AS (
    SELECT
        de.person_id,
        de.drug_concept_id,
        de.drug_exposure_start_date,
        COALESCE(de.drug_exposure_end_date, de.drug_exposure_start_date) AS drug_exposure_end_date
    FROM drug_exposure de
    WHERE EXISTS (
        SELECT 1
        FROM concept c
        WHERE c.concept_id = de.drug_concept_id
          AND c.domain_id = 'Drug'
          AND c.standard_concept = 'S'
          AND c.invalid_reason IS NULL
    )
),
quads AS (
    SELECT
        e1.drug_concept_id AS drug1_concept_id,
        e2.drug_concept_id AS drug2_concept_id,
        e3.drug_concept_id AS drug3_concept_id,
        e4.drug_concept_id AS drug4_concept_id,
        COUNT(DISTINCT e1.person_id) AS person_count
    FROM valid_drug_exposures e1
    INNER JOIN valid_drug_exposures e2
        ON e2.person_id = e1.person_id
       AND e2.drug_concept_id > e1.drug_concept_id
       AND e2.drug_exposure_start_date <= e1.drug_exposure_end_date
       AND e2.drug_exposure_end_date >= e1.drug_exposure_start_date
    INNER JOIN valid_drug_exposures e3
        ON e3.person_id = e1.person_id
       AND e3.drug_concept_id > e2.drug_concept_id
       AND e3.drug_exposure_start_date <= e1.drug_exposure_end_date
       AND e3.drug_exposure_end_date >= e1.drug_exposure_start_date
       AND e3.drug_exposure_start_date <= e2.drug_exposure_end_date
       AND e3.drug_exposure_end_date >= e2.drug_exposure_start_date
    INNER JOIN valid_drug_exposures e4
        ON e4.person_id = e1.person_id
       AND e4.drug_concept_id > e3.drug_concept_id
       AND e4.drug_exposure_start_date <= e1.drug_exposure_end_date
       AND e4.drug_exposure_end_date >= e1.drug_exposure_start_date
       AND e4.drug_exposure_start_date <= e2.drug_exposure_end_date
       AND e4.drug_exposure_end_date >= e2.drug_exposure_start_date
       AND e4.drug_exposure_start_date <= e3.drug_exposure_end_date
       AND e4.drug_exposure_end_date >= e3.drug_exposure_start_date
    GROUP BY e1.drug_concept_id, e2.drug_concept_id, e3.drug_concept_id, e4.drug_concept_id
    ORDER BY person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS drug1_name,
    c2.concept_name AS drug2_name,
    c3.concept_name AS drug3_name,
    c4.concept_name AS drug4_name
    -- ,q.person_count AS patients
FROM quads q
INNER JOIN concept c1 ON c1.concept_id = q.drug1_concept_id
INNER JOIN concept c2 ON c2.concept_id = q.drug2_concept_id
INNER JOIN concept c3 ON c3.concept_id = q.drug3_concept_id
INNER JOIN concept c4 ON c4.concept_id = q.drug4_concept_id;
