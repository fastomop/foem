WITH candidates AS (
    SELECT person_id 
    FROM drug_exposure
    GROUP BY person_id 
    HAVING COUNT(*) >= 3
),
exposure_ranges AS (
    SELECT 
        de.person_id,
        de.drug_concept_id,
        daterange(de.drug_exposure_start_date, de.drug_exposure_end_date, '[]') AS dr
    FROM drug_exposure de
    INNER JOIN candidates c ON de.person_id = c.person_id
    WHERE de.drug_exposure_start_date IS NOT NULL
),
overlapping_triplets AS (
    SELECT 
        d1.person_id,
        d1.drug_concept_id AS drug_1,
        d2.drug_concept_id AS drug_2,
        d3.drug_concept_id AS drug_3,
        (d1.dr * d2.dr * d3.dr) AS overlap_period
    FROM exposure_ranges d1
    INNER JOIN exposure_ranges d2 
        ON d1.person_id = d2.person_id 
        AND d1.drug_concept_id < d2.drug_concept_id
    INNER JOIN exposure_ranges d3 
        ON d1.person_id = d3.person_id 
        AND d2.drug_concept_id < d3.drug_concept_id
        AND (d1.dr * d2.dr) && d3.dr
    LIMIT {self.result_limit} 
)
SELECT 
    c1.concept_name AS drug_name_1,
    c2.concept_name AS drug_name_2,
    c3.concept_name AS drug_name_3
FROM overlapping_triplets t
LEFT JOIN concept c1 ON t.drug_1 = c1.concept_id
LEFT JOIN concept c2 ON t.drug_2 = c2.concept_id
LEFT JOIN concept c3 ON t.drug_3 = c3.concept_id;