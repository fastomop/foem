-- Counts of patients taking drug {drug1_name} or {drug2_name}.

WITH valid_drugs AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Drug'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
eras AS (
    SELECT
        de.person_id,
        de.drug_concept_id,
        de.drug_era_start_date AS start_date,
        de.drug_era_end_date AS end_date
    FROM drug_era de
    INNER JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
),
cooccur_pairs AS (
    SELECT
        e1.person_id,
        LEAST(e1.drug_concept_id, e2.drug_concept_id) AS drug1,
        GREATEST(e1.drug_concept_id, e2.drug_concept_id) AS drug2
    FROM eras e1
    INNER JOIN eras e2
        ON e1.person_id = e2.person_id
       AND e1.drug_concept_id < e2.drug_concept_id
    GROUP BY e1.person_id, LEAST(e1.drug_concept_id, e2.drug_concept_id),
            GREATEST(e1.drug_concept_id, e2.drug_concept_id)
),
overlapping_pairs AS (
    SELECT DISTINCT
        a.person_id,
        LEAST(a.drug_concept_id, b.drug_concept_id) AS drug1,
        GREATEST(a.drug_concept_id, b.drug_concept_id) AS drug2
    FROM eras a
    INNER JOIN eras b
        ON a.person_id = b.person_id
       AND a.drug_concept_id < b.drug_concept_id
       AND a.start_date <= b.end_date
       AND b.start_date <= a.end_date
),
separate_pairs AS (
    SELECT c.person_id, c.drug1, c.drug2
    FROM cooccur_pairs c
    LEFT JOIN overlapping_pairs o
        ON o.person_id = c.person_id
       AND o.drug1 = c.drug1
       AND o.drug2 = c.drug2
    WHERE o.person_id IS NULL
    GROUP BY c.drug1, c.drug2
    ORDER BY COUNT(DISTINCT c.person_id) DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS drug1_name,
    c2.concept_name AS drug2_name
FROM separate_pairs sp
INNER JOIN concept c1 ON c1.concept_id = sp.drug1
INNER JOIN concept c2 ON c2.concept_id = sp.drug2;
