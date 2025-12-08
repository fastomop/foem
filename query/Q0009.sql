-- Counts of patients taking drug {drug1_name}, {drug2_name} or {drug3_name}.

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
cooccur_triples AS (
    SELECT
        e1.person_id,
        e1.drug_concept_id AS drug1,
        e2.drug_concept_id AS drug2,
        e3.drug_concept_id AS drug3
    FROM eras e1
    INNER JOIN eras e2
        ON e2.person_id = e1.person_id
       AND e2.drug_concept_id > e1.drug_concept_id
    INNER JOIN eras e3
        ON e3.person_id = e1.person_id
       AND e3.drug_concept_id > e2.drug_concept_id
    GROUP BY e1.person_id, e1.drug_concept_id, e2.drug_concept_id, e3.drug_concept_id
),
overlapping_triples AS (
    SELECT DISTINCT
        a.person_id,
        a.drug_concept_id AS drug1,
        b.drug_concept_id AS drug2,
        c.drug_concept_id AS drug3
    FROM eras a
    INNER JOIN eras b
        ON b.person_id = a.person_id
       AND b.drug_concept_id > a.drug_concept_id
    INNER JOIN eras c
        ON c.person_id = a.person_id
       AND c.drug_concept_id > b.drug_concept_id
    WHERE GREATEST(a.start_date, b.start_date, c.start_date)
       <= LEAST(a.end_date, b.end_date, c.end_date)
),
separate_triples AS (
    SELECT ct.drug1, ct.drug2, ct.drug3
    FROM cooccur_triples ct
    LEFT JOIN overlapping_triples ot
        ON ot.person_id = ct.person_id
       AND ot.drug1 = ct.drug1
       AND ot.drug2 = ct.drug2
       AND ot.drug3 = ct.drug3
    WHERE ot.person_id IS NULL
    GROUP BY ct.drug1, ct.drug2, ct.drug3
    ORDER BY COUNT(DISTINCT ct.person_id) DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS drug1_name,
    c2.concept_name AS drug2_name,
    c3.concept_name AS drug3_name
FROM separate_triples s
INNER JOIN concept c1 ON c1.concept_id = s.drug1
INNER JOIN concept c2 ON c2.concept_id = s.drug2
INNER JOIN concept c3 ON c3.concept_id = s.drug3;
