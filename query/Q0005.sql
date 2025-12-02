-- Counts of patients taking drug {drug1_name}, {drug2_name}, {drug3_name} and {drug4_name}.

WITH valid_drugs AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Drug'
      AND standard_concept = 'S'
      AND invalid_reason IS NULL
),
quads AS (
    SELECT
        d1.drug_concept_id AS drug1_concept_id,
        d2.drug_concept_id AS drug2_concept_id,
        d3.drug_concept_id AS drug3_concept_id,
        d4.drug_concept_id AS drug4_concept_id,
        COUNT(DISTINCT d1.person_id) AS person_count
    FROM drug_exposure d1
    INNER JOIN valid_drugs vd1 ON d1.drug_concept_id = vd1.concept_id
    INNER JOIN drug_exposure d2
        ON d2.person_id = d1.person_id
       AND d2.drug_concept_id > d1.drug_concept_id
    INNER JOIN valid_drugs vd2 ON d2.drug_concept_id = vd2.concept_id
    INNER JOIN drug_exposure d3
        ON d3.person_id = d1.person_id
       AND d3.drug_concept_id > d2.drug_concept_id
    INNER JOIN valid_drugs vd3 ON d3.drug_concept_id = vd3.concept_id
    INNER JOIN drug_exposure d4
        ON d4.person_id = d1.person_id
       AND d4.drug_concept_id > d3.drug_concept_id
    INNER JOIN valid_drugs vd4 ON d4.drug_concept_id = vd4.concept_id
    GROUP BY d1.drug_concept_id, d2.drug_concept_id, d3.drug_concept_id, d4.drug_concept_id
    ORDER BY person_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS drug1_name,
    c2.concept_name AS drug2_name,
    c3.concept_name AS drug3_name,
    c4.concept_name AS drug4_name
FROM quads q
INNER JOIN concept c1 ON c1.concept_id = q.drug1_concept_id
INNER JOIN concept c2 ON c2.concept_id = q.drug2_concept_id
INNER JOIN concept c3 ON c3.concept_id = q.drug3_concept_id
INNER JOIN concept c4 ON c4.concept_id = q.drug4_concept_id;
