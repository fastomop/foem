-- Counts of patients taking drug {drug1_name} and {drug2_name}.

WITH valid_drugs AS (
    SELECT concept_id
    FROM concept
    WHERE domain_id = 'Drug'
      AND invalid_reason IS NULL
),
drug_pairs AS (
    SELECT
        de1.drug_concept_id AS drug1_concept_id,
        de2.drug_concept_id AS drug2_concept_id,
        COUNT(DISTINCT de1.person_id) AS co_prescription_count
    FROM drug_exposure de1
    INNER JOIN valid_drugs vd1 ON de1.drug_concept_id = vd1.concept_id
    INNER JOIN drug_exposure de2
        ON de1.person_id = de2.person_id
        AND de1.drug_concept_id < de2.drug_concept_id
    INNER JOIN valid_drugs vd2 ON de2.drug_concept_id = vd2.concept_id
    GROUP BY de1.drug_concept_id, de2.drug_concept_id
    ORDER BY co_prescription_count DESC
    LIMIT {self.result_limit}
)
SELECT
    c1.concept_name AS drug1_name,
    c2.concept_name AS drug2_name
FROM drug_pairs dp
INNER JOIN concept c1 ON dp.drug1_concept_id = c1.concept_id
INNER JOIN concept c2 ON dp.drug2_concept_id = c2.concept_id;
