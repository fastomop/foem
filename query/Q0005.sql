WITH valid_drugs AS (
    SELECT concept_id, concept_name
    FROM concept
    WHERE domain_id = 'Drug' 
      AND standard_concept = 'S' 
      AND invalid_reason IS NULL
),
-- 1. IDENTIFY THE "HEAD" (Top 50 Most Common Drugs)
top_drugs AS (
    SELECT 
        d.drug_concept_id
    FROM drug_exposure d
    INNER JOIN valid_drugs v ON d.drug_concept_id = v.concept_id
    GROUP BY d.drug_concept_id
    ORDER BY COUNT(DISTINCT d.person_id) DESC
    LIMIT 50 -- <--- ADJUST THIS NUMBER TO WIDEN/NARROW SEARCH
),
-- 2. FILTER DATASET (Ignore rare drugs)
filtered_drug_eras AS (
    SELECT 
        de.person_id, 
        de.drug_concept_id, 
        de.drug_exposure_start_date AS start_date,
        -- Handle NULL end dates by assuming 1-day duration
        COALESCE(de.drug_exposure_end_date, de.drug_exposure_start_date) AS end_date
    FROM drug_exposure de
    INNER JOIN top_drugs td ON de.drug_concept_id = td.drug_concept_id
),
-- 3. THE HEAVY LIFT (Quad Join on Small Subset)
concomitant_quads AS (
    SELECT 
        d1.drug_concept_id AS d1_id,
        d2.drug_concept_id AS d2_id,
        d3.drug_concept_id AS d3_id,
        d4.drug_concept_id AS d4_id,
        COUNT(DISTINCT d1.person_id) AS patient_count
    FROM filtered_drug_eras d1
    -- Join D2
    INNER JOIN filtered_drug_eras d2 
        ON d1.person_id = d2.person_id 
        AND d2.drug_concept_id > d1.drug_concept_id
        AND d2.start_date <= d1.end_date AND d2.end_date >= d1.start_date
    -- Join D3
    INNER JOIN filtered_drug_eras d3 
        ON d1.person_id = d3.person_id 
        AND d3.drug_concept_id > d2.drug_concept_id
        AND d3.start_date <= d1.end_date AND d3.end_date >= d1.start_date -- Overlap D1
        AND d3.start_date <= d2.end_date AND d3.end_date >= d2.start_date -- Overlap D2
    -- Join D4
    INNER JOIN filtered_drug_eras d4 
        ON d1.person_id = d4.person_id 
        AND d4.drug_concept_id > d3.drug_concept_id
        AND d4.start_date <= d1.end_date AND d4.end_date >= d1.start_date -- Overlap D1
        AND d4.start_date <= d2.end_date AND d4.end_date >= d2.start_date -- Overlap D2
        AND d4.start_date <= d3.end_date AND d4.end_date >= d3.start_date -- Overlap D3
    
    -- Ensure 4-way intersection exists
    WHERE GREATEST(d1.start_date, d2.start_date, d3.start_date, d4.start_date) 
          <= 
          LEAST(d1.end_date, d2.end_date, d3.end_date, d4.end_date)
          
    GROUP BY d1.drug_concept_id, d2.drug_concept_id, d3.drug_concept_id, d4.drug_concept_id
    ORDER BY patient_count DESC
    LIMIT 10
)
-- 4. FINAL DISPLAY
SELECT 
    v1.concept_name AS drug1_name,
    v2.concept_name AS drug2_name,
    v3.concept_name AS drug3_name,
    v4.concept_name AS drug4_name,
    q.patient_count
FROM concomitant_quads q
JOIN valid_drugs v1 ON q.d1_id = v1.concept_id
JOIN valid_drugs v2 ON q.d2_id = v2.concept_id
JOIN valid_drugs v3 ON q.d3_id = v3.concept_id
JOIN valid_drugs v4 ON q.d4_id = v4.concept_id;