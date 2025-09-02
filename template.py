import re

class Template:
    
    def __init__(self, schema: str = "public"):
        self.__set_schema(schema)

    def __set_schema(self, schema: str):
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", schema):
            raise ValueError(f"Invalid schema name: {schema!r}")
        self.schema = schema  

    def find_code_by_name(self, name: str, vocab_dict: dict) -> tuple[str, str] | None:
      """
      Given a drug/concept name (case-insensitive) and a dictionary in the form:
            {"RxNorm": {"concept_code": "concept_name", ...}, ...}

      Return (vocabulary_id, concept_code) if found, else None.
      """
      for vocab_id, codes in vocab_dict.items():
            for concept_code, concept_name in codes.items():
                  if concept_name.lower() == name.lower():
                        return vocab_id, concept_code
      return None


    def patients_group_by_gender_and_ethn(self):
        desc = f"""
            Number of patients grouped by gender and ethnicity.
            """
        
        sql = """
            SELECT gen_temp1.gender, eth_temp1.ethnicity, COUNT(DISTINCT pe1.person_id) AS number_of_patients
            FROM {schema}.person AS pe1
                  JOIN (SELECT concept_id, concept_name AS gender
                        FROM {schema}.concept
                        WHERE domain_id = 'Gender' AND standard_concept = 'S') AS gen_temp1
                        ON pe1.gender_concept_id = gen_temp1.concept_id
                  JOIN (SELECT concept_id, concept_name AS ethnicity
                        FROM {schema}.concept
                        WHERE domain_id = 'Ethnicity' AND standard_concept = 'S') AS eth_temp1
                        ON pe1.ethnicity_concept_id = eth_temp1.concept_id
            GROUP BY gen_temp1.gender, eth_temp1.ethnicity;
            """.replace("{schema}", self.schema)
        
        return sql, desc, {}
    
    def patients_group_by_race(self):
        desc = f"""
            Count of patients grouped by race.
            """
        
        sql = """
            SELECT race, COUNT(DISTINCT pe1.person_id) AS number_of_patients
            FROM {schema}.person AS pe1
                  LEFT JOIN (SELECT concept_id, concept_name AS race
                        FROM {schema}.concept
                        WHERE domain_id = 'Race' AND standard_concept = 'S') AS alias1 ON pe1.race_concept_id = concept_id
            GROUP BY race;
            """.replace("{schema}", self.schema)
        
        return sql, desc, {}
    
    def patients_2drugs_and_time(self, v_id1: str, d_id1: str, v_id2: str, d_id2: str, days: int):
        """
        Counts of patients taking drug <ARG-DRUG><0> and <ARG-DRUG><1> within <ARG-TIMEDAYS><0> days.
        """
        
        sql = """
            SELECT COUNT(DISTINCT a.person_id)
            FROM (SELECT dr1.person_id, dr1.drug_exposure_start_date AS start_date
                  FROM {schema}.drug_exposure AS dr1
                        JOIN (SELECT descendant_concept_id AS concept_id
                              FROM (SELECT *
                                    FROM (SELECT concept_id_2
                                          FROM ((SELECT concept_id
                                                FROM {schema}.concept
                                                WHERE vocabulary_id = %(v_id1)s
                                                      AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                          FROM {schema}.concept_relationship
                                                                                          WHERE relationship_id = 'Maps to') AS alias1
                                                ON concept_id = concept_id_1)) as c
                                                JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                          JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) AS d1
                              ON dr1.drug_concept_id = concept_id) AS a
                  JOIN (SELECT dr2.person_id, dr2.drug_exposure_start_date AS start_date
                        FROM {schema}.drug_exposure AS dr2
                                    JOIN (SELECT descendant_concept_id AS concept_id
                                          FROM (SELECT *
                                                FROM (SELECT concept_id_2
                                                      FROM ((SELECT concept_id
                                                            FROM {schema}.concept
                                                            WHERE vocabulary_id = %(v_id2)s
                                                            AND (concept_code = %(d_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                FROM {schema}.concept_relationship
                                                                                                WHERE relationship_id = 'Maps to') AS alias2
                                                            ON concept_id = concept_id_1)) as i
                                                      JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) AS d2
                                    ON dr2.drug_concept_id = concept_id) AS b ON a.person_id = b.person_id
            WHERE CAST(EXTRACT(epoch FROM CAST(GREATEST(a.start_date, b.start_date) AS TIMESTAMP) -
                                          CAST(LEAST(a.start_date, b.start_date) AS TIMESTAMP)) / 86400 AS BIGINT) <= %(days)s;
            """.replace("{schema}", self.schema)
        
        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2, "days": days}
    
    def patients_2drugs_and(self, v_id1,  d_id1, v_id2, d_id2):
        desc = f"""
            Counts of patients taking drug <ARG-DRUG><0> and <ARG-DRUG><1>.
            """
        
        sql = """
            SELECT COUNT(DISTINCT dr1.person_id)
            FROM (({schema}.drug_exposure AS dr1 JOIN (SELECT descendant_concept_id AS concept_id
                                                FROM (SELECT *
                                                      FROM (SELECT concept_id_2
                                                            FROM ((SELECT concept_id
                                                                        FROM {schema}.concept
                                                                        WHERE vocabulary_id = %(v_id1)s
                                                                        AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias1
                                                                  ON concept_id = concept_id_1)) as c
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                  ON dr1.drug_concept_id = concept_id) JOIN ({schema}.drug_exposure AS dr2 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                                FROM (SELECT *
                                                                                                      FROM (SELECT concept_id_2
                                                                                                            FROM ((SELECT concept_id
                                                                                                                  FROM {schema}.concept
                                                                                                                  WHERE vocabulary_id = %(v_id2)s
                                                                                                                  AND (concept_code = %(d_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                      FROM {schema}.concept_relationship
                                                                                                                                                      WHERE relationship_id = 'Maps to') AS alias2
                                                                                                                  ON concept_id = concept_id_1)) as i
                                                                                                            JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                                                      JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                                                            ON dr2.drug_concept_id = concept_id)
                  ON dr1.person_id = dr2.person_id);
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2}
    
    def patients_2drugs_or(self, v_id1, v_id2, d_id1, d_id2):
        """
        Counts of patients taking drug <ARG-DRUG><0> or <ARG-DRUG><1>.
        """

        sql = """
            SELECT COUNT(DISTINCT dr1.person_id)
            FROM ({schema}.drug_exposure AS dr1 JOIN ((SELECT descendant_concept_id AS concept_id
                                                FROM (SELECT *
                                                      FROM (SELECT concept_id_2
                                                            FROM ((SELECT concept_id
                                                                        FROM {schema}.concept
                                                                        WHERE vocabulary_id = %(v_id1)s
                                                                        AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias1
                                                                  ON concept_id = concept_id_1)) as c
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id)
                                                UNION
                                                (SELECT descendant_concept_id AS concept_id
                                                FROM (SELECT *
                                                      FROM (SELECT concept_id_2
                                                            FROM ((SELECT concept_id
                                                                        FROM {schema}.concept
                                                                        WHERE vocabulary_id = %(v_id2)s
                                                                        AND (concept_code = %(d_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias2
                                                                  ON concept_id = concept_id_1)) as i
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id)) as "ccia1ci2c*caciccia2ci2c*caci"
                  ON dr1.drug_concept_id = concept_id);
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2}
    
    def patients_4drugs_and_time(self, v_id1, v_id2, v_id3, v_id4, d_id1, d_id2, d_id3, d_id4, days):
        """
        Counts of patients taking drug <ARG-DRUG><0>, <ARG-DRUG><1>, <ARG-DRUG><2> and <ARG-DRUG><3> within <ARG-TIMEDAYS><0> days.
        """
        sql = """
            SELECT COUNT(DISTINCT a.person_id)
            FROM (SELECT dr1.person_id, dr1.drug_exposure_start_date AS start_date
                  FROM {schema}.drug_exposure AS dr1
                        JOIN (SELECT descendant_concept_id AS concept_id
                              FROM (SELECT *
                                    FROM (SELECT concept_id_2
                                          FROM ((SELECT concept_id
                                                FROM {schema}.concept
                                                WHERE vocabulary_id = %(v_id1)s
                                                      AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                          FROM {schema}.concept_relationship
                                                                                          WHERE relationship_id = 'Maps to') AS alias1
                                                ON concept_id = concept_id_1)) as i
                                                JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                          JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                              ON dr1.drug_concept_id = concept_id) AS a
                  JOIN (SELECT dr2.person_id, dr2.drug_exposure_start_date AS start_date
                        FROM {schema}.drug_exposure AS dr2
                                    JOIN (SELECT descendant_concept_id AS concept_id
                                          FROM (SELECT *
                                                FROM (SELECT concept_id_2
                                                      FROM ((SELECT concept_id
                                                            FROM {schema}.concept
                                                            WHERE vocabulary_id = %(v_id2)s
                                                            AND (concept_code = %(d_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                FROM {schema}.concept_relationship
                                                                                                WHERE relationship_id = 'Maps to') AS alias2
                                                            ON concept_id = concept_id_1)) as ccia2ci2
                                                      JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                                    ON dr2.drug_concept_id = concept_id) AS b ON a.person_id = b.person_id
                  JOIN (SELECT dr3.person_id, dr3.drug_exposure_start_date AS start_date
                        FROM {schema}.drug_exposure AS dr3
                                    JOIN (SELECT descendant_concept_id AS concept_id
                                          FROM (SELECT *
                                                FROM (SELECT concept_id_2
                                                      FROM ((SELECT concept_id
                                                            FROM {schema}.concept
                                                            WHERE vocabulary_id = %(v_id3)s
                                                            AND (concept_code = %(d_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                FROM {schema}.concept_relationship
                                                                                                WHERE relationship_id = 'Maps to') AS alias3
                                                            ON concept_id = concept_id_1)) as ccia3ci2
                                                      JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia3ci2c*"
                                                JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia3ci2c*caci"
                                    ON dr3.drug_concept_id = concept_id) AS c ON b.person_id = c.person_id
                  JOIN (SELECT dr4.person_id, dr4.drug_exposure_start_date AS start_date
                        FROM {schema}.drug_exposure AS dr4
                                    JOIN (SELECT descendant_concept_id AS concept_id
                                          FROM (SELECT *
                                                FROM (SELECT concept_id_2
                                                      FROM ((SELECT concept_id
                                                            FROM {schema}.concept
                                                            WHERE vocabulary_id = %(v_id4)s
                                                            AND (concept_code = %(d_id4)s)) as cci4 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                FROM {schema}.concept_relationship
                                                                                                WHERE relationship_id = 'Maps to') AS alias4
                                                            ON concept_id = concept_id_1)) as ccia4ci2
                                                      JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia4ci2c*"
                                                JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia4ci2c*caci"
                                    ON dr4.drug_concept_id = concept_id) AS d ON c.person_id = d.person_id
            WHERE CAST(EXTRACT(epoch FROM CAST(GREATEST(a.start_date, b.start_date, c.start_date, d.start_date) AS TIMESTAMP) -
                                          CAST(LEAST(a.start_date, b.start_date, c.start_date, d.start_date) AS TIMESTAMP)) /
                  86400 AS BIGINT) <= %(days)s;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2, "v_id3": v_id3, "d_id3": d_id3, "v_id4": v_id4, "d_id4": d_id4, "days": days}
    
    def patients_4drugs_and(self, v_id1, v_id2, v_id3, v_id4, d_id1, d_id2, d_id3, d_id4):
        """
        Counts of patients taking drug <ARG-DRUG><0>, <ARG-DRUG><1>, <ARG-DRUG><2> and <ARG-DRUG><3>.
        """

        sql = """
            SELECT COUNT(DISTINCT dr1.person_id)
            FROM ((({schema}.drug_exposure AS dr1 JOIN (SELECT descendant_concept_id AS concept_id
                                                      FROM (SELECT *
                                                            FROM (SELECT concept_id_2
                                                                  FROM ((SELECT concept_id
                                                                        FROM {schema}.concept
                                                                        WHERE vocabulary_id = %(v_id1)s
                                                                        AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias1
                                                                        ON concept_id = concept_id_1)) as c
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                  ON dr1.drug_concept_id = concept_id) JOIN ({schema}.drug_exposure AS dr2 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                                FROM (SELECT *
                                                                                                      FROM (SELECT concept_id_2
                                                                                                            FROM ((SELECT concept_id
                                                                                                                  FROM {schema}.concept
                                                                                                                  WHERE vocabulary_id = %(v_id2)s
                                                                                                                  AND (concept_code = %(d_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                      FROM {schema}.concept_relationship
                                                                                                                                                      WHERE relationship_id = 'Maps to') AS alias2
                                                                                                                  ON concept_id = concept_id_1)) as i
                                                                                                            JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                                                      JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                                                            ON dr2.drug_concept_id = concept_id)
                  ON dr1.person_id = dr2.person_id) JOIN ({schema}.drug_exposure AS dr3 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                          FROM (SELECT *
                                                                                                FROM (SELECT concept_id_2
                                                                                                      FROM ((SELECT concept_id
                                                                                                            FROM {schema}.concept
                                                                                                            WHERE vocabulary_id = %(v_id3)s
                                                                                                                  AND (concept_code = %(d_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                FROM {schema}.concept_relationship
                                                                                                                                                WHERE relationship_id = 'Maps to') AS alias3
                                                                                                            ON concept_id = concept_id_1)) as ccia3ci2
                                                                                                            JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia3ci2c*"
                                                                                                      JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia3ci2c*caci"
                                                      ON dr3.drug_concept_id = concept_id) ON dr2.person_id = dr3.person_id)
                  JOIN ({schema}.drug_exposure AS dr4 JOIN (SELECT descendant_concept_id AS concept_id
                                                            FROM (SELECT *
                                                                  FROM (SELECT concept_id_2
                                                                        FROM ((SELECT concept_id
                                                                              FROM {schema}.concept
                                                                              WHERE vocabulary_id = %(v_id4)s
                                                                              AND (concept_code = %(d_id4)s)) as cci4 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                  FROM {schema}.concept_relationship
                                                                                                                  WHERE relationship_id = 'Maps to') AS alias4
                                                                              ON concept_id = concept_id_1)) as ccia4ci2
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia4ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia4ci2c*caci"
                        ON dr4.drug_concept_id = concept_id) ON dr3.person_id = dr4.person_id;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2, "v_id3": v_id3, "d_id3": d_id3, "v_id4": v_id4, "d_id4": d_id4}
    
    def patients_4drugs_or(self, v_id1, v_id2, v_id3, v_id4, d_id1, d_id2, d_id3, d_id4):
        """
        Counts of patients taking drug <ARG-DRUG><0>, <ARG-DRUG><1>, <ARG-DRUG><2> or <ARG-DRUG><3>.
        """

        sql = """
            SELECT COUNT(DISTINCT dr1.person_id)
            FROM ({schema}.drug_exposure AS dr1 JOIN ((((SELECT descendant_concept_id AS concept_id
                                                      FROM (SELECT *
                                                            FROM (SELECT concept_id_2
                                                                  FROM ((SELECT concept_id
                                                                        FROM {schema}.concept
                                                                        WHERE vocabulary_id = %(v_id1)s
                                                                        AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                  FROM {schema}.concept_relationship
                                                                                                                  WHERE relationship_id = 'Maps to') AS alias1
                                                                        ON concept_id = concept_id_1)) as c
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id)
                                                      UNION
                                                      (SELECT descendant_concept_id AS concept_id
                                                      FROM (SELECT *
                                                            FROM (SELECT concept_id_2
                                                                  FROM ((SELECT concept_id
                                                                        FROM {schema}.concept
                                                                        WHERE vocabulary_id = %(v_id2)s
                                                                        AND (concept_code = %(d_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias2
                                                                        ON concept_id = concept_id_1)) as i
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id))
                                                UNION
                                                (SELECT descendant_concept_id AS concept_id
                                                      FROM (SELECT *
                                                            FROM (SELECT concept_id_2
                                                                  FROM ((SELECT concept_id
                                                                        FROM {schema}.concept
                                                                        WHERE vocabulary_id = %(v_id3)s
                                                                        AND (concept_code = %(d_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias3
                                                                        ON concept_id = concept_id_1)) as ccia3ci2
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia3ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id))
                                                UNION
                                                (SELECT descendant_concept_id AS concept_id
                                                FROM (SELECT *
                                                      FROM (SELECT concept_id_2
                                                            FROM ((SELECT concept_id
                                                                        FROM {schema}.concept
                                                                        WHERE vocabulary_id = %(v_id4)s
                                                                        AND (concept_code = %(d_id4)s)) as cci4 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias4
                                                                  ON concept_id = concept_id_1)) as ccia4ci2
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia4ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id)) as "ccia1ci2c*caciccia2ci2c*caciccia3ci2c*caciccia4ci2c*caci"
                  ON dr1.drug_concept_id = concept_id);
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2, "v_id3": v_id3, "d_id3": d_id3, "v_id4": v_id4, "d_id4": d_id4}
    
    def patients_3drugs_and_time(self, v_id1, v_id2, v_id3, d_id1, d_id2, d_id3, days):
        """
        Counts of patients taking drug <ARG-DRUG><0>, <ARG-DRUG><1>, and <ARG-DRUG><2> within <ARG-TIMEDAYS><0> days.
        """

        sql = """
            SELECT COUNT(DISTINCT a.person_id)
            FROM (SELECT dr1.person_id, dr1.drug_exposure_start_date AS start_date
                  FROM {schema}.drug_exposure AS dr1
                        JOIN (SELECT descendant_concept_id AS concept_id
                              FROM (SELECT *
                                    FROM (SELECT concept_id_2
                                          FROM ((SELECT concept_id
                                                FROM {schema}.concept
                                                WHERE vocabulary_id = %(v_id1)s
                                                      AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                          FROM {schema}.concept_relationship
                                                                                          WHERE relationship_id = 'Maps to') AS alias1
                                                ON concept_id = concept_id_1)) as i
                                                JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                          JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                              ON dr1.drug_concept_id = concept_id) AS a
                  JOIN (SELECT dr2.person_id, dr2.drug_exposure_start_date AS start_date
                        FROM {schema}.drug_exposure AS dr2
                                    JOIN (SELECT descendant_concept_id AS concept_id
                                          FROM (SELECT *
                                                FROM (SELECT concept_id_2
                                                      FROM ((SELECT concept_id
                                                            FROM {schema}.concept
                                                            WHERE vocabulary_id = %(v_id2)s
                                                            AND (concept_code = %(d_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                FROM {schema}.concept_relationship
                                                                                                WHERE relationship_id = 'Maps to') AS alias2
                                                            ON concept_id = concept_id_1)) as ccia2ci2
                                                      JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                                    ON dr2.drug_concept_id = concept_id) AS b ON a.person_id = b.person_id
                  JOIN (SELECT dr3.person_id, dr3.drug_exposure_start_date AS start_date
                        FROM {schema}.drug_exposure AS dr3
                                    JOIN (SELECT descendant_concept_id AS concept_id
                                          FROM (SELECT *
                                                FROM (SELECT concept_id_2
                                                      FROM ((SELECT concept_id
                                                            FROM {schema}.concept
                                                            WHERE vocabulary_id = %(v_id3)s
                                                            AND (concept_code = %(d_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                FROM {schema}.concept_relationship
                                                                                                WHERE relationship_id = 'Maps to') AS alias3
                                                            ON concept_id = concept_id_1)) as ccia3ci2
                                                      JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia3ci2c*"
                                                JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia3ci2c*caci"
                                    ON dr3.drug_concept_id = concept_id) AS c ON b.person_id = c.person_id
            WHERE CAST(EXTRACT(epoch FROM CAST(GREATEST(a.start_date, b.start_date, c.start_date) AS TIMESTAMP) -
                                          CAST(LEAST(a.start_date, b.start_date, c.start_date) AS TIMESTAMP)) / 86400 AS BIGINT) <= %(days)s;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2, "v_id3": v_id3, "d_id3": d_id3, "days": days}
    
    def patients_3drugs_and(self, v_id1, v_id2, v_id3, d_id1, d_id2, d_id3):
        """
        Counts of patients taking drug <ARG-DRUG><0>, <ARG-DRUG><1>, and <ARG-DRUG><2>.
        """

        sql = """
            SELECT COUNT(DISTINCT dr1.person_id)
            FROM (({schema}.drug_exposure AS dr1 JOIN (SELECT descendant_concept_id AS concept_id
                                                FROM (SELECT *
                                                      FROM (SELECT concept_id_2
                                                            FROM ((SELECT concept_id
                                                                        FROM {schema}.concept
                                                                        WHERE vocabulary_id = %(v_id1)s
                                                                        AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias1
                                                                  ON concept_id = concept_id_1)) as c
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                  ON dr1.drug_concept_id = concept_id) JOIN ({schema}.drug_exposure AS dr2 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                                FROM (SELECT *
                                                                                                      FROM (SELECT concept_id_2
                                                                                                            FROM ((SELECT concept_id
                                                                                                                  FROM {schema}.concept
                                                                                                                  WHERE vocabulary_id = %(v_id2)s
                                                                                                                  AND (concept_code = %(d_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                      FROM {schema}.concept_relationship
                                                                                                                                                      WHERE relationship_id = 'Maps to') AS alias2
                                                                                                                  ON concept_id = concept_id_1)) as i
                                                                                                            JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                                                      JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                                                            ON dr2.drug_concept_id = concept_id) ON dr1.person_id = dr2.person_id)
                  JOIN ({schema}.drug_exposure AS dr3 JOIN (SELECT descendant_concept_id AS concept_id
                                                            FROM (SELECT *
                                                                  FROM (SELECT concept_id_2
                                                                        FROM ((SELECT concept_id
                                                                              FROM {schema}.concept
                                                                              WHERE vocabulary_id = %(v_id3)s
                                                                              AND (concept_code = %(d_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                  FROM {schema}.concept_relationship
                                                                                                                  WHERE relationship_id = 'Maps to') AS alias3
                                                                              ON concept_id = concept_id_1)) as ccia3ci2
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia3ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia3ci2c*caci"
                        ON dr3.drug_concept_id = concept_id) ON dr2.person_id = dr3.person_id;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2, "v_id3": v_id3, "d_id3": d_id3}
    
    def patients_3drugs_or(self, v_id1, v_id2, v_id3, d_id1, d_id2, d_id3):
        """
        Counts of patients taking drug <ARG-DRUG><0>, <ARG-DRUG><1>, or <ARG-DRUG><2>.
        """

        sql = """
            SELECT COUNT(DISTINCT dr1.person_id)
            FROM ({schema}.drug_exposure AS dr1 JOIN (((SELECT descendant_concept_id AS concept_id
                                                      FROM (SELECT *
                                                            FROM (SELECT concept_id_2
                                                                  FROM ((SELECT concept_id
                                                                        FROM {schema}.concept
                                                                        WHERE vocabulary_id = %(v_id1)s
                                                                        AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias1
                                                                        ON concept_id = concept_id_1)) as c
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id)
                                                UNION
                                                (SELECT descendant_concept_id AS concept_id
                                                      FROM (SELECT *
                                                            FROM (SELECT concept_id_2
                                                                  FROM ((SELECT concept_id
                                                                        FROM {schema}.concept
                                                                        WHERE vocabulary_id = %(v_id2)s
                                                                        AND (concept_code = %(d_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias2
                                                                        ON concept_id = concept_id_1)) as i
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id))
                                                UNION
                                                (SELECT descendant_concept_id AS concept_id
                                                FROM (SELECT *
                                                      FROM (SELECT concept_id_2
                                                            FROM ((SELECT concept_id
                                                                        FROM {schema}.concept
                                                                        WHERE vocabulary_id = %(v_id3)s
                                                                        AND (concept_code = %(d_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias3
                                                                  ON concept_id = concept_id_1)) as ccia3ci2
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia3ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id)) as "ccia1ci2c*caciccia2ci2c*caciccia3ci2c*caci"
                  ON dr1.drug_concept_id = concept_id);
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2, "v_id3": v_id3, "d_id3": d_id3}
    
    def patients_2conditions_and_time(self, v_id1, v_id2, c_id1, c_id2, days):
        """
        Counts of patients with condition <ARG-CONDITION><0> and <ARG-CONDITION><1> within <ARG-TIMEDAYS><0> days.
        """
        
        sql = """
            WITH seed_a AS
            (SELECT c.concept_id AS src_id
            FROM concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code = %(c_id1)s
            AND c.invalid_reason IS NULL),
            std_a AS
            (SELECT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_a s
            LEFT JOIN concept_relationship cr ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL),
            desc_a AS
            (SELECT ca.descendant_concept_id AS concept_id
            FROM std_a sa
            JOIN concept_ancestor ca ON ca.ancestor_concept_id = sa.standard_id),
            a AS
            (SELECT con1.person_id,
                  con1.condition_start_date::date AS start_date
            FROM {schema}.condition_occurrence con1
            JOIN desc_a ON con1.condition_concept_id = desc_a.concept_id),
            seed_b AS
            (SELECT c.concept_id AS src_id
            FROM concept c
            WHERE c.vocabulary_id = %(v_id2)s
            AND c.concept_code = %(c_id2)s
            AND c.invalid_reason IS NULL),
            std_b AS
            (SELECT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_b s
            LEFT JOIN concept_relationship cr ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL),
            desc_b AS
            (SELECT ca.descendant_concept_id AS concept_id
            FROM std_b sb
            JOIN concept_ancestor ca ON ca.ancestor_concept_id = sb.standard_id),
            b AS
            (SELECT con2.person_id,
                  con2.condition_start_date::date AS start_date
            FROM {schema}.condition_occurrence con2
            JOIN desc_b ON con2.condition_concept_id = desc_b.concept_id)
            SELECT COUNT(DISTINCT a.person_id)
            FROM a
            JOIN b ON a.person_id = b.person_id
            WHERE ABS(a.start_date - b.start_date) <= %(days)s;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "days": days}
    
    def patients_2conditions_and(self, v_id1, v_id2, c_id1, c_id2):
        """
        Counts of patients with condition <ARG-CONDITION><0> and <ARG-CONDITION><1>.
        """

        sql = """
            WITH 
            seed_a AS
            (SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code = %(c_id1)s
            AND c.invalid_reason IS NULL),
                        std_a AS
            (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_a s
            LEFT JOIN {schema}.concept_relationship cr ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL),
                        desc_a AS
            (SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_a sa
            JOIN {schema}.concept_ancestor ca ON ca.ancestor_concept_id = sa.standard_id
            JOIN {schema}.concept c ON c.concept_id = ca.descendant_concept_id
            AND c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL),
            seed_b AS
            (SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id2)s
            AND c.concept_code = %(c_id2)s
            AND c.invalid_reason IS NULL),
                                                      std_b AS
            (SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_b s
            LEFT JOIN {schema}.concept_relationship cr ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL),
                                                      desc_b AS
            (SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_b sb
            JOIN {schema}.concept_ancestor ca ON ca.ancestor_concept_id = sb.standard_id
            JOIN {schema}.concept c ON c.concept_id = ca.descendant_concept_id
            AND c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL),
            persons_a AS
            (SELECT DISTINCT co.person_id
            FROM {schema}.condition_occurrence co
            JOIN desc_a da ON co.condition_concept_id = da.concept_id),
                                                                              persons_b AS
            (SELECT DISTINCT co.person_id
            FROM {schema}.condition_occurrence co
            JOIN desc_b db ON co.condition_concept_id = db.concept_id) 

            SELECT COUNT(DISTINCT a.person_id)
            FROM persons_a a
            JOIN persons_b b USING (person_id);
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2}
    
    def patients_2conditions_or(self, v_id1, v_id2, c_id1, c_id2):
        """
        Counts of patients with condition <ARG-CONDITION><0> or <ARG-CONDITION><1>.
        """

        sql = """
            WITH
            seeds AS (
            SELECT %(v_id1)s::text AS vocabulary_id, %(c_id1)s::text AS concept_code
            UNION ALL
            SELECT %(v_id2)s::text,       %(c_id2)s::text
            ),
            seed_concepts AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            JOIN seeds s
            ON s.vocabulary_id = c.vocabulary_id
            AND s.concept_code  = c.concept_code
            WHERE c.invalid_reason IS NULL
            ),
            std AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, sc.src_id) AS standard_id
            FROM seed_concepts sc
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = sc.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            descendants AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = std.standard_id
            ),
            valid_condition_desc AS (
            SELECT d.concept_id
            FROM descendants d
            JOIN {schema}.concept c
            ON c.concept_id = d.concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            )
            SELECT COUNT(DISTINCT co.person_id)
            FROM {schema}.condition_occurrence co
            JOIN valid_condition_desc vcd
            ON co.condition_concept_id = vcd.concept_id;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2}
    
    def patients_4conditions_and_time(self, v_id1, v_id2, v_id3, v_id4, c_id1, c_id2, c_id3, c_id4, days):
        """
        Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, <ARG-CONDITION><2> and <ARG-CONDITION><3> within <ARG-TIMEDAYS><0> days.
        """
        
        sql = """
            WITH
            seed_a AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code  = %(c_id1)s
            AND c.invalid_reason IS NULL
            ),
            std_a AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_a s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_a AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_a sa
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sa.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            AND c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_b AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id2)s
            AND c.concept_code  = %(c_id2)s
            AND c.invalid_reason IS NULL
            ),
            std_b AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_b s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_b AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_b sb
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sb.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            AND c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_c AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id3)s
            AND c.concept_code  = %(c_id3)s
            AND c.invalid_reason IS NULL
            ),
            std_c AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_c s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_c AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_c sc
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sc.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            AND c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_d AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id4)s
            AND c.concept_code  = %(c_id4)s
            AND c.invalid_reason IS NULL
            ),
            std_d AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_d s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_d AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_d sd
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sd.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            AND c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            occ AS (
            SELECT co.person_id, co.condition_start_date::date AS start_date, 'A'::text AS grp
            FROM {schema}.condition_occurrence co
            JOIN desc_a da ON co.condition_concept_id = da.concept_id
            UNION ALL
            SELECT co.person_id, co.condition_start_date::date AS start_date, 'B'
            FROM {schema}.condition_occurrence co
            JOIN desc_b db ON co.condition_concept_id = db.concept_id
            UNION ALL
            SELECT co.person_id, co.condition_start_date::date AS start_date, 'C'
            FROM {schema}.condition_occurrence co
            JOIN desc_c dc ON co.condition_concept_id = dc.concept_id
            UNION ALL
            SELECT co.person_id, co.condition_start_date::date AS start_date, 'D'
            FROM {schema}.condition_occurrence co
            JOIN desc_d dd ON co.condition_concept_id = dd.concept_id
            ),
            persons_with_all_4 AS (
            SELECT o1.person_id
            FROM occ o1
            JOIN occ o2
            ON o2.person_id = o1.person_id
            AND o2.start_date >= o1.start_date
            AND (o2.start_date - o1.start_date) <= %(days)s::int
            GROUP BY o1.person_id, o1.start_date
            HAVING COUNT(DISTINCT o2.grp) = 4
            )
            SELECT COUNT(DISTINCT person_id)
            FROM persons_with_all_4;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "v_id3": v_id3, "c_id3": c_id3, "v_id4": v_id4, "c_id4": c_id4, "days": days}
    
    def patients_4conditions_and(self, v_id1, v_id2, v_id3, v_id4, c_id1, c_id2, c_id3, c_id4):
        """
        Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, <ARG-CONDITION><2> and <ARG-CONDITION><3>.
        """

        sql = """
            WITH
            seed_a AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code  = %(c_id1)s
            AND c.invalid_reason IS NULL
            ),
            std_a AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_a s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_a AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_a sa
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sa.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            AND c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_b AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id2)s
            AND c.concept_code  = %(c_id2)s
            AND c.invalid_reason IS NULL
            ),
            std_b AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_b s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_b AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_b sb
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sb.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            AND c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_c AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id3)s
            AND c.concept_code  = %(c_id3)s
            AND c.invalid_reason IS NULL
            ),
            std_c AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_c s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_c AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_c sc
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sc.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            AND c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_d AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id4)s
            AND c.concept_code  = %(c_id4)s
            AND c.invalid_reason IS NULL
            ),
            std_d AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_d s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_d AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_d sd
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sd.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            AND c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            occ AS (
            SELECT co.person_id, 'A'::text AS grp
            FROM {schema}.condition_occurrence co JOIN desc_a da ON co.condition_concept_id = da.concept_id
            UNION
            SELECT co.person_id, 'B'
            FROM {schema}.condition_occurrence co JOIN desc_b db ON co.condition_concept_id = db.concept_id
            UNION
            SELECT co.person_id, 'C'
            FROM {schema}.condition_occurrence co JOIN desc_c dc ON co.condition_concept_id = dc.concept_id
            UNION
            SELECT co.person_id, 'D'
            FROM {schema}.condition_occurrence co JOIN desc_d dd ON co.condition_concept_id = dd.concept_id
            )
            SELECT COUNT(*)
            FROM (
            SELECT person_id
            FROM occ
            GROUP BY person_id
            HAVING COUNT(DISTINCT grp) = 4
            ) x;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "v_id3": v_id3, "c_id3": c_id3, "v_id4": v_id4, "c_id4": c_id4}
    
    def patients_4conditions_or(self, v_id1, v_id2, v_id3, v_id4, c_id1, c_id2, c_id3, c_id4):
        """
        Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, <ARG-CONDITION><2> or <ARG-CONDITION><3>.
        """

        sql = """
            WITH
            seeds(vocabulary_id, concept_code) AS (
            SELECT %(v_id1)s::text, %(c_id1)s::text UNION ALL
            SELECT %(v_id2)s::text, %(c_id2)s::text UNION ALL
            SELECT %(v_id3)s::text, %(c_id3)s::text UNION ALL
            SELECT %(v_id4)s::text, %(c_id4)s::text
            ),
            seed_concepts AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            JOIN seeds s
            ON s.vocabulary_id = c.vocabulary_id
            AND s.concept_code  = c.concept_code
            WHERE c.invalid_reason IS NULL
            ),
            std AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, sc.src_id) AS standard_id
            FROM seed_concepts sc
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = sc.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            descendants AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = std.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            )

            SELECT COUNT(DISTINCT co.person_id)
            FROM {schema}.condition_occurrence co
            JOIN descendants d
            ON co.condition_concept_id = d.concept_id;
            """.replace("{schema}", self.schema)
        
        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "v_id3": v_id3, "c_id3": c_id3, "v_id4": v_id4, "c_id4": c_id4}
    
    def patients_3conditions_and_time(self, v_id1, v_id2, v_id3, c_id1, c_id2, c_id3, days):
        """
        Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, and <ARG-CONDITION><2> within <ARG-TIMEDAYS><0> days.
        """

        sql = """
            WITH
            seed_a AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code  = %(c_id1)s
            AND c.invalid_reason IS NULL
            ),
            std_a AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_a s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_a AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_a sa
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sa.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_b AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id2)s
            AND c.concept_code  = %(c_id2)s
            AND c.invalid_reason IS NULL
            ),
            std_b AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_b s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_b AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_b sb
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sb.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_c AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id3)s
            AND c.concept_code  = %(c_id3)s
            AND c.invalid_reason IS NULL
            ),
            std_c AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_c s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_c AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_c sc
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sc.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            occ AS (
            SELECT co.person_id, co.condition_start_date::date AS start_date, 'A'::text AS grp
            FROM {schema}.condition_occurrence co
            JOIN desc_a da ON co.condition_concept_id = da.concept_id
            UNION ALL
            SELECT co.person_id, co.condition_start_date::date, 'B'
            FROM {schema}.condition_occurrence co
            JOIN desc_b db ON co.condition_concept_id = db.concept_id
            UNION ALL
            SELECT co.person_id, co.condition_start_date::date, 'C'
            FROM {schema}.condition_occurrence co
            JOIN desc_c dc ON co.condition_concept_id = dc.concept_id
            ),
            persons_with_all_3 AS (
            SELECT o1.person_id
            FROM occ o1
            JOIN occ o2
            ON o2.person_id  = o1.person_id
            AND o2.start_date >= o1.start_date
            AND (o2.start_date - o1.start_date) <= %(days)s::int
            GROUP BY o1.person_id, o1.start_date
            HAVING COUNT(DISTINCT o2.grp) = 3
            )
            SELECT COUNT(DISTINCT person_id)
            FROM persons_with_all_3;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "v_id3": v_id3, "c_id3": c_id3, "days": days}
    
    def patients_3conditions_and(self, v_id1, v_id2, v_id3, c_id1, c_id2, c_id3):
        """
        Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, and <ARG-CONDITION><2>.
        """

        sql = """
            WITH
            seed_a AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code  = %(c_id1)s
            AND c.invalid_reason IS NULL
            ),
            std_a AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_a s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_a AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_a sa
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sa.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_b AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id2)s
            AND c.concept_code  = %(c_id2)s
            AND c.invalid_reason IS NULL
            ),
            std_b AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_b s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_b AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_b sb
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sb.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_c AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id3)s
            AND c.concept_code  = %(c_id3)s
            AND c.invalid_reason IS NULL
            ),
            std_c AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_c s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_c AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_c sc
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sc.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            )
            SELECT COUNT(*)
            FROM (
            SELECT DISTINCT co.person_id
            FROM {schema}.condition_occurrence co
            JOIN desc_a da ON co.condition_concept_id = da.concept_id

            INTERSECT

            SELECT DISTINCT co.person_id
            FROM {schema}.condition_occurrence co
            JOIN desc_b db ON co.condition_concept_id = db.concept_id

            INTERSECT

            SELECT DISTINCT co.person_id
            FROM {schema}.condition_occurrence co
            JOIN desc_c dc ON co.condition_concept_id = dc.concept_id
            ) ppl;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "v_id3": v_id3, "c_id3": c_id3}
    
    def patients_3conditions_or(self, v_id1, v_id2, v_id3, c_id1, c_id2, c_id3):
        """
        Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, or <ARG-CONDITION><2>.
        """

        sql = """
            WITH
            seeds(vocabulary_id, concept_code) AS (
            SELECT %(v_id1)s::text, %(c_id1)s::text UNION ALL
            SELECT %(v_id2)s::text, %(c_id2)s::text UNION ALL
            SELECT %(v_id3)s::text, %(c_id3)s::text
            ),
            seed_concepts AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            JOIN seeds s
            ON s.vocabulary_id = c.vocabulary_id
            AND s.concept_code  = c.concept_code
            WHERE c.invalid_reason IS NULL
            ),
            std AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, sc.src_id) AS standard_id
            FROM seed_concepts sc
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = sc.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            descendants AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = std.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            )
            SELECT COUNT(DISTINCT co.person_id)
            FROM {schema}.condition_occurrence co
            JOIN descendants d
            ON co.condition_concept_id = d.concept_id;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "v_id3": v_id3, "c_id3": c_id3}
    
    def patients_distribution_by_birth(self):
        """
        Distribution of patients by year of birth.
        """

        sql = """
            SELECT year_of_birth, COUNT(DISTINCT pe1.person_id)
            FROM {schema}.person AS pe1
            GROUP BY year_of_birth;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_condition_followed_condition(self, v_id1, v_id2, c_id1, c_id2):
        """
        How many people have Condition <ARG-CONDITION><0> followed by Condition <ARG-CONDITION><1>?
        """

        sql = """
            WITH
            seed_a AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code  = %(c_id1)s
            AND c.invalid_reason IS NULL
            ),
            std_a AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_a s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_a AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_a sa
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sa.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            seed_b AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id2)s
            AND c.concept_code  = %(c_id2)s
            AND c.invalid_reason IS NULL
            ),
            std_b AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed_b s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            desc_b AS (
            SELECT DISTINCT ca.descendant_concept_id AS concept_id
            FROM std_b sb
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = sb.standard_id
            JOIN {schema}.concept c
            ON c.concept_id = ca.descendant_concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            ),
            occ_a AS (
            SELECT co.person_id, co.condition_start_date::date AS start_date
            FROM {schema}.condition_occurrence co
            JOIN desc_a da ON co.condition_concept_id = da.concept_id
            ),
            occ_b AS (
            SELECT co.person_id, co.condition_start_date::date AS start_date
            FROM {schema}.condition_occurrence co
            JOIN desc_b db ON co.condition_concept_id = db.concept_id
            )
            SELECT COUNT(DISTINCT a.person_id)
            FROM occ_a a
            JOIN occ_b b
            ON b.person_id = a.person_id
            AND b.start_date > a.start_date;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2}
    
    def patients_condition_time_condition(self, v_id1, v_id2, c_id1, c_id2, days):
        """
        How many people have Condition <ARG-CONDITION><0> more than <ARG-TIMEDAYS><0> days after diagnosed by Condition <ARG-CONDITION><1>?
        """

        sql = """
            SELECT COUNT(DISTINCT con1.person_id)
            FROM ({schema}.condition_occurrence AS con1 JOIN (SELECT descendant_concept_id AS concept_id
                                                            FROM (SELECT *
                                                                  FROM (SELECT concept_id_2
                                                                        FROM ((SELECT concept_id
                                                                              FROM {schema}.concept
                                                                              WHERE vocabulary_id = %(v_id1)s
                                                                              AND (concept_code = %(c_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                  FROM {schema}.concept_relationship
                                                                                                                  WHERE relationship_id = 'Maps to') AS alias1
                                                                              ON concept_id = concept_id_1)) as c
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                  ON con1.condition_concept_id = concept_id)
                  JOIN ({schema}.condition_occurrence AS con2 JOIN (SELECT descendant_concept_id AS concept_id
                                                                  FROM (SELECT *
                                                                        FROM (SELECT concept_id_2
                                                                              FROM ((SELECT concept_id
                                                                                    FROM {schema}.concept
                                                                                    WHERE vocabulary_id = %(v_id2)s
                                                                                          AND (concept_code = %(c_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                              FROM {schema}.concept_relationship
                                                                                                                              WHERE relationship_id = 'Maps to') AS alias2
                                                                                    ON concept_id = concept_id_1)) as i
                                                                                    JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                              JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                        ON con2.condition_concept_id = concept_id) ON con1.person_id = con2.person_id AND CAST(EXTRACT(epoch FROM
                                                                                                                        CAST(con1.condition_start_date AS TIMESTAMP) -
                                                                                                                        CAST(con2.condition_start_date AS TIMESTAMP)) / 86400 AS BIGINT) > %(days)s;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "days": days}
    
    def patients_condition_age(self, v_id1, c_id1, age):
        """
        How many people have condition <ARG-CONDITION><0> at age <ARG-AGE><0>?
        """

        sql = """
            WITH
            seed AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code  = %(c_id1)s
            AND c.invalid_reason IS NULL
            ),
            std AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            concept_ids AS (
            SELECT standard_id AS concept_id FROM std
            UNION
            SELECT ca.descendant_concept_id
            FROM std
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = std.standard_id
            ),
            cond_concepts AS (
            SELECT ci.concept_id
            FROM concept_ids ci
            JOIN {schema}.concept c
            ON c.concept_id = ci.concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            )
            SELECT COUNT(DISTINCT p.person_id)
            FROM {schema}.person p
            JOIN {schema}.condition_occurrence co
            ON co.person_id = p.person_id
            JOIN cond_concepts cc
            ON co.condition_concept_id = cc.concept_id
            WHERE EXTRACT(YEAR FROM co.condition_start_date) - p.year_of_birth = %(age)s::int;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "age": age}
    
    def patients_condition_race(self, v_id1, c_id1, race):
        """
        How many people have condition <ARG-CONDITION><0> in the cohort of race <ARG-RACE><0>?
        """

        sql = """
            WITH
            race AS (
            SELECT c.concept_id
            FROM {schema}.concept c
            WHERE c.domain_id = 'Race'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
            AND (
                  lower(c.concept_name) = lower(%(race)s)
                  OR EXISTS (
                  SELECT 1
                  FROM {schema}.concept_synonym cs
                  WHERE cs.concept_id = c.concept_id
                  AND lower(cs.concept_synonym_name) = lower(%(race)s)
                  )
            )
            ),
            seed AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code  = %(c_id1)s
            AND c.invalid_reason IS NULL
            ),
            std AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            cond_concepts AS (
            SELECT DISTINCT c.concept_id
            FROM (
            SELECT standard_id AS concept_id FROM std
            UNION
            SELECT ca.descendant_concept_id
            FROM std
            JOIN {schema}.concept_ancestor ca
                  ON ca.ancestor_concept_id = std.standard_id
            ) x
            JOIN {schema}.concept c ON c.concept_id = x.concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            )

            SELECT COUNT(DISTINCT p.person_id)
            FROM {schema}.person p
            JOIN race r
            ON p.race_concept_id = r.concept_id
            JOIN {schema}.condition_occurrence co
            ON co.person_id = p.person_id
            JOIN cond_concepts cc
            ON co.condition_concept_id = cc.concept_id;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "race": race}
    
    def patients_condition_state(self, v_id1, c_id1, state):
        """
        How many people have condition <ARG-CONDITION><0> in the state <ARG-STATE><0>?
        """

        sql = """
            WITH
            seed AS (
            SELECT c.concept_id AS src_id
            FROM {schema}.concept c
            WHERE c.vocabulary_id = %(v_id1)s
            AND c.concept_code  = %(c_id1)s
            AND c.invalid_reason IS NULL
            ),
            std AS (
            SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
            FROM seed s
            LEFT JOIN {schema}.concept_relationship cr
            ON cr.concept_id_1 = s.src_id
            AND cr.relationship_id = 'Maps to'
            AND cr.invalid_reason IS NULL
            ),
            concept_ids AS (
            SELECT standard_id AS concept_id FROM std
            UNION
            SELECT ca.descendant_concept_id
            FROM std
            JOIN {schema}.concept_ancestor ca
            ON ca.ancestor_concept_id = std.standard_id
            ),
            cond_concepts AS (
            SELECT ci.concept_id
            FROM concept_ids ci
            JOIN {schema}.concept c
            ON c.concept_id = ci.concept_id
            WHERE c.standard_concept = 'S'
            AND c.domain_id = 'Condition'
            AND c.invalid_reason IS NULL
            )
            SELECT COUNT(DISTINCT p.person_id) AS number_of_patients
            FROM {schema}.person p
            JOIN {schema}.location l
            ON l.location_id = p.location_id
            JOIN {schema}.condition_occurrence co
            ON co.person_id = p.person_id
            JOIN cond_concepts cc
            ON co.condition_concept_id = cc.concept_id
            WHERE UPPER(TRIM(l.state)) = UPPER(TRIM(%(state)s));
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "state": state}
    
    def patients_condition_year(self, v_id1, c_id1, year):
        """
        How many people have condition <ARG-CONDITION><0> in the year <ARG-TIMEYEARS><0>?
        """

        sql = """
            SELECT COUNT(DISTINCT con1.person_id) AS number_of_patients
            FROM {schema}.condition_occurrence AS con1
                  JOIN (SELECT descendant_concept_id AS concept_id
                        FROM (SELECT *
                              FROM (SELECT concept_id_2
                                    FROM ((SELECT concept_id
                                          FROM {schema}.concept
                                          WHERE vocabulary_id = %(v_id1)s
                                                AND (concept_code = %(c_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                              FROM {schema}.concept_relationship
                                                                              WHERE relationship_id = 'Maps to') AS alias1
                                          ON concept_id = concept_id_1)) as c
                                          JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                    JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                        ON con1.condition_concept_id = concept_id
            WHERE EXTRACT(year FROM con1.condition_start_date) = %(year)s;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "year": year}
    
    def patients_drug_time_drug(self, v_id1, v_id2, d_id1, d_id2, days):
        """
        How many people have treated by drug <ARG-DRUG><0> after more than <ARG-TIMEDAYS><0> days of starting with drug <ARG-DRUG><1>?
        """

        sql = """
            SELECT COUNT(DISTINCT dr1.person_id)
            FROM ({schema}.drug_exposure AS dr1 JOIN (SELECT descendant_concept_id AS concept_id
                                                FROM (SELECT *
                                                      FROM (SELECT concept_id_2
                                                            FROM ((SELECT concept_id
                                                                  FROM {schema}.concept
                                                                  WHERE vocabulary_id = %(v_id1)s
                                                                        AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias1
                                                                  ON concept_id = concept_id_1)) as c
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                  ON dr1.drug_concept_id = concept_id)
                  JOIN ({schema}.drug_exposure AS dr2 JOIN (SELECT descendant_concept_id AS concept_id
                                                            FROM (SELECT *
                                                                  FROM (SELECT concept_id_2
                                                                        FROM ((SELECT concept_id
                                                                              FROM {schema}.concept
                                                                              WHERE vocabulary_id = %(v_id2)s
                                                                              AND (concept_code = %(d_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                  FROM {schema}.concept_relationship
                                                                                                                  WHERE relationship_id = 'Maps to') AS alias2
                                                                              ON concept_id = concept_id_1)) as i
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                        ON dr2.drug_concept_id = concept_id) ON dr1.person_id = dr2.person_id AND CAST(EXTRACT(epoch FROM
                                                                                                                  CAST(dr1.drug_exposure_start_date AS TIMESTAMP) -
                                                                                                                  CAST(dr2.drug_exposure_start_date AS TIMESTAMP)) / 86400 AS BIGINT) > %(days)s;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2, "days": days}
    
    def patients_drug_followed_drug(self, v_id1, v_id2, d_id1, d_id2):
        """
        How many people have treated by drug <ARG-DRUG><0> followed by drug <ARG-DRUG><1>?
        """

        sql = """
            SELECT COUNT(DISTINCT dr1.person_id)
            FROM ({schema}.drug_exposure AS dr1 JOIN (SELECT descendant_concept_id AS concept_id
                                                FROM (SELECT *
                                                      FROM (SELECT concept_id_2
                                                            FROM ((SELECT concept_id
                                                                  FROM {schema}.concept
                                                                  WHERE vocabulary_id = %(v_id1)s
                                                                        AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias1
                                                                  ON concept_id = concept_id_1)) as c
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                  ON dr1.drug_concept_id = concept_id)
                  JOIN ({schema}.drug_exposure AS dr2 JOIN (SELECT descendant_concept_id AS concept_id
                                                            FROM (SELECT *
                                                                  FROM (SELECT concept_id_2
                                                                        FROM ((SELECT concept_id
                                                                              FROM {schema}.concept
                                                                              WHERE vocabulary_id = %(v_id2)s
                                                                              AND (concept_code = %(d_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                  FROM {schema}.concept_relationship
                                                                                                                  WHERE relationship_id = 'Maps to') AS alias2
                                                                              ON concept_id = concept_id_1)) as i
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                        ON dr2.drug_concept_id = concept_id) ON dr1.person_id = dr2.person_id AND CAST(EXTRACT(epoch FROM
                                                                                                                  CAST(dr2.drug_exposure_start_date AS TIMESTAMP) -
                                                                                                                  CAST(dr1.drug_exposure_start_date AS TIMESTAMP)) / 86400 AS BIGINT) > 0;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2}
    
    def patients_condition_ethnicity(self, v_id1, c_id1, ethnicity):
        """
        How many people of ethnicity <ARG-ETHNICITY><0> have condition <ARG-CONDITION><0>?
        """

        sql = """
            SELECT COUNT(DISTINCT pe1.person_id)
            FROM (({schema}.person AS pe1 JOIN (SELECT concept_id
                                          FROM {schema}.concept
                                          WHERE concept_name = %(ethnicity)s
                                                AND domain_id = 'Ethnicity'
                                                AND standard_concept = 'S') AS alias1
                  ON pe1.ethnicity_concept_id = concept_id) JOIN ((SELECT descendant_concept_id AS concept_id
                                                                  FROM (SELECT *
                                                                        FROM (SELECT concept_id_2
                                                                              FROM ((SELECT concept_id
                                                                                    FROM {schema}.concept
                                                                                    WHERE vocabulary_id = %(v_id1)s
                                                                                    AND (concept_code = %(c_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                        FROM {schema}.concept_relationship
                                                                                                                        WHERE relationship_id = 'Maps to') AS alias2
                                                                                    ON concept_id = concept_id_1)) as c
                                                                              JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                        JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci" JOIN {schema}.condition_occurrence AS con1
                                                                  ON concept_id = con1.condition_concept_id)
                  ON pe1.person_id = con1.person_id);
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "ethnicity": ethnicity}
    
    def patients_drug_year(self, v_id1, d_id1, year):
        """
        How many people were taking drug <ARG-DRUG><0> in year <ARG-TIMEYEARS><0>.
        """

        sql = """
            SELECT COUNT(DISTINCT dr1.person_id)
            FROM ({schema}.drug_exposure AS dr1 JOIN (SELECT descendant_concept_id AS concept_id
                                                FROM (SELECT *
                                                      FROM (SELECT concept_id_2
                                                            FROM ((SELECT concept_id
                                                                  FROM {schema}.concept
                                                                  WHERE vocabulary_id = %(v_id1)s
                                                                        AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                            FROM {schema}.concept_relationship
                                                                                                            WHERE relationship_id = 'Maps to') AS alias1
                                                                  ON concept_id = concept_id_1)) as c
                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                  ON dr1.drug_concept_id = concept_id)
            WHERE EXTRACT(year FROM dr1.drug_exposure_start_date) <= %(year)s
            AND EXTRACT(year FROM dr1.drug_exposure_end_date) >= %(year)s;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "year": year}
    
    def patients_drug_after_condition(self, v_id1, v_id2, d_id1, c_id1):
        """
        How many people were treated by drug <ARG-DRUG><0> after the diagnosis of Condition <ARG-CONDITION><0>?
        """

        sql = """
            SELECT COUNT(DISTINCT con1.person_id)
            FROM ({schema}.condition_occurrence AS con1 JOIN (SELECT descendant_concept_id AS concept_id
                                                            FROM (SELECT *
                                                                  FROM (SELECT concept_id_2
                                                                        FROM ((SELECT concept_id
                                                                              FROM {schema}.concept
                                                                              WHERE vocabulary_id = %(v_id1)s
                                                                              AND (concept_code = %(c_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                  FROM {schema}.concept_relationship
                                                                                                                  WHERE relationship_id = 'Maps to') AS alias1
                                                                              ON concept_id = concept_id_1)) as c
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                  ON con1.condition_concept_id = concept_id)
                  JOIN ({schema}.drug_exposure AS dr1 JOIN (SELECT descendant_concept_id AS concept_id
                                                            FROM (SELECT *
                                                                  FROM (SELECT concept_id_2
                                                                        FROM ((SELECT concept_id
                                                                              FROM {schema}.concept
                                                                              WHERE vocabulary_id = %(v_id2)s
                                                                              AND (concept_code = %(d_id1)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                        FROM {schema}.concept_relationship
                                                                                                                        WHERE relationship_id = 'Maps to') AS alias2
                                                                              ON concept_id = concept_id_1)) as i
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                        ON dr1.drug_concept_id = concept_id)
                        ON con1.person_id = dr1.person_id AND con1.condition_start_date < dr1.drug_exposure_start_date;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "d_id1": d_id1}
    
    def patients_drug_time_after_condition(self, v_id1, v_id2, d_id1, c_id1, days):
        """
        How many people were treated by drug <ARG-DRUG><0> more than <ARG-TIMEDAYS><0> days after being diagnosed of Condition <ARG-CONDITION><0>?
        """

        sql = """
            SELECT COUNT(DISTINCT con1.person_id)
            FROM ({schema}.condition_occurrence AS con1 JOIN (SELECT descendant_concept_id AS concept_id
                                                            FROM (SELECT *
                                                                  FROM (SELECT concept_id_2
                                                                        FROM ((SELECT concept_id
                                                                              FROM {schema}.concept
                                                                              WHERE vocabulary_id = %(v_id1)s
                                                                              AND (concept_code = %(c_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                  FROM {schema}.concept_relationship
                                                                                                                  WHERE relationship_id = 'Maps to') AS alias1
                                                                              ON concept_id = concept_id_1)) as c
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                  ON con1.condition_concept_id = concept_id)
                  JOIN ({schema}.drug_exposure AS dr1 JOIN (SELECT descendant_concept_id AS concept_id
                                                            FROM (SELECT *
                                                                  FROM (SELECT concept_id_2
                                                                        FROM ((SELECT concept_id
                                                                              FROM {schema}.concept
                                                                              WHERE vocabulary_id = %(v_id2)s
                                                                              AND (concept_code = %(d_id1)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                        FROM {schema}.concept_relationship
                                                                                                                        WHERE relationship_id = 'Maps to') AS alias2
                                                                              ON concept_id = concept_id_1)) as i
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                        ON dr1.drug_concept_id = concept_id) ON con1.person_id = dr1.person_id AND CAST(EXTRACT(epoch FROM
                                                                                                                  CAST(dr1.drug_exposure_start_date AS TIMESTAMP) -
                                                                                                                  CAST(con1.condition_start_date AS TIMESTAMP)) / 86400 AS BIGINT) > %(days)s;
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "d_id1": d_id1, "days": days}
    
    def patients_gender_condition(self, v_id1, c_id1, gender):
        """
        Number of <ARG-GENDER><O> patients with <ARG-CONDITION><O>.
        """

        sql = """
            SELECT COUNT(DISTINCT pe1.person_id)
            FROM (({schema}.person AS pe1 JOIN (SELECT concept_id
                                          FROM {schema}.concept
                                          WHERE concept_name = %(gender)s
                                                AND domain_id = 'Gender'
                                                AND standard_concept = 'S') AS alias1
                  ON pe1.gender_concept_id = concept_id) JOIN ({schema}.condition_occurrence AS co JOIN (SELECT descendant_concept_id AS concept_id
                                                                                                      FROM (SELECT *
                                                                                                            FROM (SELECT concept_id_2
                                                                                                                  FROM ((SELECT concept_id
                                                                                                                        FROM {schema}.concept
                                                                                                                        WHERE vocabulary_id = %(v_id1)s
                                                                                                                        AND (concept_code = %(c_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                            FROM {schema}.concept_relationship
                                                                                                                                                            WHERE relationship_id = 'Maps to') AS alias2
                                                                                                                        ON concept_id = concept_id_1)) as c
                                                                                                                  JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                                                            ON co.condition_concept_id = concept_id) AS pe2
                  ON pe1.person_id = pe2.person_id);
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "gender": gender}
    
    def patients_year(self, year):
        """
        Number of patients born in year <ARG-TIMEYEARS><0>.
        """

        sql = """
            SELECT COUNT(DISTINCT pe1.person_id)
            FROM {schema}.person AS pe1
            WHERE year_of_birth = %(year)s;
            """.replace("{schema}", self.schema)

        return sql, {"year": year}
    
    def patients_gender_state(self):
        """
        Number of patients by gender and state.
        """

        sql = """
            SELECT gt.gender, loc1.state, COUNT(DISTINCT pe1.person_id) AS number_of_patients
            FROM {schema}.person AS pe1
                  JOIN (SELECT concept_id, concept_name AS gender
                        FROM {schema}.concept
                        WHERE domain_id = 'Gender' AND standard_concept = 'S') AS gt ON pe1.gender_concept_id = gt.concept_id
                  JOIN {schema}.location AS loc1 ON pe1.location_id = loc1.location_id
            GROUP BY gt.gender, loc1.state;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_group_by_ethnicity_location(self):
        """
        Number of patients grouped by ethnicity and residence state location.
        """

        sql = """
            SELECT et.ethnicity, st.state, COUNT(DISTINCT pe1.person_id)
            FROM (({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS ethnicity
                                          FROM {schema}.concept
                                          WHERE domain_id = 'Ethnicity' AND standard_concept = 'S') AS et
                  ON pe1.ethnicity_concept_id = et.concept_id) JOIN (SELECT location_id, state FROM {schema}.location) AS st
                  ON pe1.location_id = st.location_id)
            GROUP BY et.ethnicity, st.state;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_group_by_ethnicity_birth(self):
        """
        Number of patients grouped by ethnicity and year of birth.
        """

        sql = """
            SELECT ethnicity, year_of_birth, COUNT(DISTINCT pe1.person_id)
            FROM ({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS ethnicity
                                          FROM {schema}.concept
                                          WHERE domain_id = 'Ethnicity' AND standard_concept = 'S') AS alias1
                  ON pe1.ethnicity_concept_id = concept_id)
            GROUP BY ethnicity, year_of_birth;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_group_by_ethnicity(self):
        """
        Number of patients grouped by ethnicity.
        """

        sql = """
            SELECT ethnicity, COUNT(DISTINCT pe1.person_id)
            FROM {schema}.person AS pe1
                  JOIN (SELECT concept_id, concept_name AS ethnicity
                        FROM {schema}.concept
                        WHERE domain_id = 'Ethnicity' AND standard_concept = 'S') AS alias1
                        ON pe1.ethnicity_concept_id = concept_id
            GROUP BY ethnicity;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_group_by_gender(self):
        """
        Number of patients grouped by gender.
        """

        sql = """
            SELECT gender, COUNT(DISTINCT pe1.person_id)
            FROM ({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS gender
                                          FROM {schema}.concept
                                          WHERE domain_id = 'Gender' AND standard_concept = 'S') AS alias1
                  ON pe1.gender_concept_id = concept_id)
            GROUP BY gender;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_group_by_race_ethnicity(self):
        """
        Number of patients grouped by race and ethnicity.
        """

        sql = """
            SELECT rt.race, et.ethnicity, COUNT(DISTINCT pe1.person_id)
            FROM (({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS race
                                          FROM {schema}.concept
                                          WHERE domain_id = 'Race' AND standard_concept = 'S') AS rt
                  ON pe1.race_concept_id = rt.concept_id) JOIN (SELECT concept_id, concept_name AS ethnicity
                                                            FROM {schema}.concept
                                                            WHERE domain_id = 'Ethnicity' AND standard_concept = 'S') AS et
                  ON pe1.ethnicity_concept_id = et.concept_id)
            GROUP BY rt.race, et.ethnicity;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_grouped_by_race_gender(self):
        """
        Number of patients grouped by race and gender.
        """

        sql = """
            SELECT rt.race, gen_temp1.gender, COUNT(DISTINCT pe1.person_id)
            FROM (({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS race
                                          FROM {schema}.concept
                                          WHERE domain_id = 'Race' AND standard_concept = 'S') AS rt
                  ON pe1.race_concept_id = rt.concept_id) JOIN (SELECT concept_id, concept_name AS gender
                                                            FROM {schema}.concept
                                                            WHERE domain_id = 'Gender' AND standard_concept = 'S') AS gen_temp1
                  ON pe1.gender_concept_id = gen_temp1.concept_id)
            GROUP BY rt.race, gen_temp1.gender;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_group_by_race_location(self):
        """
        Number of patients grouped by race and residence state location.
        """

        sql = """
            SELECT rt.race, st.state, COUNT(DISTINCT pe1.person_id)
            FROM (({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS race
                                          FROM {schema}.concept
                                          WHERE domain_id = 'Race' AND standard_concept = 'S') AS rt
                  ON pe1.race_concept_id = rt.concept_id) JOIN (SELECT location_id, state FROM {schema}.location) AS st
                  ON pe1.location_id = st.location_id)
            GROUP BY rt.race, st.state;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_group_by_race_birth(self):
        """
        Number of patients grouped by race and year of birth.
        """

        sql = """
            SELECT rt.race, pe1.year_of_birth, COUNT(DISTINCT pe1.person_id)
            FROM {schema}.person AS pe1
                  JOIN (SELECT concept_id, concept_name AS race
                        FROM {schema}.concept
                        WHERE domain_id = 'Race' AND standard_concept = 'S') AS rt ON pe1.race_concept_id = rt.concept_id
            GROUP BY rt.race, pe1.year_of_birth;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_group_by_location(self):
        """
        Number of patients grouped by residence state location.
        """

        sql = """
            SELECT st.state, COUNT(DISTINCT pe1.person_id)
            FROM {schema}.person AS pe1
                  JOIN (SELECT location_id, state FROM {schema}.location) AS st ON pe1.location_id = st.location_id
            GROUP BY st.state;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_group_by_birth_gender(self):
        """
        Number of patients grouped by year of birth and gender.
        """

        sql = """
            SELECT pe1.year_of_birth, gt.gender, COUNT(DISTINCT pe1.person_id)
            FROM {schema}.person AS pe1
                  JOIN (SELECT concept_id, concept_name AS gender
                        FROM {schema}.concept
                        WHERE domain_id = 'Gender' AND standard_concept = 'S') AS gt ON pe1.gender_concept_id = gt.concept_id
            GROUP BY pe1.year_of_birth, gt.gender;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_group_by_birth_location(self):
        """
        Number of patients grouped by year of birth and residence state location.
        """

        sql = """
            SELECT pe1.year_of_birth, st.state, COUNT(DISTINCT pe1.person_id)
            FROM {schema}.person AS pe1
                  JOIN (SELECT location_id, state FROM {schema}.location) AS st ON pe1.location_id = st.location_id
            GROUP BY pe1.year_of_birth, st.state;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_count(self):
        """
        Number of patients in the dataset.
        """

        sql = """
            SELECT COUNT(DISTINCT pe1.person_id)
            FROM {schema}.person AS pe1;
            """.replace("{schema}", self.schema)

        return sql, {}
    
    def patients_count_by_ethnicity(self, ethnicity):
        """
        Number of patients of ethnicity <ARG-ETHNICITY><0>.
        """

        sql = """
            SELECT COUNT(DISTINCT pe1.person_id)
            FROM {schema}.person AS pe1
                  JOIN (SELECT concept_id
                        FROM {schema}.concept
                        WHERE concept_name = %(ethnicity)s
                        AND domain_id = 'Ethnicity'
                        AND standard_concept = 'S') AS alias1 ON pe1.ethnicity_concept_id = concept_id;
            """.replace("{schema}", self.schema)

        return sql, {"ethnicity": ethnicity}
    
    def patients_count_by_race(self, race):
        """
        Number of patients of race <ARG-RACE><0>.
        """

        sql = """
            SELECT COUNT(DISTINCT pe1.person_id) AS number_of_patients
            FROM {schema}.person AS pe1
                  JOIN (SELECT concept_id
                        FROM {schema}.concept
                        WHERE concept_name = %(race)s AND domain_id = 'Race' AND standard_concept = 'S') AS alias1
                        ON pe1.race_concept_id = concept_id;
            """.replace("{schema}", self.schema)

        return sql, {"race": race}
    
    def patients_count_by_gender(self, gender):
        """
        Number of patients of specific gender <ARG-GENDER><0>.
        """

        sql = """
            SELECT COUNT(DISTINCT pe1.person_id)
            FROM {schema}.person AS pe1
                  JOIN (SELECT concept_id
                        FROM {schema}.concept
                        WHERE concept_name = %(gender)s AND domain_id = 'Gender' AND standard_concept = 'S') AS alias1
                        ON pe1.gender_concept_id = concept_id;
            """.replace("{schema}", self.schema)

        return sql, {"gender": gender}
    
    def patients_drug(self, v_id1, d_id1):
        """
        Number of patients taking <ARG-DRUG><0>.
        """

        sql = """
            SELECT COUNT(DISTINCT pe1.person_id)
            FROM ({schema}.person AS pe1 JOIN ((SELECT descendant_concept_id AS concept_id
                                          FROM (SELECT *
                                                FROM (SELECT concept_id_2
                                                      FROM ((SELECT concept_id
                                                            FROM {schema}.concept
                                                            WHERE vocabulary_id = %(v_id1)s
                                                                  AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                      FROM {schema}.concept_relationship
                                                                                                      WHERE relationship_id = 'Maps to') AS alias1
                                                            ON concept_id = concept_id_1)) as c
                                                            JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                      JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci" JOIN {schema}.drug_exposure AS dr1
                                          ON concept_id = drug_concept_id) ON pe1.person_id = dr1.person_id);
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1}
    
    def patients_condition(self, v_id1, c_id1):
        """
        Number of patients with <ARG-CONDITION><O>.
        """

        sql = """
            SELECT COUNT(DISTINCT con1.person_id)
            FROM ({schema}.condition_occurrence AS con1 JOIN (SELECT descendant_concept_id AS concept_id
                                                            FROM (SELECT *
                                                                  FROM (SELECT concept_id_2
                                                                        FROM ((SELECT concept_id
                                                                              FROM {schema}.concept
                                                                              WHERE vocabulary_id = %(v_id1)s
                                                                              AND (concept_code = %(c_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                  FROM {schema}.concept_relationship
                                                                                                                  WHERE relationship_id = 'Maps to') AS alias1
                                                                              ON concept_id = concept_id_1)) as c
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                  ON con1.condition_concept_id = concept_id);
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1}
    
    def patients_count_by_location(self, location):
        """
        Number of patients with residence state location at <ARG-STATE><0>.
        """

        sql = """
            SELECT COUNT(DISTINCT pe1.person_id)
            FROM {schema}.person AS pe1
                  JOIN (SELECT location_id FROM {schema}.location WHERE state = %(location)s) AS state_temp1
                        ON pe1.location_id = state_temp1.location_id;
            """.replace("{schema}", self.schema)

        return sql, {"location": location}
    
    def patients_condition_group_by_year(self, v_id1, c_id1):
        """
        counts of patients with condition <ARG-CONDITION><0> grouped by year of diagnosis.
        """

        sql = """
            SELECT EXTRACT(year FROM con1.condition_start_date) AS year, COUNT(DISTINCT con1.person_id)
            FROM {schema}.condition_occurrence AS con1
                  JOIN (SELECT descendant_concept_id AS concept_id
                        FROM (SELECT *
                              FROM (SELECT concept_id_2
                                    FROM ((SELECT concept_id
                                          FROM {schema}.concept
                                          WHERE vocabulary_id = %(v_id1)s
                                                AND (concept_code = %(c_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                              FROM {schema}.concept_relationship
                                                                              WHERE relationship_id = 'Maps to') AS alias1
                                          ON concept_id = concept_id_1)) as c
                                          JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                    JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                        ON con1.condition_concept_id = concept_id
            GROUP BY EXTRACT(year FROM con1.condition_start_date);
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1}
    
    def patients_drug_group_by_year(self, v_id1, d_id1):
        """
        counts of patients with taking drug <ARG-DRUG><0> grouped by year of prescription.
        """

        sql = """
            SELECT EXTRACT(year FROM dr1.drug_exposure_start_date) AS year, COUNT(DISTINCT dr1.person_id)
            FROM {schema}.drug_exposure AS dr1
                  JOIN (SELECT descendant_concept_id AS concept_id
                        FROM (SELECT *
                              FROM (SELECT concept_id_2
                                    FROM ((SELECT concept_id
                                          FROM {schema}.concept
                                          WHERE vocabulary_id = %(v_id1)s
                                                AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                    FROM {schema}.concept_relationship
                                                                                    WHERE relationship_id = 'Maps to') AS alias1
                                          ON concept_id = concept_id_1)) as c
                                          JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                    JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
                        ON dr1.drug_concept_id = concept_id
            GROUP BY EXTRACT(year FROM dr1.drug_exposure_start_date);
            """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1}