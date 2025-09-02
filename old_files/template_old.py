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
            SELECT gender, ethnicity, COUNT(pe1.person_id) AS number_of_patients
            FROM {schema}.person AS pe1
            JOIN (SELECT concept_id, concept_name AS gender
               FROM {schema}.concept
               WHERE domain_id = 'Gender'
                 AND standard_concept = 'S') AS gen_temp1
              ON pe1.gender_concept_id = gen_temp1.concept_id
            JOIN (SELECT concept_id, concept_name AS ethnicity
               FROM {schema}.concept
               WHERE domain_id = 'Ethnicity'
                 AND standard_concept = 'S') AS eth_temp1
              ON pe1.ethnicity_concept_id = eth_temp1.concept_id
            GROUP BY gender, ethnicity;
            """.replace("{schema}", self.schema)
        
        return sql, desc, {}
    
    def patients_group_by_race(self):
        desc = f"""
            Count of patients grouped by race.
            """
        
        sql = """
            SELECT race, COUNT(DISTINCT pe1.person_id) AS number_of_patients
            FROM {schema}.person AS pe1
            JOIN (SELECT concept_id, concept_name AS race
               FROM {schema}.concept
               WHERE domain_id = 'Race'
                 AND standard_concept = 'S') AS alias1 ON pe1.race_concept_id = concept_id
            GROUP BY race;
                """.replace("{schema}", self.schema)
        
        return sql, desc, {}
    
    def patients_2drugs_and_time(self, v_id1: str, v_id2: str, d_id1: str, d_id2: str, days: int):
        """
        Counts of patients taking drug <ARG-DRUG><0> and <ARG-DRUG><1> within <ARG-TIMEDAYS><0> days.
        """
        
        sql = """
            SELECT COUNT(DISTINCT a.person_id)
            FROM ((SELECT person_id, MIN(drug_exposure_start_date) AS min_start_date
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
            GROUP BY person_id) AS a JOIN (SELECT person_id, MIN(drug_exposure_start_date) AS min_start_date
                                      FROM ({schema}.drug_exposure AS dr2 JOIN (SELECT descendant_concept_id AS concept_id
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
                                      GROUP BY person_id) AS b ON a.person_id = b.person_id)
            WHERE CAST(EXTRACT(epoch FROM CAST(LEAST(a.min_start_date, b.min_start_date) AS TIMESTAMP) -
            CAST(GREATEST(a.min_start_date, b.min_start_date) AS TIMESTAMP)) / 86400 AS BIGINT) < %(days)s;
            """.replace("{schema}", self.schema)
        
        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2, "days": days}
    
    def patients_2drugs_and(self, v_id1, v_id2, d_id1, d_id2):
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
        """
        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2}
    
    def patients_2drugs_or(self, v_id1, v_id2, d_id1, d_id2):
        """
        Query: Counts of patients taking drug <ARG-DRUG><0> or <ARG-DRUG><1>.
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
        FROM ((((SELECT person_id, MIN(drug_exposure_start_date) AS min_start_date
        FROM ({schema}.drug_exposure AS dr1 JOIN (SELECT descendant_concept_id AS concept_id
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
               ON dr1.drug_concept_id = concept_id)
        GROUP BY person_id) AS a JOIN (SELECT person_id, MIN(drug_exposure_start_date) AS min_start_date
                                        FROM ({schema}.drug_exposure AS dr2 JOIN (SELECT descendant_concept_id AS concept_id
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
                                              ON dr2.drug_concept_id = concept_id)
                                        GROUP BY person_id) AS b
        ON a.person_id = b.person_id) JOIN (SELECT person_id, MIN(drug_exposure_start_date) AS min_start_date
                                            FROM ({schema}.drug_exposure AS dr3 JOIN (SELECT descendant_concept_id AS concept_id
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
                                                  ON dr3.drug_concept_id = concept_id)
                                            GROUP BY person_id) AS c
        ON b.person_id = c.person_id) JOIN (SELECT person_id, MIN(drug_exposure_start_date) AS min_start_date
                                           FROM ({schema}.drug_exposure AS dr4 JOIN (SELECT descendant_concept_id AS concept_id
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
                                                 ON dr4.drug_concept_id = concept_id)
                                           GROUP BY person_id) AS d ON c.person_id = d.person_id)
        WHERE CAST(EXTRACT(epoch FROM
                   CAST(LEAST(a.min_start_date, b.min_start_date, c.min_start_date, d.min_start_date) AS TIMESTAMP) -
                   CAST(GREATEST(a.min_start_date, b.min_start_date, c.min_start_date,
                                 d.min_start_date) AS TIMESTAMP)) / 86400 AS BIGINT) < %(days)s;
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2, "v_id3": v_id3, "d_id3": d_id3, "v_id4": v_id4, "d_id4": d_id4, "days": days}
    
    def patients_4drugs_and(self, v_id1, v_id2, v_id3, v_id4, d_id1, d_id2, d_id3, d_id4):
        """
        Counts of patients taking drug <ARG-DRUG><0>, <ARG-DRUG><1>, <ARG-DRUG><2> and <ARG-DRUG><3>.
        """

        sql = """
        SELECT COUNT(DISTINCT dr1.person_id)
        FROM (((({schema}.drug_exposure AS dr1 JOIN (SELECT descendant_concept_id AS concept_id
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
                                                ON drug_concept_id = concept_id)
        ON dr2.person_id = dr3.person_id) JOIN ({schema}.drug_exposure AS dr4 JOIN (SELECT descendant_concept_id AS concept_id
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
                                                ON drug_concept_id = concept_id) ON dr3.person_id = dr4.person_id);
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
                                                                AND (concept_code = %(d_id2)s) as cci2 JOIN (SELECT concept_id_1, concept_id_2
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
        FROM (((SELECT person_id, MIN(drug_exposure_start_date) AS min_start_date
        FROM ({schema}.drug_exposure AS dr1 JOIN (SELECT descendant_concept_id AS concept_id
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
              ON dr1.drug_concept_id = concept_id)
        GROUP BY person_id) AS a JOIN (SELECT person_id, MIN(drug_exposure_start_date) AS min_start_date
                                       FROM ({schema}.drug_exposure AS dr2 JOIN (SELECT descendant_concept_id AS concept_id
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
                                             ON dr2.drug_concept_id = concept_id)
                                       GROUP BY person_id) AS b
        ON a.person_id = b.person_id) JOIN (SELECT person_id, MIN(drug_exposure_start_date) AS min_start_date
                                           FROM ({schema}.drug_exposure AS dr3 JOIN (SELECT descendant_concept_id AS concept_id
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
                                                 ON dr3.drug_concept_id = concept_id)
                                           GROUP BY person_id) AS c ON b.person_id = c.person_id)
        WHERE CAST(EXTRACT(epoch FROM CAST(LEAST(a.min_start_date, b.min_start_date, c.min_start_date) AS TIMESTAMP) -
                              CAST(GREATEST(a.min_start_date, b.min_start_date, c.min_start_date) AS TIMESTAMP)) /
           86400 AS BIGINT) < %(days)s;
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1, "v_id2": v_id2, "d_id2": d_id2, "v_id3": v_id3, "d_id3": d_id3, "days": days}
    
    def patients_3drugs_and(self, v_id1, v_id2, v_id3, d_id1, d_id2, d_id3):
        """
        Counts of patients taking drug <ARG-DRUG><0>, <ARG-DRUG><1>, and <ARG-DRUG><2>.
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
                                               ON drug_concept_id = concept_id) ON dr2.person_id = dr3.person_id);
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
        SELECT COUNT(DISTINCT a.person_id)
        FROM ((SELECT person_id, MIN(condition_start_date) AS min_start_date
        FROM ({schema}.condition_occurrence AS con1 JOIN (SELECT descendant_concept_id AS concept_id
                                                       FROM (SELECT *
                                                             FROM (SELECT concept_id_2
                                                                   FROM ((SELECT concept_id
                                                                          FROM {schema}.concept
                                                                          WHERE vocabulary_id = %(v_id1)s
                                                                            AND (concept_code = %(c_id1)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                      FROM {schema}.concept_relationship
                                                                                                                      WHERE relationship_id = 'Maps to') AS alias1
                                                                         ON concept_id = concept_id_1)) as i
                                                                      JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                                JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
             ON con1.condition_concept_id = concept_id)
        GROUP BY person_id) AS a JOIN (SELECT person_id, MIN(condition_start_date) AS min_start_date
                                      FROM ({schema}.condition_occurrence AS con2 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                      FROM (SELECT *
                                                                                            FROM (SELECT concept_id_2
                                                                                                  FROM ((SELECT concept_id
                                                                                                         FROM {schema}.concept
                                                                                                         WHERE vocabulary_id = %(v_id2)s
                                                                                                           AND (concept_code = %(c_id2)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                       FROM {schema}.concept_relationship
                                                                                                                                                       WHERE relationship_id = 'Maps to') AS alias2
                                                                                                        ON concept_id = concept_id_1)) as c
                                                                                                     JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                                               JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                                            ON con2.condition_concept_id = concept_id)
                                      GROUP BY person_id) AS b ON a.person_id = b.person_id)
        WHERE (GREATEST(a.min_start_date, b.min_start_date) - LEAST(a.min_start_date, b.min_start_date)) <= %(days)s;
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "days": days}
    
    def patients_conditions_and(self, v_id1, v_id2, c_id1, c_id2):
        """
        Counts of patients with condition <ARG-CONDITION><0> and <ARG-CONDITION><1>.
        """

        sql = """
        SELECT COUNT(DISTINCT pe1.person_id)
        FROM (({schema}.condition_occurrence JOIN (SELECT descendant_concept_id AS concept_id
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
        ON condition_concept_id = concept_id) AS pe1 JOIN ({schema}.condition_occurrence JOIN (SELECT descendant_concept_id AS concept_id
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
                                                          ON condition_concept_id = concept_id) AS pe2
        ON pe1.person_id = pe2.person_id);
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2}
    
    def patients_conditions_or(self, v_id1, v_id2, c_id1, c_id2):
        """
        Counts of patients with condition <ARG-CONDITION><0> or <ARG-CONDITION><1>.
        """

        sql = """
        SELECT COUNT(DISTINCT person_id)
        FROM ({schema}.condition_occurrence JOIN ((SELECT descendant_concept_id AS concept_id
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
                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id)
                                        UNION
                                        (SELECT descendant_concept_id AS concept_id
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
                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id)) as "ccia1ci2c*caciccia2ci2c*caci"
        ON condition_concept_id = concept_id);
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2}
    
    def patients_conditions_and_time(self, v_id1, v_id2, v_id3, v_id4, c_id1, c_id2, c_id3, c_id4, days):
        """
        Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, <ARG-CONDITION><2> and <ARG-CONDITION><3> within <ARG-TIMEDAYS><0> days.
        """
        
        sql = """
        SELECT COUNT(DISTINCT a.person_id)
        FROM ((((SELECT person_id, MIN(condition_start_date) AS min_start_date
        FROM ({schema}.condition_occurrence AS con1 JOIN (SELECT descendant_concept_id AS concept_id
                                                         FROM (SELECT *
                                                               FROM (SELECT concept_id_2
                                                                     FROM ((SELECT concept_id
                                                                            FROM {schema}.concept
                                                                            WHERE vocabulary_id = %(v_id1)s
                                                                              AND (concept_code = %(c_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                       FROM {schema}.concept_relationship
                                                                                                                       WHERE relationship_id = 'Maps to') AS alias1
                                                                           ON concept_id = concept_id_1)) as i
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
               ON con1.condition_concept_id = concept_id)
        GROUP BY person_id) AS a JOIN (SELECT person_id, MIN(condition_start_date) AS min_start_date
                                        FROM ({schema}.condition_occurrence AS con2 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                        FROM (SELECT *
                                                                                              FROM (SELECT concept_id_2
                                                                                                    FROM ((SELECT concept_id
                                                                                                           FROM {schema}.concept
                                                                                                           WHERE vocabulary_id = %(v_id2)s
                                                                                                             AND (concept_code = %(c_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                          FROM {schema}.concept_relationship
                                                                                                                                                          WHERE relationship_id = 'Maps to') AS alias2
                                                                                                          ON concept_id = concept_id_1)) as ccia2ci2
                                                                                                       JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                                                 JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                                              ON con2.condition_concept_id = concept_id)
                                        GROUP BY person_id) AS b
        ON a.person_id = b.person_id) JOIN (SELECT person_id, MIN(condition_start_date) AS min_start_date
                                            FROM ({schema}.condition_occurrence AS con3 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                            FROM (SELECT *
                                                                                                  FROM (SELECT concept_id_2
                                                                                                        FROM ((SELECT concept_id
                                                                                                               FROM {schema}.concept
                                                                                                               WHERE vocabulary_id = %(v_id3)s
                                                                                                                 AND (concept_code = %(c_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                           FROM {schema}.concept_relationship
                                                                                                                                                           WHERE relationship_id = 'Maps to') AS alias3
                                                                                                              ON concept_id = concept_id_1)) as ccia3ci2
                                                                                                           JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia3ci2c*"
                                                                                                     JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia3ci2c*caci"
                                                  ON con3.condition_concept_id = concept_id)
                                            GROUP BY person_id) AS c
        ON b.person_id = c.person_id) JOIN (SELECT person_id, MIN(condition_start_date) AS min_start_date
                                           FROM ({schema}.condition_occurrence AS con4 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                           FROM (SELECT *
                                                                                                 FROM (SELECT concept_id_2
                                                                                                       FROM ((SELECT concept_id
                                                                                                              FROM {schema}.concept
                                                                                                              WHERE vocabulary_id = %(v_id4)s
                                                                                                                AND (concept_code = %(c_id4)s)) as cci4 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                            FROM {schema}.concept_relationship
                                                                                                                                                            WHERE relationship_id = 'Maps to') AS alias4
                                                                                                             ON concept_id = concept_id_1)) as ccia4ci2
                                                                                                          JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia4ci2c*"
                                                                                                    JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia4ci2c*caci"
                                                 ON con4.condition_concept_id = concept_id)
                                           GROUP BY person_id) AS d ON c.person_id = d.person_id)
        WHERE CAST(EXTRACT(epoch FROM
                   CAST(LEAST(a.min_start_date, b.min_start_date, c.min_start_date, d.min_start_date) AS TIMESTAMP) -
                   CAST(GREATEST(a.min_start_date, b.min_start_date, c.min_start_date,
                                 d.min_start_date) AS TIMESTAMP)) / 86400 AS BIGINT) < %(days)s;
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "v_id3": v_id3, "c_id3": c_id3, "v_id4": v_id4, "c_id4": c_id4, "days": days}
    
    def patients_conditions_and(self, v_id1, v_id2, v_id3, v_id4, c_id1, c_id2, c_id3, c_id4):
        """
        Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, <ARG-CONDITION><2> and <ARG-CONDITION><3>.
        """

        sql = """
        SELECT COUNT(DISTINCT con1.person_id)
        FROM (((({schema}.condition_occurrence AS con1 JOIN (SELECT descendant_concept_id AS concept_id
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
        ON con1.condition_concept_id = concept_id) JOIN ({schema}.condition_occurrence AS con2 JOIN (SELECT descendant_concept_id AS concept_id
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
                                                          ON con2.condition_concept_id = concept_id)
        ON con1.person_id = con2.person_id) JOIN ({schema}.condition_occurrence AS con3 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                            FROM (SELECT *
                                                                                                  FROM (SELECT concept_id_2
                                                                                                        FROM ((SELECT concept_id
                                                                                                               FROM {schema}.concept
                                                                                                               WHERE vocabulary_id = %(v_id3)s
                                                                                                                 AND (concept_code = %(c_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                           FROM {schema}.concept_relationship
                                                                                                                                                           WHERE relationship_id = 'Maps to') AS alias3
                                                                                                              ON concept_id = concept_id_1)) as ccia3ci2
                                                                                                           JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia3ci2c*"
                                                                                                     JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia3ci2c*caci"
                                                  ON con3.condition_concept_id = concept_id)
        ON con2.person_id = con3.person_id) JOIN ({schema}.condition_occurrence AS con4 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                           FROM (SELECT *
                                                                                                 FROM (SELECT concept_id_2
                                                                                                       FROM ((SELECT concept_id
                                                                                                              FROM {schema}.concept
                                                                                                              WHERE vocabulary_id = %(v_id4)s
                                                                                                                AND (concept_code = %(c_id4)s)) as cci4 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                            FROM {schema}.concept_relationship
                                                                                                                                                            WHERE relationship_id = 'Maps to') AS alias4
                                                                                                             ON concept_id = concept_id_1)) as ccia4ci2
                                                                                                          JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia4ci2c*"
                                                                                                    JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia4ci2c*caci"
                                                 ON con4.condition_concept_id = concept_id)
        ON con3.person_id = con4.person_id);
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "v_id3": v_id3, "c_id3": c_id3, "v_id4": v_id4, "c_id4": c_id4}
    
    def patients_conditions_or(self, v_id1, v_id2, v_id3, v_id4, c_id1, c_id2, c_id3, c_id4):
        """
        Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, <ARG-CONDITION><2> or <ARG-CONDITION><3>.
        """

        sql = """
        SELECT COUNT(DISTINCT con1.person_id)
        FROM ({schema}.condition_occurrence AS con1 JOIN ((((SELECT descendant_concept_id AS concept_id
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
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id)
                                                  UNION
                                                  (SELECT descendant_concept_id AS concept_id
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
                                                            JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id))
                                                 UNION
                                                 (SELECT descendant_concept_id AS concept_id
                                                  FROM (SELECT *
                                                        FROM (SELECT concept_id_2
                                                              FROM ((SELECT concept_id
                                                                     FROM {schema}.concept
                                                                     WHERE vocabulary_id = %(v_id3)s
                                                                       AND (concept_code = %(c_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
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
                                                                      AND (concept_code = %(c_id4)s)) as cci4 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                  FROM {schema}.concept_relationship
                                                                                                                  WHERE relationship_id = 'Maps to') AS alias4
                                                                   ON concept_id = concept_id_1)) as ccia4ci2
                                                                JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia4ci2c*"
                                                          JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id)) as "ccia1ci2c*caciccia2ci2c*caciccia3ci2c*caciccia4ci2c*caci"
        ON con1.condition_concept_id = concept_id);
        """.replace("{schema}", self.schema)
        
        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "v_id3": v_id3, "c_id3": c_id3, "v_id4": v_id4, "c_id4": c_id4}
    
    def patients_conditions_and_time(self, v_id1, v_id2, v_id3, c_id1, c_id2, c_id3, days):
        """
        Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, and <ARG-CONDITION><2> within <ARG-TIMEDAYS><0> days.
        """

        sql = """
        SELECT COUNT(DISTINCT a.person_id)
        FROM (((SELECT person_id, MIN(condition_start_date) AS min_start_date
        FROM ({schema}.condition_occurrence AS con1 JOIN (SELECT descendant_concept_id AS concept_id
                                                        FROM (SELECT *
                                                              FROM (SELECT concept_id_2
                                                                    FROM ((SELECT concept_id
                                                                           FROM {schema}.concept
                                                                           WHERE vocabulary_id = %(v_id1)s
                                                                             AND (concept_code = %(c_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                      FROM {schema}.concept_relationship
                                                                                                                      WHERE relationship_id = 'Maps to') AS alias1
                                                                          ON concept_id = concept_id_1)) as i
                                                                       JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                                 JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
              ON con1.condition_concept_id = concept_id)
        GROUP BY person_id) AS a JOIN (SELECT person_id, MIN(condition_start_date) AS min_start_date
                                       FROM ({schema}.condition_occurrence AS con2 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                       FROM (SELECT *
                                                                                             FROM (SELECT concept_id_2
                                                                                                   FROM ((SELECT concept_id
                                                                                                          FROM {schema}.concept
                                                                                                          WHERE vocabulary_id = %(v_id2)s
                                                                                                            AND (concept_code = %(c_id2)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                         FROM {schema}.concept_relationship
                                                                                                                                                         WHERE relationship_id = 'Maps to') AS alias2
                                                                                                         ON concept_id = concept_id_1)) as ccia2ci2
                                                                                                      JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                                                JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
                                             ON con2.condition_concept_id = concept_id)
                                       GROUP BY person_id) AS b
        ON a.person_id = b.person_id) JOIN (SELECT person_id, MIN(condition_start_date) AS min_start_date
                                           FROM ({schema}.condition_occurrence AS con3 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                           FROM (SELECT *
                                                                                                 FROM (SELECT concept_id_2
                                                                                                       FROM ((SELECT concept_id
                                                                                                              FROM {schema}.concept
                                                                                                              WHERE vocabulary_id = %(v_id3)s
                                                                                                                AND (concept_code = %(c_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                          FROM {schema}.concept_relationship
                                                                                                                                                          WHERE relationship_id = 'Maps to') AS alias3
                                                                                                             ON concept_id = concept_id_1)) as ccia3ci2
                                                                                                          JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia3ci2c*"
                                                                                                    JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia3ci2c*caci"
                                                 ON con3.condition_concept_id = concept_id)
                                           GROUP BY person_id) AS c ON b.person_id = c.person_id)
        WHERE CAST(EXTRACT(epoch FROM CAST(LEAST(a.min_start_date, b.min_start_date, c.min_start_date) AS TIMESTAMP) -
                              CAST(GREATEST(a.min_start_date, b.min_start_date, c.min_start_date) AS TIMESTAMP)) /
        86400 AS BIGINT) < %(days)s;
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "v_id3": v_id3, "c_id3": c_id3, "days": days}
    
    def patients_conditions_and(self, v_id1, v_id2, c_id1, c_id2):
        """
        Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, and <ARG-CONDITION><2>.
        """

        sql = """
        SELECT COUNT(DISTINCT con1.person_id)
        FROM ((({schema}.condition_occurrence AS con1 JOIN (SELECT descendant_concept_id AS concept_id
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
        ON con1.condition_concept_id = concept_id) JOIN ({schema}.condition_occurrence AS con2 JOIN (SELECT descendant_concept_id AS concept_id
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
                                                         ON con2.condition_concept_id = concept_id)
        ON con1.person_id = con2.person_id) JOIN ({schema}.condition_occurrence AS con3 JOIN (SELECT descendant_concept_id AS concept_id
                                                                                           FROM (SELECT *
                                                                                                 FROM (SELECT concept_id_2
                                                                                                       FROM ((SELECT concept_id
                                                                                                              FROM {schema}.concept
                                                                                                              WHERE vocabulary_id = %(v_id3)s
                                                                                                                AND (concept_code = %(c_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                                                          FROM {schema}.concept_relationship
                                                                                                                                                          WHERE relationship_id = 'Maps to') AS alias3
                                                                                                             ON concept_id = concept_id_1)) as ccia3ci2
                                                                                                          JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia3ci2c*"
                                                                                                    JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia3ci2c*caci"
                                                 ON con3.condition_concept_id = concept_id)
        ON con2.person_id = con3.person_id);
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2}
    
    def patients_conditions_or(self, v_id1, v_id2, v_id3, c_id1, c_id2, c_id3):
        """
        Counts of patients with condition <ARG-CONDITION><0>, <ARG-CONDITION><1>, or <ARG-CONDITION><2>.
        """

        sql = """
        SELECT COUNT(DISTINCT con1.person_id)
        FROM ({schema}.condition_occurrence AS con1 JOIN (((SELECT descendant_concept_id AS concept_id
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
                                                           JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id)
                                                 UNION
                                                 (SELECT descendant_concept_id AS concept_id
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
                                                           JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id))
                                                UNION
                                                (SELECT descendant_concept_id AS concept_id
                                                 FROM (SELECT *
                                                       FROM (SELECT concept_id_2
                                                             FROM ((SELECT concept_id
                                                                    FROM {schema}.concept
                                                                    WHERE vocabulary_id = %(v_id3)s
                                                                      AND (concept_code = %(c_id3)s)) as cci3 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                FROM {schema}.concept_relationship
                                                                                                                WHERE relationship_id = 'Maps to') AS alias3
                                                                   ON concept_id = concept_id_1)) as ccia3ci2
                                                                JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia3ci2c*"
                                                          JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id)) as "ccia1ci2c*caciccia2ci2c*caciccia3ci2c*caci"
        ON con1.condition_concept_id = concept_id);
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "v_id3": v_id3, "c_id3": c_id3}
    
    def patients_distribution_by_birth(self):
        desc = f"""
            Distribution of patients by year of birth.
            """

        sql = """
        SELECT year_of_birth, COUNT(DISTINCT pe1.person_id)
        FROM {schema}.person AS pe1
        GROUP BY year_of_birth;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_condition_followed_condition(self, v_id1, v_id2, c_id1, c_id2):
        """
        How many people have Condition <ARG-CONDITION><0> followed by Condition <ARG-CONDITION><1>?
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
                                                                              AND (concept_code = %(c_id2)s')) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                           FROM {schema}.concept_relationship
                                                                                                                           WHERE relationship_id = 'Maps to') AS alias2
                                                                           ON concept_id = concept_id_1)) as i
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
        ON con2.condition_concept_id = concept_id) ON con1.person_id = con2.person_id AND CAST(EXTRACT(epoch FROM
                                                                                                              CAST(con1.condition_start_date AS TIMESTAMP) -
                                                                                                              CAST(con2.condition_start_date AS TIMESTAMP)) / 86400 AS BIGINT) >= 0;
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
                                                                     AND (concept_code = %(c_id1)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                               FROM {schema}.concept_relationship
                                                                                                               WHERE relationship_id = 'Maps to') AS alias1
                                                                  ON concept_id = concept_id_1)) as i
                                                               JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                         JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
        ON con1.condition_concept_id = concept_id)
        JOIN ({schema}.condition_occurrence AS con2 JOIN (SELECT descendant_concept_id AS concept_id
                                                         FROM (SELECT *
                                                               FROM (SELECT concept_id_2
                                                                     FROM ((SELECT concept_id
                                                                            FROM {schema}.concept
                                                                            WHERE vocabulary_id = %(v_id2)s
                                                                              AND (concept_code = %(c_id2)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                          FROM {schema}.concept_relationship
                                                                                                                          WHERE relationship_id = 'Maps to') AS alias2
                                                                           ON concept_id = concept_id_1)) as c
                                                                        JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                                  JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
        ON con2.condition_concept_id = concept_id) ON con1.person_id = con2.person_id AND CAST(EXTRACT(epoch FROM
                                                                                                              CAST(con1.condition_start_date AS TIMESTAMP) -
                                                                                                              CAST(con2.condition_start_date AS TIMESTAMP)) / 86400 AS BIGINT) >= %(days)s;
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "v_id2": v_id2, "c_id2": c_id2, "days": days}
    
    def patients_condition_age(self, v_id1, c_id1, age):
        """
        How many people have condition <ARG-CONDITION><0> at age <ARG-AGE><0>?
        """

        sql = """
        SELECT COUNT(DISTINCT pe1.person_id)
        FROM (({schema}.person AS pe1 JOIN {schema}.condition_occurrence AS con1
        ON pe1.person_id = con1.person_id) JOIN (SELECT descendant_concept_id AS concept_id
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
        WHERE EXTRACT(YEAR FROM condition_start_date) - year_of_birth = %(age)s;
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "age": age}
    
    def patients_condition_race(self, v_id1, c_id1, race):
        """
        How many people have condition <ARG-CONDITION><0> in the cohort of race <ARG-RACE><0>?
        """

        sql = """
        SELECT COUNT(pe1.person_id)
        FROM (({schema}.person AS pe1 JOIN (SELECT concept_id
                                  FROM {schema}.concept
                                  WHERE concept_name = %(race)s
                                    AND domain_id = 'Race'
                                    AND standard_concept = 'S') AS alias1
        ON pe1.race_concept_id = concept_id) JOIN ({schema}.condition_occurrence AS con1 JOIN (SELECT descendant_concept_id AS concept_id
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
                                                  ON condition_concept_id = concept_id)
        ON pe1.person_id = con1.person_id);
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "race": race}
    
    def patients_condition_state(self, v_id1, c_id1, state):
        """
        How many people have condition <ARG-CONDITION><0> in the state <ARG-STATE><0>?
        """

        sql = """
        SELECT COUNT(DISTINCT pe1.person_id) AS number_of_patients
        FROM {schema}.person AS pe1
        JOIN {schema}.condition_occurrence AS con1 ON pe1.person_id = con1.person_id
        JOIN (SELECT location_id FROM {schema}.location WHERE state = %(state)s) AS loc_temp1
              ON pe1.location_id = loc_temp1.location_id
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
        ON con1.condition_concept_id = concept_id;
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
                                                             AND (concept_code = %(d_id1)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                           FROM {schema}.concept_relationship
                                                                                                           WHERE relationship_id = 'Maps to') AS alias1
                                                          ON concept_id = concept_id_1)) as i
                                                       JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                 JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
        ON drug_concept_id = concept_id)
        JOIN ({schema}.drug_exposure AS dr2 JOIN (SELECT descendant_concept_id AS concept_id
                                                 FROM (SELECT *
                                                       FROM (SELECT concept_id_2
                                                             FROM ((SELECT concept_id
                                                                    FROM {schema}.concept
                                                                    WHERE vocabulary_id = %(v_id2)s
                                                                      AND (concept_code = %(d_id2)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                FROM {schema}.concept_relationship
                                                                                                                WHERE relationship_id = 'Maps to') AS alias2
                                                                   ON concept_id = concept_id_1)) as c
                                                                JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                          JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
        ON dr2.drug_concept_id = concept_id) ON dr1.person_id = dr2.person_id AND CAST(EXTRACT(epoch FROM
                                                                                                      CAST(dr1.drug_exposure_start_date AS TIMESTAMP) -
                                                                                                      CAST(dr2.drug_exposure_start_date AS TIMESTAMP)) /
                                                                                              86400 AS BIGINT) >= %(days)s;
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
                                                             AND (concept_code = %(d_id1)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                           FROM {schema}.concept_relationship
                                                                                                           WHERE relationship_id = 'Maps to') AS alias1
                                                          ON concept_id = concept_id_1)) as i
                                                       JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                 JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
        ON dr1.drug_concept_id = concept_id)
        JOIN ({schema}.drug_exposure AS dr2 JOIN (SELECT descendant_concept_id AS concept_id
                                                 FROM (SELECT *
                                                       FROM (SELECT concept_id_2
                                                             FROM ((SELECT concept_id
                                                                    FROM {schema}.concept
                                                                    WHERE vocabulary_id = %(v_id2)s
                                                                      AND (concept_code = %(d_id2)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                FROM {schema}.concept_relationship
                                                                                                                WHERE relationship_id = 'Maps to') AS alias2
                                                                   ON concept_id = concept_id_1)) as c
                                                                JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia2ci2c*"
                                                          JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia2ci2c*caci"
        ON dr2.drug_concept_id = concept_id) ON dr1.person_id = dr2.person_id AND CAST(EXTRACT(epoch FROM
                                                                                                      CAST(dr1.drug_exposure_start_date AS TIMESTAMP) -
                                                                                                      CAST(dr2.drug_exposure_start_date AS TIMESTAMP)) /
                                                                                              86400 AS BIGINT) >= 0;
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
    
    def patients_drug_year(self, v_id1, c_id1, year):
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
                                                             AND (concept_code = %(c_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                          FROM {schema}.concept_relationship
                                                                                                          WHERE relationship_id = 'Maps to') AS alias1
                                                          ON concept_id = concept_id_1)) as c
                                                       JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                 JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
        ON dr1.drug_concept_id = concept_id)
        WHERE EXTRACT(YEAR FROM drug_exposure_start_date) <= %(year)s
        AND EXTRACT(YEAR FROM drug_exposure_end_date) >= %(year)s;
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1, "year": year}
    
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
                                                                     AND (concept_code = %(c_id1)s)) as cci2 JOIN (SELECT concept_id_1, concept_id_2
                                                                                                               FROM {schema}.concept_relationship
                                                                                                               WHERE relationship_id = 'Maps to') AS alias1
                                                                  ON concept_id = concept_id_1)) as i
                                                               JOIN {schema}.concept ON concept_id_2 = concept_id) as "ccia1ci2c*"
                                                         JOIN {schema}.concept_ancestor ON concept_id = ancestor_concept_id) as "ccia1ci2c*caci"
        ON con1.condition_concept_id = concept_id)
        JOIN ({schema}.drug_exposure AS dr1 JOIN (SELECT descendant_concept_id AS concept_id
                                                 FROM (SELECT *
                                                       FROM (SELECT concept_id_2
                                                             FROM ((SELECT concept_id
                                                                    FROM {schema}.concept
                                                                    WHERE vocabulary_id = %(v_id2)s
                                                                      AND (concept_code = %(d_id1)s)) as cci JOIN (SELECT concept_id_1, concept_id_2
                                                                                                                   FROM {schema}.concept_relationship
                                                                                                                   WHERE relationship_id = 'Maps to') AS alias2
                                                                   ON concept_id = concept_id_1)) as c
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
        FROM (({schema}.person JOIN (SELECT concept_id
                           FROM {schema}.concept
                           WHERE concept_name = %(gender)s
                             AND domain_id = 'Gender'
                             AND standard_concept = 'S') AS alias1
        ON gender_concept_id = concept_id) AS pe1 JOIN ({schema}.condition_occurrence JOIN (SELECT descendant_concept_id AS concept_id
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
                                                       ON condition_concept_id = concept_id) AS pe2
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
        desc = f"""
            Number of patients by gender and state.
            """

        sql = """
        SELECT gender, loc1.state, COUNT(pe1.person_id) AS number_of_patients
        FROM {schema}.person AS pe1
        JOIN (SELECT concept_id, concept_name AS gender
               FROM {schema}.concept
               WHERE domain_id = 'Gender'
                 AND standard_concept = 'S') AS alias1 ON pe1.gender_concept_id = concept_id
        JOIN {schema}.location AS loc1 ON pe1.location_id = loc1.location_id
        GROUP BY gender, state;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_group_by_ethnicity_location(self):
        desc = f"""
            Number of patients grouped by ethnicity and residence state location.
            """

        sql = """
        SELECT ethnicity, state, COUNT(DISTINCT pe1.person_id)
        FROM (({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS ethnicity
                                  FROM {schema}.concept
                                  WHERE domain_id = 'Ethnicity'
                                    AND standard_concept = 'S') AS alias1
        ON pe1.ethnicity_concept_id = concept_id) JOIN (SELECT location_id, state FROM {schema}.location) AS state_temp1
        ON pe1.location_id = state_temp1.location_id)
        GROUP BY ethnicity, state;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_group_by_ethnicity_birth(self):
        desc = f"""
            Number of patients grouped by ethnicity and year of birth.
            """

        sql = """
        SELECT ethnicity, year_of_birth, COUNT(DISTINCT pe1.person_id)
        FROM ({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS ethnicity
                                 FROM {schema}.concept
                                 WHERE domain_id = 'Ethnicity'
                                   AND standard_concept = 'S') AS alias1
        ON pe1.ethnicity_concept_id = concept_id)
        GROUP BY ethnicity, year_of_birth;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_group_by_ethnicity(self):
        desc = f"""
            Number of patients grouped by ethnicity.
            """

        sql = """
        SELECT ethnicity, COUNT(DISTINCT pe1.person_id)
        FROM {schema}.person AS pe1
        JOIN (SELECT concept_id, concept_name AS ethnicity
               FROM {schema}.concept
               WHERE domain_id = 'Ethnicity'
                 AND standard_concept = 'S') AS alias1
              ON pe1.ethnicity_concept_id = concept_id
        GROUP BY ethnicity;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_group_by_gender(self):
        desc = f"""
            Number of patients grouped by gender.
            """

        sql = """
        SELECT gender, COUNT(DISTINCT pe1.person_id)
        FROM ({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS gender
                                 FROM {schema}.concept
                                 WHERE domain_id = 'Gender'
                                   AND standard_concept = 'S') AS alias1
        ON pe1.gender_concept_id = concept_id)
        GROUP BY gender;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_group_by_race_ethnicity(self):
        desc = f"""
            Number of patients grouped by race and ethnicity.
            """

        sql = """
        SELECT race, ethnicity, COUNT(DISTINCT pe1.person_id)
        FROM (({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS race
                                  FROM {schema}.concept
                                  WHERE domain_id = 'Race'
                                    AND standard_concept = 'S') AS alias1
        ON pe1.race_concept_id = concept_id) JOIN (SELECT concept_id, concept_name AS ethnicity
                                                  FROM {schema}.concept
                                                  WHERE domain_id = 'Ethnicity'
                                                    AND standard_concept = 'S') AS eth_temp1
        ON pe1.ethnicity_concept_id = eth_temp1.concept_id)
        GROUP BY race, ethnicity;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_grouped_by_race_gender(self):
        desc = f"""
            Number of patients grouped by race and gender.
            """

        sql = """
        SELECT race, gender, COUNT(DISTINCT pe1.person_id)
        FROM (({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS race
                                  FROM {schema}.concept
                                  WHERE domain_id = 'Race'
                                    AND standard_concept = 'S') AS alias1
        ON pe1.race_concept_id = concept_id) JOIN (SELECT concept_id, concept_name AS gender
                                                  FROM {schema}.concept
                                                  WHERE domain_id = 'Gender'
                                                    AND standard_concept = 'S') AS gen_temp1
        ON pe1.gender_concept_id = gen_temp1.concept_id)
        GROUP BY race, gender;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_group_by_race_location(self):
        desc = f"""
            Number of patients grouped by race and residence state location.
            """

        sql = """
        SELECT race, state, COUNT(DISTINCT pe1.person_id)
        FROM (({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS race
                                  FROM {schema}.concept
                                  WHERE domain_id = 'Race'
                                    AND standard_concept = 'S') AS alias1
        ON pe1.race_concept_id = concept_id) JOIN (SELECT location_id, state FROM {schema}.location) AS state_temp1
        ON pe1.location_id = state_temp1.location_id)
        GROUP BY race, state;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_group_by_race_birth(self):
        desc = f"""
            Number of patients grouped by race and year of birth.
            """

        sql = """
        SELECT race, year_of_birth, COUNT(DISTINCT pe1.person_id)
        FROM ({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS race
                                 FROM {schema}.concept
                                 WHERE domain_id = 'Race'
                                   AND standard_concept = 'S') AS alias1
        ON pe1.race_concept_id = concept_id)
        GROUP BY race, year_of_birth;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_group_by_location(self):
        desc = f"""
            Number of patients grouped by residence state location.
            """

        sql = """
        SELECT state, COUNT(DISTINCT pe1.person_id)
        FROM {schema}.person AS pe1
        JOIN (SELECT location_id, state FROM {schema}.location) AS state_temp1
              ON pe1.location_id = state_temp1.location_id
        GROUP BY state;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_group_by_birth_gender(self):
        desc = f"""
            Number of patients grouped by year of birth and gender.
            """

        sql = """
        SELECT year_of_birth, gender, COUNT(DISTINCT pe1.person_id)
        FROM ({schema}.person AS pe1 JOIN (SELECT concept_id, concept_name AS gender
                                 FROM {schema}.concept
                                 WHERE domain_id = 'Gender'
                                   AND standard_concept = 'S') AS alias1
        ON pe1.gender_concept_id = concept_id)
        GROUP BY year_of_birth, gender;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_group_by_birth_location(self):
        desc = f"""
            Number of patients grouped by year of birth and residence state location.
            """

        sql = """
        SELECT year_of_birth, state, COUNT(DISTINCT pe1.person_id)
        FROM ({schema}.person AS pe1 JOIN (SELECT location_id, state FROM {schema}.location) AS state_temp1
        ON pe1.location_id = state_temp1.location_id)
        GROUP BY year_of_birth, state;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
    def patients_count(self):
        desc = f"""
            Number of patients in the dataset.
            """

        sql = """
        SELECT COUNT(DISTINCT pe1.person_id)
        FROM {schema}.person AS pe1;
        """.replace("{schema}", self.schema)

        return sql, desc, {}
    
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
               WHERE concept_name = %(race)s
                 AND domain_id = 'Race'
                 AND standard_concept = 'S') AS alias1
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
               WHERE concept_name = %(gender)s
                 AND domain_id = 'Gender'
                 AND standard_concept = 'S') AS alias1
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
        SELECT EXTRACT(YEAR FROM condition_start_date) AS year, COUNT(DISTINCT con1.person_id)
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
        GROUP BY EXTRACT(YEAR FROM condition_start_date);
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "c_id1": c_id1}
    
    def patients_drug_group_by_year(self, v_id1, d_id1):
        """
        counts of patients with taking drug <ARG-DRUG><0> grouped by year of prescription.
        """

        sql = """
        SELECT EXTRACT(YEAR FROM drug_exposure_start_date), COUNT(DISTINCT dr1.person_id)
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
        GROUP BY EXTRACT(YEAR FROM drug_exposure_start_date);
        """.replace("{schema}", self.schema)

        return sql, {"v_id1": v_id1, "d_id1": d_id1}