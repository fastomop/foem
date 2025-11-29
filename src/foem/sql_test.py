from .config import get_db_connection
from .transpiler import transpile_query
from typing import Dict
from contextlib import contextmanager
from pathlib import Path
from sqlalchemy import text as sql_text
import re
import os

class SqlTest:

    def __init__(self, conn=None, result_limit=1):
        self.conn = conn or get_db_connection()
        self.dataset_dir = Path(__file__).parent.parent.parent / "dataset"
        self.vocab_dict = self.__build_vocab_dict()
        self._id = 1
        self.result_limit = result_limit

        # Mapping from template method names to SQL file numbers
        self.template_map = {
            "patients_group_by_gender_and_ethn": "F0001",
            "patients_group_by_race": "F0002",
            "patients_2drugs_and_time": "F0003",
            "patients_2drugs_and": "F0004",
            "patients_2drugs_or": "F0005",
            "patients_4drugs_and_time": "F0006",
            "patients_4drugs_and": "F0007",
            "patients_4drugs_or": "F0008",
            "patients_3drugs_and_time": "F0009",
            "patients_3drugs_and": "F0010",
            "patients_3drugs_or": "F0011",
            "patients_2conditions_and_time": "F0012",
            "patients_2conditions_and": "F0013",
            "patients_2conditions_or": "F0014",
            "patients_4conditions_and_time": "F0015",
            "patients_4conditions_and": "F0016",
            "patients_4conditions_or": "F0017",
            "patients_3conditions_and_time": "F0018",
            "patients_3conditions_and": "F0019",
            "patients_3conditions_or": "F0020",
            "patients_distribution_by_birth": "F0021",
            "patients_condition_followed_condition": "F0022",
            "patients_condition_time_condition": "F0023",
            "patients_condition_age": "F0024",
            "patients_condition_race": "F0025",
            "patients_condition_state": "F0026",
            "patients_condition_year": "F0027",
            "patients_drug_time_drug": "F0028",
            "patients_drug_followed_drug": "F0029",
            "patients_condition_ethnicity": "F0030",
            "patients_drug_year": "F0031",
            "patients_drug_after_condition": "F0032",
            "patients_drug_time_after_condition": "F0033",
            "patients_gender_condition": "F0034",
            "patients_year": "F0035",
            "patients_gender_state": "F0036",
            "patients_group_by_ethnicity_location": "F0037",
            "patients_group_by_ethnicity_birth": "F0038",
            "patients_group_by_ethnicity": "F0039",
            "patients_group_by_gender": "F0040",
            "patients_group_by_race_ethnicity": "F0041",
            "patients_grouped_by_race_gender": "F0042",
            "patients_group_by_race_location": "F0043",
            "patients_group_by_race_birth": "F0044",
            "patients_group_by_location": "F0045",
            "patients_group_by_birth_gender": "F0046",
            "patients_group_by_birth_location": "F0047",
            "patients_count": "F0048",
            "patients_count_by_ethnicity": "F0049",
            "patients_count_by_race": "F0050",
            "patients_count_by_gender": "F0051",
            "patients_drug": "F0052",
            "patients_condition": "F0053",
            "patients_count_by_location": "F0054",
            "patients_condition_group_by_year": "F0055",
            "patients_drug_group_by_year": "F0056",
        }
    
    def close(self) -> None:
        """Close the database connection."""
        try:
            if self.conn:
                self.conn.close()
        finally:
            self.conn = None

    def _read_template(self, method_name: str):
        """Read SQL template from dataset folder based on method name."""
        file_id = self.template_map.get(method_name)
        if not file_id:
            raise ValueError(f"No template file found for method: {method_name}")

        file_path = self.dataset_dir / f"{file_id}.sql"
        if not file_path.exists():
            raise FileNotFoundError(f"Template file not found: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Extract the description (first comment line) and SQL
        lines = content.split('\n')
        description = ""
        sql_lines = []

        for line in lines:
            if line.strip().startswith('--'):
                if not description:
                    description = line.strip()[2:].strip()
            else:
                sql_lines.append(line)

        sql = '\n'.join(sql_lines).strip()
        return sql, description

    def _get_template_sql(self, method_name: str, params: dict = None):
        """Get SQL template and return it with parameters."""
        sql, description = self._read_template(method_name)
        return sql, params or {}

    def find_code_by_name(self, name: str, vocab_dict: dict):
        """
        Given a drug/concept name (case-insensitive) and a dictionary in the form:
            {"RxNorm": {"concept_code": "concept_name", ...}, ...}

        Return list of [(vocabulary_id, concept_code)] for ALL matches found.
        """
        results = []
        for vocab_id, codes in vocab_dict.items():
            for concept_code, concept_name in codes.items():
                if concept_name.lower() == name.lower():
                    results.append((vocab_id, concept_code))
        return results

    def _is_databricks(self) -> bool:
        """Check if the current database connection is Databricks."""
        db_type = os.getenv("DB_TYPE", "postgresql").lower()
        return db_type == "databricks"

    def _maybe_transpile(self, query: str) -> str:
        """Transpile query from PostgreSQL to Databricks if using Databricks."""
        if self._is_databricks():
            try:
                return transpile_query(query, source_dialect="postgres", target_dialect="databricks")
            except Exception as e:
                print(f"Warning: Failed to transpile query: {e}")
                return query
        return query

    @contextmanager
    def _cursor(self):
        """Context manager for database connection."""
        conn = self.conn.connect()
        try:
            yield conn
        finally:
            conn.close()

    def _run_query(self, query: str, params=None):
        """Helper to execute a query and fetch all results."""
        query = self._maybe_transpile(query)
        converted_query = re.sub(r'%\((\w+)\)s', r':\1', query)

        with self._cursor() as cur:
            if params:
                result = cur.execute(sql_text(converted_query), params)
            else:
                result = cur.execute(sql_text(converted_query))
            return result.fetchall()

    def _process_results(self, results, text_template, template_method_name, *args):
        """Helper to process results, format text, read template from file, and return output data."""
        import re
        from decimal import Decimal
        output = []
        for row in results:
            text = text_template.format(*row)
            ids = [self.find_code_by_name(val, self.vocab_dict) for val in row if isinstance(val, str)]
            ids_flat = [item for sublist in ids for item in sublist]

            params = {}
            if 'drug_after_condition' in template_method_name or 'drug_time_after_condition' in template_method_name:
                if len(ids_flat) >= 2:
                    params['v_id1'] = ids_flat[0][0]
                    params['c_id1'] = ids_flat[0][1]
                    params['v_id2'] = ids_flat[1][0]
                    params['d_id1'] = ids_flat[1][1]
            elif 'drugs' in template_method_name or 'drug' in template_method_name:
                for idx, (vocab_id, code_id) in enumerate(ids_flat, start=1):
                    params[f'v_id{idx}'] = vocab_id
                    params[f'd_id{idx}'] = code_id
            elif 'condition' in template_method_name:
                for idx, (vocab_id, code_id) in enumerate(ids_flat, start=1):
                    params[f'v_id{idx}'] = vocab_id
                    params[f'c_id{idx}'] = code_id

            if 'age' in template_method_name:
                age_values = [val for val in row if isinstance(val, (int, float, Decimal))]
                if age_values:
                    params['age'] = int(age_values[-1])
                elif args:
                    params['age'] = args[0]
            elif 'year' in template_method_name:
                year_values = [val for val in row if isinstance(val, (int, float, Decimal))]
                if year_values:
                    params['year'] = int(year_values[-1])
                elif args:
                    params['year'] = args[0]
            elif args:
                if 'time' in template_method_name or 'days' in template_method_name:
                    params['days'] = args[0]

            string_values = [val for val in row if isinstance(val, str) and val not in [item for sublist in ids for item in sublist]]

            if 'race' in template_method_name:
                for val in row:
                    if isinstance(val, str) and not self.find_code_by_name(val, self.vocab_dict):
                        params['race'] = val
                        break

            if 'ethnicity' in template_method_name:
                for val in row:
                    if isinstance(val, str) and not self.find_code_by_name(val, self.vocab_dict):
                        params['ethnicity'] = val
                        break

            if 'gender' in template_method_name:
                for val in row:
                    if isinstance(val, str) and not self.find_code_by_name(val, self.vocab_dict):
                        params['gender'] = val
                        break

            if 'state' in template_method_name or 'location' in template_method_name:
                for val in row:
                    if isinstance(val, str) and not self.find_code_by_name(val, self.vocab_dict):
                        params['state'] = val
                        params['location'] = val
                        break

            query, _ = self._read_template(template_method_name)
            query = self._maybe_transpile(query)
            sql_raw = self.__finalise_sql(query, params, self.conn)
            sql = re.sub(r'\s+', ' ', sql_raw).strip()

            converted_query = re.sub(r'%\((\w+)\)s', r':\1', query)
            with self._cursor() as cur:
                result = cur.execute(sql_text(converted_query), params)
                query_result = result.fetchall()
            output.append({
                "id": self._id,
                "input": text,
                "expected_output": sql,
                "execution_result": query_result
            })
            self._id += 1
        return output

    def _add_result(self, text, query, params):
        """
        Centralized method to execute query and add result to output.

        Args:
            text: The description text
            query: SQL query (or template name if params provided)
            params: Parameters for the query template

        Returns:
            List with a single result dict
        """
        query = self._maybe_transpile(query)
        sql_raw = self.__finalise_sql(query, params, self.conn)
        sql = re.sub(r'\s+', ' ', sql_raw).strip()
        converted_query = re.sub(r'%\((\w+)\)s', r':\1', query)

        with self._cursor() as cur:
            result = cur.execute(sql_text(converted_query), params)
            query_result = result.fetchall()

        # Create result dict
        result = {
            "id": self._id,
            "input": text,
            "expected_output": sql,
            "execution_result": query_result
        }

        self._id += 1
        return [result]

    def __build_vocab_dict(self) -> dict:
        """
        Build a nested dict:
            {
              "RxNorm": {"1154343": "Hydrochlorothiazide 25 MG Oral Tablet", ...},
              "ATC": {"C10AA05": "Atorvastatin", ...},
              ...
            }
        Only includes STANDARD concepts (standard_concept = 'S').
        """

        vocabularies = {
            "Drug": ["RxNorm", "ATC", "SPL"],
            "Condition": ["SNOMED", "ICD10CM"],
        }

        vocab_dict: Dict[str, Dict[str, str]] = {}

        with self._cursor() as cur:
            query = sql_text(
                "SELECT concept_code, concept_name "
                "FROM concept "
                "WHERE vocabulary_id = :vocab "
                "  AND domain_id = :domain "
                "  AND standard_concept = 'S'"
            )

            for domain, vocab_list in vocabularies.items():
                for vocab in vocab_list:
                    result = cur.execute(query, {"vocab": vocab, "domain": domain})
                    rows = result.fetchall()
                    vocab_dict[vocab] = {code: name for code, name in rows}

        return vocab_dict
    
    def __finalise_sql(self, sql: str, params: dict, conn) -> str:
        """
        Compile SQL query with parameters for display purposes.
        This converts parameterized queries into their final form.
        """
        # Convert %(param)s format to :param format for SQLAlchemy
        converted_sql = re.sub(r'%\((\w+)\)s', r':\1', sql)

        compiled = sql_text(converted_sql).bindparams(**params)
        compiled_query = compiled.compile(
            dialect=self.conn.dialect,
            compile_kwargs={"literal_binds": True}
        )
        return str(compiled_query)

    def patients_2drugs_and_time(self):

        query = f"""
        WITH drug_pairs AS (
        SELECT
            de1.drug_concept_id AS drug1_concept_id,
            de2.drug_concept_id AS drug2_concept_id,
            c1.concept_name     AS drug1_name,
            c2.concept_name     AS drug2_name,
            COUNT(DISTINCT de1.person_id) AS co_prescription_count
        FROM drug_exposure de1
        JOIN drug_exposure de2
            ON de1.person_id = de2.person_id
        AND de1.drug_concept_id < de2.drug_concept_id
        JOIN concept c1 ON de1.drug_concept_id = c1.concept_id
        JOIN concept c2 ON de2.drug_concept_id = c2.concept_id
        WHERE
            ABS(de1.drug_exposure_start_date - de2.drug_exposure_start_date) <= 30
            AND c1.domain_id = 'Drug'
            AND c2.domain_id = 'Drug'
            AND c1.invalid_reason IS NULL
            AND c2.invalid_reason IS NULL
        GROUP BY
            de1.drug_concept_id,
            de2.drug_concept_id,
            c1.concept_name,
            c2.concept_name
        )
        SELECT
        drug1_name,
        drug2_name
        -- ,co_prescription_count AS person_count
        FROM drug_pairs
        ORDER BY co_prescription_count DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients taking drug {0} and {1} within 30 days.",
            "patients_2drugs_and_time",
            30
        )
    
    def patients_2drugs_and(self):
        
        query = f"""
        WITH drug_pairs AS (
        SELECT
            de1.drug_concept_id AS drug1_concept_id,
            de2.drug_concept_id AS drug2_concept_id,
            c1.concept_name     AS drug1_name,
            c2.concept_name     AS drug2_name,
            COUNT(DISTINCT de1.person_id) AS co_prescription_count
        FROM drug_exposure de1
        JOIN drug_exposure de2
            ON de1.person_id = de2.person_id
        AND de1.drug_concept_id < de2.drug_concept_id
        JOIN concept c1 ON de1.drug_concept_id = c1.concept_id
        JOIN concept c2 ON de2.drug_concept_id = c2.concept_id
        WHERE
            c1.domain_id = 'Drug'
            AND c2.domain_id = 'Drug'
            AND c1.invalid_reason IS NULL
            AND c2.invalid_reason IS NULL
        GROUP BY
            de1.drug_concept_id,
            de2.drug_concept_id,
            c1.concept_name,
            c2.concept_name
        )
        SELECT
        drug1_name,
        drug2_name
        FROM drug_pairs
        ORDER BY co_prescription_count DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients taking drug {0} and {1}.",
            "patients_2drugs_and"
        )

    def patients_2drugs_or(self):

        query = f"""
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
            de.drug_era_end_date   AS end_date
        FROM drug_era de
        JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        cooccur_pairs AS (
        SELECT
            e1.person_id,
            LEAST(e1.drug_concept_id, e2.drug_concept_id)  AS drug1,
            GREATEST(e1.drug_concept_id, e2.drug_concept_id) AS drug2
        FROM eras e1
        JOIN eras e2
            ON e1.person_id = e2.person_id
        AND e1.drug_concept_id < e2.drug_concept_id
        GROUP BY e1.person_id, LEAST(e1.drug_concept_id, e2.drug_concept_id),
                GREATEST(e1.drug_concept_id, e2.drug_concept_id)
        ),
        overlapping_pairs AS (
        SELECT DISTINCT
            a.person_id,
            LEAST(a.drug_concept_id, b.drug_concept_id)    AS drug1,
            GREATEST(a.drug_concept_id, b.drug_concept_id) AS drug2
        FROM eras a
        JOIN eras b
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
        )
        SELECT
        c1.concept_name AS drug1_name,
        c2.concept_name AS drug2_name
        FROM separate_pairs sp
        JOIN concept c1 ON c1.concept_id = sp.drug1
        JOIN concept c2 ON c2.concept_id = sp.drug2
        GROUP BY sp.drug1, c1.concept_name, sp.drug2, c2.concept_name
        LIMIT {self.result_limit};
        """

        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients taking drug {0} or {1}.",
            "patients_2drugs_or"
        )

    def patients_4drugs_and_time(self):

        query = f"""
        WITH valid_drugs AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Drug'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        exposures AS (
        SELECT DISTINCT
                de.person_id,
                de.drug_concept_id,
                de.drug_exposure_start_date::date AS start_date,
                COALESCE(de.drug_exposure_end_date::date, de.drug_exposure_start_date::date) AS end_date
        FROM drug_exposure de
        JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        quads AS (
        SELECT
            e1.drug_concept_id AS drug1_concept_id,
            e2.drug_concept_id AS drug2_concept_id,
            e3.drug_concept_id AS drug3_concept_id,
            e4.drug_concept_id AS drug4_concept_id,
            COUNT(DISTINCT e1.person_id) AS person_count
        FROM exposures e1
        JOIN exposures e2
            ON e2.person_id = e1.person_id
            AND e2.drug_concept_id > e1.drug_concept_id
            AND e2.start_date <= e1.end_date
            AND e2.end_date >= e1.start_date
        JOIN exposures e3
            ON e3.person_id = e1.person_id
            AND e3.drug_concept_id > e2.drug_concept_id
            AND e3.start_date <= e1.end_date
            AND e3.end_date >= e1.start_date
            AND e3.start_date <= e2.end_date
            AND e3.end_date >= e2.start_date
        JOIN exposures e4
            ON e4.person_id = e1.person_id
            AND e4.drug_concept_id > e3.drug_concept_id
            AND e4.start_date <= e1.end_date
            AND e4.end_date >= e1.start_date
            AND e4.start_date <= e2.end_date
            AND e4.end_date >= e2.start_date
            AND e4.start_date <= e3.end_date
            AND e4.end_date >= e3.start_date
        GROUP BY
            e1.drug_concept_id, e2.drug_concept_id, e3.drug_concept_id, e4.drug_concept_id
        )
        SELECT
        c1.concept_name AS drug1_name,
        c2.concept_name AS drug2_name,
        c3.concept_name AS drug3_name,
        c4.concept_name AS drug4_name
        -- ,q.person_count AS patients
        FROM quads q
        JOIN concept c1 ON c1.concept_id = q.drug1_concept_id
        JOIN concept c2 ON c2.concept_id = q.drug2_concept_id
        JOIN concept c3 ON c3.concept_id = q.drug3_concept_id
        JOIN concept c4 ON c4.concept_id = q.drug4_concept_id
        ORDER BY q.person_count DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients taking drug {0}, {1}, {2} and {3} within 30 days.",
            "patients_4drugs_and_time",
            30
        )

    def patients_4drugs_and(self):

        query = f"""
        WITH valid_drugs AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Drug'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        person_drugs AS (
        SELECT DISTINCT de.person_id, de.drug_concept_id
        FROM drug_exposure de
        JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        quads AS (
        SELECT
            d1.drug_concept_id AS drug1_concept_id,
            d2.drug_concept_id AS drug2_concept_id,
            d3.drug_concept_id AS drug3_concept_id,
            d4.drug_concept_id AS drug4_concept_id,
            COUNT(DISTINCT d1.person_id) AS person_count
        FROM person_drugs d1
        JOIN person_drugs d2
            ON d2.person_id = d1.person_id
        AND d2.drug_concept_id > d1.drug_concept_id
        JOIN person_drugs d3
            ON d3.person_id = d1.person_id
        AND d3.drug_concept_id > d2.drug_concept_id
        JOIN person_drugs d4
            ON d4.person_id = d1.person_id
        AND d4.drug_concept_id > d3.drug_concept_id
        GROUP BY d1.drug_concept_id, d2.drug_concept_id, d3.drug_concept_id, d4.drug_concept_id
        )
        SELECT
        c1.concept_name AS drug1_name,
        c2.concept_name AS drug2_name,
        c3.concept_name AS drug3_name,
        c4.concept_name AS drug4_name
        FROM quads q
        JOIN concept c1 ON c1.concept_id = q.drug1_concept_id
        JOIN concept c2 ON c2.concept_id = q.drug2_concept_id
        JOIN concept c3 ON c3.concept_id = q.drug3_concept_id
        JOIN concept c4 ON c4.concept_id = q.drug4_concept_id
        ORDER BY q.person_count DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients taking drug {0}, {1}, {2} and {3}.",
            "patients_4drugs_and"
        )

    def patients_4drugs_or(self):

        query = f"""
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
                de.drug_era_end_date   AS end_date
            FROM drug_era de
            JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        cooccur_quads AS (
            SELECT
                e1.person_id,
                e1.drug_concept_id AS drug1,
                e2.drug_concept_id AS drug2,
                e3.drug_concept_id AS drug3,
                e4.drug_concept_id AS drug4
            FROM eras e1
            JOIN eras e2
            ON e2.person_id = e1.person_id
            AND e2.drug_concept_id > e1.drug_concept_id
            JOIN eras e3
            ON e3.person_id = e1.person_id
            AND e3.drug_concept_id > e2.drug_concept_id
            JOIN eras e4
            ON e4.person_id = e1.person_id
            AND e4.drug_concept_id > e3.drug_concept_id
            GROUP BY e1.person_id, e1.drug_concept_id, e2.drug_concept_id, e3.drug_concept_id, e4.drug_concept_id
        ),
        overlapping_quads AS (
            SELECT DISTINCT
                a.person_id,
                a.drug_concept_id AS drug1,
                b.drug_concept_id AS drug2,
                c.drug_concept_id AS drug3,
                d.drug_concept_id AS drug4
            FROM eras a
            JOIN eras b
            ON b.person_id = a.person_id
            AND b.drug_concept_id > a.drug_concept_id
            JOIN eras c
            ON c.person_id = a.person_id
            AND c.drug_concept_id > b.drug_concept_id
            JOIN eras d
            ON d.person_id = a.person_id
            AND d.drug_concept_id > c.drug_concept_id
            WHERE GREATEST(a.start_date, b.start_date, c.start_date, d.start_date)
                <= LEAST(a.end_date, b.end_date, c.end_date, d.end_date)
        ),
        separate_quads AS (
            SELECT cq.person_id, cq.drug1, cq.drug2, cq.drug3, cq.drug4
            FROM cooccur_quads cq
            LEFT JOIN overlapping_quads oq
            ON oq.person_id = cq.person_id
            AND oq.drug1 = cq.drug1
            AND oq.drug2 = cq.drug2
            AND oq.drug3 = cq.drug3
            AND oq.drug4 = cq.drug4
            WHERE oq.person_id IS NULL
        )
        SELECT
            c1.concept_name AS drug1_name,
            c2.concept_name AS drug2_name,
            c3.concept_name AS drug3_name,
            c4.concept_name AS drug4_name
        FROM separate_quads sq
        JOIN concept c1 ON c1.concept_id = sq.drug1
        JOIN concept c2 ON c2.concept_id = sq.drug2
        JOIN concept c3 ON c3.concept_id = sq.drug3
        JOIN concept c4 ON c4.concept_id = sq.drug4
        GROUP BY sq.drug1, c1.concept_name, sq.drug2, c2.concept_name, sq.drug3, c3.concept_name, sq.drug4, c4.concept_name
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients taking drug {0}, {1}, {2} or {3}.",
            "patients_4drugs_or"
        )

    def patients_3drugs_and_time(self):

        query = f"""
        WITH valid_drugs AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Drug'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        exposures AS (
        SELECT DISTINCT
                de.person_id,
                de.drug_concept_id,
                de.drug_exposure_start_date::date AS start_date
        FROM drug_exposure de
        JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        triples AS (
        SELECT
            e1.drug_concept_id AS drug1_concept_id,
            e2.drug_concept_id AS drug2_concept_id,
            e3.drug_concept_id AS drug3_concept_id,
            COUNT(DISTINCT e1.person_id) AS person_count
        FROM exposures e1
        JOIN exposures e2
            ON e2.person_id = e1.person_id
        AND e2.drug_concept_id > e1.drug_concept_id
        JOIN exposures e3
            ON e3.person_id = e1.person_id
        AND e3.drug_concept_id > e2.drug_concept_id
        WHERE
            (GREATEST(e1.start_date, e2.start_date, e3.start_date)
            - LEAST(e1.start_date, e2.start_date, e3.start_date)) <= 30
        GROUP BY
            e1.drug_concept_id, e2.drug_concept_id, e3.drug_concept_id
        )
        SELECT
        c1.concept_name AS drug1_name,
        c2.concept_name AS drug2_name,
        c3.concept_name AS drug3_name
        -- ,t.person_count AS patients
        FROM triples t
        JOIN concept c1 ON c1.concept_id = t.drug1_concept_id
        JOIN concept c2 ON c2.concept_id = t.drug2_concept_id
        JOIN concept c3 ON c3.concept_id = t.drug3_concept_id
        ORDER BY t.person_count DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients taking drug {0}, {1} and {2} within 30 days.",
            "patients_3drugs_and_time",
            30
        )

    def patients_3drugs_and(self):

        query = f"""
        WITH valid_drugs AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Drug'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        person_drugs AS (  -- one row per person per drug (no dates needed)
        SELECT DISTINCT de.person_id, de.drug_concept_id
        FROM drug_exposure de
        JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        triples AS (
        SELECT
            d1.drug_concept_id AS drug1_concept_id,
            d2.drug_concept_id AS drug2_concept_id,
            d3.drug_concept_id AS drug3_concept_id,
            COUNT(DISTINCT d1.person_id) AS person_count
        FROM person_drugs d1
        JOIN person_drugs d2
            ON d2.person_id = d1.person_id
        AND d2.drug_concept_id > d1.drug_concept_id
        JOIN person_drugs d3
            ON d3.person_id = d1.person_id
        AND d3.drug_concept_id > d2.drug_concept_id
        GROUP BY d1.drug_concept_id, d2.drug_concept_id, d3.drug_concept_id
        )
        SELECT
        c1.concept_name AS drug1_name,
        c2.concept_name AS drug2_name,
        c3.concept_name AS drug3_name
        FROM triples t
        JOIN concept c1 ON c1.concept_id = t.drug1_concept_id
        JOIN concept c2 ON c2.concept_id = t.drug2_concept_id
        JOIN concept c3 ON c3.concept_id = t.drug3_concept_id
        ORDER BY t.person_count DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients taking drug {0}, {1} and {2}.",
            "patients_3drugs_and"
        )

    def patients_3drugs_or(self):

        query = f"""
        WITH valid_drugs AS (
        SELECT concept_id
        FROM concept
        WHERE domain_id = 'Drug' AND standard_concept = 'S' AND invalid_reason IS NULL
        ),
        eras AS (
        SELECT de.person_id,
                de.drug_concept_id,
                de.drug_era_start_date AS start_date,
                de.drug_era_end_date   AS end_date
        FROM drug_era de
        JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        cooccur_triples AS (
        SELECT e1.person_id,
                e1.drug_concept_id AS drug1,
                e2.drug_concept_id AS drug2,
                e3.drug_concept_id AS drug3
        FROM eras e1
        JOIN eras e2 ON e2.person_id = e1.person_id AND e2.drug_concept_id > e1.drug_concept_id
        JOIN eras e3 ON e3.person_id = e1.person_id AND e3.drug_concept_id > e2.drug_concept_id
        GROUP BY e1.person_id, e1.drug_concept_id, e2.drug_concept_id, e3.drug_concept_id
        ),
        overlapping_triples AS (
        SELECT DISTINCT
                a.person_id,
                a.drug_concept_id AS drug1,
                b.drug_concept_id AS drug2,
                c.drug_concept_id AS drug3
        FROM eras a
        JOIN eras b ON b.person_id = a.person_id AND b.drug_concept_id > a.drug_concept_id
        JOIN eras c ON c.person_id = a.person_id AND c.drug_concept_id > b.drug_concept_id
        WHERE GREATEST(a.start_date, b.start_date, c.start_date)
                <= LEAST(a.end_date, b.end_date, c.end_date)
        ),
        separate_triples AS (
        SELECT ct.person_id, ct.drug1, ct.drug2, ct.drug3
        FROM cooccur_triples ct
        LEFT JOIN overlapping_triples ot
            ON ot.person_id = ct.person_id
        AND ot.drug1 = ct.drug1 AND ot.drug2 = ct.drug2 AND ot.drug3 = ct.drug3
        WHERE ot.person_id IS NULL
        ),
        stats AS (
        SELECT drug1, drug2, drug3, COUNT(DISTINCT person_id) AS patient_count
        FROM separate_triples
        GROUP BY drug1, drug2, drug3
        )
        SELECT c1.concept_name AS drug1_name,
            c2.concept_name AS drug2_name,
            c3.concept_name AS drug3_name
        FROM stats s
        JOIN concept c1 ON c1.concept_id = s.drug1
        JOIN concept c2 ON c2.concept_id = s.drug2
        JOIN concept c3 ON c3.concept_id = s.drug3
        ORDER BY s.patient_count
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients taking drug {0}, {1} or {2}.",
            "patients_3drugs_or"
        )

    def patients_2conditions_and_time(self):

        query = f"""
        WITH valid_conditions AS (
            SELECT concept_id
            FROM concept
            WHERE domain_id = 'Condition'
                AND standard_concept = 'S'
                AND invalid_reason IS NULL
        ),
        occ AS (
        SELECT
            co.person_id,
            co.condition_concept_id,
            co.condition_start_date
        FROM condition_occurrence co
        JOIN valid_conditions vc
            ON vc.concept_id = co.condition_concept_id
        ),
        pairs_per_patient AS (
        -- make one row per patient per condition pair that’s within 30 days
        SELECT
            o1.person_id,
            LEAST(o1.condition_concept_id, o2.condition_concept_id) AS cond1_id,
            GREATEST(o1.condition_concept_id, o2.condition_concept_id) AS cond2_id
        FROM occ o1
        JOIN occ o2
            ON o1.person_id = o2.person_id
        AND o1.condition_concept_id <> o2.condition_concept_id
        AND ABS(o1.condition_start_date - o2.condition_start_date) <= 30
        ),
        distinct_patient_pairs AS (
        -- ensure each patient counts at most once per pair
        SELECT DISTINCT person_id, cond1_id, cond2_id
        FROM pairs_per_patient
        ),
        pair_counts AS (
        SELECT
            cond1_id, cond2_id,
            COUNT(DISTINCT person_id) AS patient_count
        FROM distinct_patient_pairs
        GROUP BY cond1_id, cond2_id
        )
        SELECT
        c1.concept_name AS condition1_name,
        c2.concept_name AS condition2_name
        FROM pair_counts pc
        JOIN concept c1 ON c1.concept_id = pc.cond1_id
        JOIN concept c2 ON c2.concept_id = pc.cond2_id
        ORDER BY pc.patient_count DESC, condition1_name, condition2_name
        LIMIT {self.result_limit};
        """
        query = self._maybe_transpile(query)
        result = None
        with self._cursor() as cur:

            query_result = cur.execute(sql_text(query))
            result = query_result.fetchall()

        return self._process_results(
            result,
            "Counts of patients with condition {0} and {1} within 30 days.",
            "patients_2conditions_and_time",
            30
        )

    def patients_2conditions_and(self):
        
        query = f"""
        WITH valid_conditions AS (
            SELECT concept_id
            FROM concept
            WHERE domain_id = 'Condition'
            AND standard_concept = 'S'
            AND invalid_reason IS NULL
        ),
        occ AS (
            SELECT
                co.person_id,
                co.condition_concept_id,
                co.condition_start_date
            FROM condition_occurrence co
            JOIN valid_conditions vc
            ON vc.concept_id = co.condition_concept_id
        ),
        pairs_per_patient AS (
            SELECT
                o1.person_id,
                LEAST(o1.condition_concept_id, o2.condition_concept_id) AS cond1_id,
                GREATEST(o1.condition_concept_id, o2.condition_concept_id) AS cond2_id
            FROM occ o1
            JOIN occ o2
            ON o1.person_id = o2.person_id
            AND o1.condition_concept_id <> o2.condition_concept_id
        ),
        distinct_patient_pairs AS (
            SELECT DISTINCT person_id, cond1_id, cond2_id
            FROM pairs_per_patient
        ),
        pair_counts AS (
            SELECT
                cond1_id, cond2_id,
                COUNT(DISTINCT person_id) AS patient_count
            FROM distinct_patient_pairs
            GROUP BY cond1_id, cond2_id
        )
        SELECT
            c1.concept_name AS condition1_name,
            c2.concept_name AS condition2_name
        FROM pair_counts pc
        JOIN concept c1 ON c1.concept_id = pc.cond1_id
        JOIN concept c2 ON c2.concept_id = pc.cond2_id
        ORDER BY pc.patient_count DESC, condition1_name, condition2_name
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients with condition {0} and {1}.",
            "patients_2conditions_and"
        )
    
    def patients_2conditions_or(self):

        query = f"""
        WITH valid_conditions AS (
            SELECT concept_id
            FROM concept
            WHERE domain_id = 'Condition'
            AND standard_concept = 'S'
            AND invalid_reason IS NULL
        ),
        eras AS (
            SELECT
                ce.person_id,
                ce.condition_concept_id,
                ce.condition_era_start_date AS start_date,
                ce.condition_era_end_date   AS end_date
            FROM condition_era ce
            JOIN valid_conditions vc ON vc.concept_id = ce.condition_concept_id
        ),
        cooccur_pairs AS (
            SELECT
                e1.person_id,
                LEAST(e1.condition_concept_id, e2.condition_concept_id)   AS cond1,
                GREATEST(e1.condition_concept_id, e2.condition_concept_id) AS cond2
            FROM eras e1
            JOIN eras e2
                ON e1.person_id = e2.person_id
            AND e1.condition_concept_id < e2.condition_concept_id
            GROUP BY e1.person_id,
                    LEAST(e1.condition_concept_id, e2.condition_concept_id),
                    GREATEST(e1.condition_concept_id, e2.condition_concept_id)
        ),
        overlapping_pairs AS (
            SELECT DISTINCT
                a.person_id,
                LEAST(a.condition_concept_id, b.condition_concept_id)    AS cond1,
                GREATEST(a.condition_concept_id, b.condition_concept_id) AS cond2
            FROM eras a
            JOIN eras b
                ON a.person_id = b.person_id
            AND a.condition_concept_id < b.condition_concept_id
            AND a.start_date <= b.end_date
            AND b.start_date <= a.end_date
        ),
        separate_pairs AS (
            SELECT c.person_id, c.cond1, c.cond2
            FROM cooccur_pairs c
            LEFT JOIN overlapping_pairs o
                ON o.person_id = c.person_id
            AND o.cond1 = c.cond1
            AND o.cond2 = c.cond2
            WHERE o.person_id IS NULL
        )
        SELECT
            c1.concept_name AS condition1_name,
            c2.concept_name AS condition2_name
        FROM separate_pairs sp
        JOIN concept c1 ON c1.concept_id = sp.cond1
        JOIN concept c2 ON c2.concept_id = sp.cond2
        GROUP BY sp.cond1, c1.concept_name, sp.cond2, c2.concept_name
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients with condition {0} or {1}.",
            "patients_2conditions_or"
        )

    def patients_4conditions_and_time(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        exposures AS (
        SELECT DISTINCT
            co.person_id,
            co.condition_concept_id,
            co.condition_start_date::date AS start_date
        FROM condition_occurrence co
        JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        ),
        quads AS (
        SELECT
            e1.condition_concept_id AS cond1_concept_id,
            e2.condition_concept_id AS cond2_concept_id,
            e3.condition_concept_id AS cond3_concept_id,
            e4.condition_concept_id AS cond4_concept_id,
            COUNT(DISTINCT e1.person_id) AS person_count
        FROM exposures e1
        JOIN exposures e2
            ON e2.person_id = e1.person_id
        AND e2.condition_concept_id > e1.condition_concept_id
        JOIN exposures e3
            ON e3.person_id = e1.person_id
        AND e3.condition_concept_id > e2.condition_concept_id
        JOIN exposures e4
            ON e4.person_id = e1.person_id
        AND e4.condition_concept_id > e3.condition_concept_id
        WHERE
            (GREATEST(e1.start_date, e2.start_date, e3.start_date, e4.start_date)
            - LEAST(e1.start_date, e2.start_date, e3.start_date, e4.start_date)) <= 1000
        GROUP BY
            e1.condition_concept_id, e2.condition_concept_id, e3.condition_concept_id, e4.condition_concept_id
        )
        SELECT
        c1.concept_name AS condition1_name,
        c2.concept_name AS condition2_name,
        c3.concept_name AS condition3_name,
        c4.concept_name AS condition4_name
        -- q.person_count   AS patient_count
        FROM quads q
        JOIN concept c1 ON c1.concept_id = q.cond1_concept_id
        JOIN concept c2 ON c2.concept_id = q.cond2_concept_id
        JOIN concept c3 ON c3.concept_id = q.cond3_concept_id
        JOIN concept c4 ON c4.concept_id = q.cond4_concept_id
        ORDER BY q.person_count DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients with condition {0}, {1}, {2} and {3} within 1000 days.",
            "patients_4conditions_and_time",
            1000
        )

    def patients_4conditions_and(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        exposures AS (
        SELECT DISTINCT
            co.person_id,
            co.condition_concept_id,
            co.condition_start_date::date AS start_date
        FROM condition_occurrence co
        JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        ),
        quads AS (
        SELECT
            e1.condition_concept_id AS cond1_concept_id,
            e2.condition_concept_id AS cond2_concept_id,
            e3.condition_concept_id AS cond3_concept_id,
            e4.condition_concept_id AS cond4_concept_id,
            COUNT(DISTINCT e1.person_id) AS person_count
        FROM exposures e1
        JOIN exposures e2
            ON e2.person_id = e1.person_id
        AND e2.condition_concept_id > e1.condition_concept_id
        JOIN exposures e3
            ON e3.person_id = e1.person_id
        AND e3.condition_concept_id > e2.condition_concept_id
        JOIN exposures e4
            ON e4.person_id = e1.person_id
        AND e4.condition_concept_id > e3.condition_concept_id
        GROUP BY
            e1.condition_concept_id, e2.condition_concept_id, e3.condition_concept_id, e4.condition_concept_id
        )
        SELECT
        c1.concept_name AS condition1_name,
        c2.concept_name AS condition2_name,
        c3.concept_name AS condition3_name,
        c4.concept_name AS condition4_name
        -- q.person_count   AS patient_count
        FROM quads q
        JOIN concept c1 ON c1.concept_id = q.cond1_concept_id
        JOIN concept c2 ON c2.concept_id = q.cond2_concept_id
        JOIN concept c3 ON c3.concept_id = q.cond3_concept_id
        JOIN concept c4 ON c4.concept_id = q.cond4_concept_id
        ORDER BY q.person_count DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients with condition {0}, {1}, {2} and {3}.",
            "patients_4conditions_and"
        )

    def patients_4conditions_or(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT concept_id
        FROM concept
        WHERE domain_id = 'Condition'
            AND standard_concept = 'S'
            AND invalid_reason IS NULL
        ),
        eras AS (
        SELECT
            ce.person_id,
            ce.condition_concept_id,
            ce.condition_era_start_date AS start_date,
            ce.condition_era_end_date   AS end_date
        FROM condition_era ce
        JOIN valid_conditions vc ON vc.concept_id = ce.condition_concept_id
        ),
        cooccur_quads AS (
        SELECT
            e1.person_id,
            e1.condition_concept_id AS cond1,
            e2.condition_concept_id AS cond2,
            e3.condition_concept_id AS cond3,
            e4.condition_concept_id AS cond4
        FROM eras e1
        JOIN eras e2
            ON e2.person_id = e1.person_id
        AND e2.condition_concept_id > e1.condition_concept_id
        JOIN eras e3
            ON e3.person_id = e1.person_id
        AND e3.condition_concept_id > e2.condition_concept_id
        JOIN eras e4
            ON e4.person_id = e1.person_id
        AND e4.condition_concept_id > e3.condition_concept_id
        GROUP BY e1.person_id, e1.condition_concept_id, e2.condition_concept_id, e3.condition_concept_id, e4.condition_concept_id
        ),
        overlapping_quads AS (
        SELECT DISTINCT
            a.person_id,
            a.condition_concept_id AS cond1,
            b.condition_concept_id AS cond2,
            c.condition_concept_id AS cond3,
            d.condition_concept_id AS cond4
        FROM eras a
        JOIN eras b
            ON b.person_id = a.person_id
        AND b.condition_concept_id > a.condition_concept_id
        JOIN eras c
            ON c.person_id = a.person_id
        AND c.condition_concept_id > b.condition_concept_id
        JOIN eras d
            ON d.person_id = a.person_id
        AND d.condition_concept_id > c.condition_concept_id
        WHERE GREATEST(a.start_date, b.start_date, c.start_date, d.start_date)
                <= LEAST(a.end_date, b.end_date, c.end_date, d.end_date)
        ),
        separate_quads AS (
        -- quads that co-occur for a person but NEVER overlap in time
        SELECT cq.person_id, cq.cond1, cq.cond2, cq.cond3, cq.cond4
        FROM cooccur_quads cq
        LEFT JOIN overlapping_quads oq
            ON oq.person_id = cq.person_id
        AND oq.cond1 = cq.cond1
        AND oq.cond2 = cq.cond2
        AND oq.cond3 = cq.cond3
        AND oq.cond4 = cq.cond4
        WHERE oq.person_id IS NULL
        )
        SELECT
        c1.concept_name AS condition1_name,
        c2.concept_name AS condition2_name,
        c3.concept_name AS condition3_name,
        c4.concept_name AS condition4_name
        FROM separate_quads sq
        JOIN concept c1 ON c1.concept_id = sq.cond1
        JOIN concept c2 ON c2.concept_id = sq.cond2
        JOIN concept c3 ON c3.concept_id = sq.cond3
        JOIN concept c4 ON c4.concept_id = sq.cond4
        GROUP BY
        sq.cond1, c1.concept_name,
        sq.cond2, c2.concept_name,
        sq.cond3, c3.concept_name,
        sq.cond4, c4.concept_name
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients with condition {0}, {1}, {2} or {3}.",
            "patients_4conditions_or"
        )

    def patients_3conditions_and_time(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        exposures AS (
        SELECT DISTINCT
            co.person_id,
            co.condition_concept_id,
            co.condition_start_date::date AS start_date
        FROM condition_occurrence co
        JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        ),
        triads AS (
        SELECT
            e1.condition_concept_id AS cond1_concept_id,
            e2.condition_concept_id AS cond2_concept_id,
            e3.condition_concept_id AS cond3_concept_id,
            COUNT(DISTINCT e1.person_id) AS person_count
        FROM exposures e1
        JOIN exposures e2
            ON e2.person_id = e1.person_id
        AND e2.condition_concept_id > e1.condition_concept_id
        JOIN exposures e3
            ON e3.person_id = e1.person_id
        AND e3.condition_concept_id > e2.condition_concept_id
        WHERE
            (GREATEST(e1.start_date, e2.start_date, e3.start_date)
            - LEAST(e1.start_date, e2.start_date, e3.start_date)) <= 300
        GROUP BY
            e1.condition_concept_id, e2.condition_concept_id, e3.condition_concept_id
        )
        SELECT
        c1.concept_name AS condition1_name,
        c2.concept_name AS condition2_name,
        c3.concept_name AS condition3_name
        FROM triads t
        JOIN concept c1 ON c1.concept_id = t.cond1_concept_id
        JOIN concept c2 ON c2.concept_id = t.cond2_concept_id
        JOIN concept c3 ON c3.concept_id = t.cond3_concept_id
        ORDER BY t.person_count DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients with condition {0}, {1} and {2} within 300 days.",
            "patients_3conditions_and_time",
            300
        )

    def patients_3conditions_and(self):

        query = f"""
        WITH valid_conditions AS (
            SELECT c.concept_id
            FROM concept c
            WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        exposures AS (
            SELECT DISTINCT
                co.person_id,
                co.condition_concept_id,
                co.condition_start_date::date AS start_date
            FROM condition_occurrence co
            JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        ),
        triads AS (
            SELECT
                e1.condition_concept_id AS cond1_concept_id,
                e2.condition_concept_id AS cond2_concept_id,
                e3.condition_concept_id AS cond3_concept_id,
                COUNT(DISTINCT e1.person_id) AS person_count
            FROM exposures e1
            JOIN exposures e2
            ON e2.person_id = e1.person_id
            AND e2.condition_concept_id > e1.condition_concept_id
            JOIN exposures e3
            ON e3.person_id = e1.person_id
            AND e3.condition_concept_id > e2.condition_concept_id
            GROUP BY
                e1.condition_concept_id,
                e2.condition_concept_id,
                e3.condition_concept_id
        )
        SELECT
            c1.concept_name AS condition1_name,
            c2.concept_name AS condition2_name,
            c3.concept_name AS condition3_name
            -- t.person_count  AS patient_count
        FROM triads t
        JOIN concept c1 ON c1.concept_id = t.cond1_concept_id
        JOIN concept c2 ON c2.concept_id = t.cond2_concept_id
        JOIN concept c3 ON c3.concept_id = t.cond3_concept_id
        ORDER BY t.person_count DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients with condition {0}, {1} and {2}.",
            "patients_3conditions_and"
        )
    
    def patients_3conditions_or(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT concept_id
        FROM concept
        WHERE domain_id = 'Condition'
            AND standard_concept = 'S'
            AND invalid_reason IS NULL
        ),
        eras AS (
        SELECT
            ce.person_id,
            ce.condition_concept_id,
            ce.condition_era_start_date AS start_date,
            ce.condition_era_end_date   AS end_date
        FROM condition_era ce
        JOIN valid_conditions vc ON vc.concept_id = ce.condition_concept_id
        ),
        cooccur_triples AS (
        SELECT
            e1.person_id,
            e1.condition_concept_id AS cond1,
            e2.condition_concept_id AS cond2,
            e3.condition_concept_id AS cond3
        FROM eras e1
        JOIN eras e2
            ON e2.person_id = e1.person_id
        AND e2.condition_concept_id > e1.condition_concept_id
        JOIN eras e3
            ON e3.person_id = e1.person_id
        AND e3.condition_concept_id > e2.condition_concept_id
        GROUP BY e1.person_id, e1.condition_concept_id, e2.condition_concept_id, e3.condition_concept_id
        ),
        overlapping_triples AS (
        SELECT DISTINCT
            a.person_id,
            a.condition_concept_id AS cond1,
            b.condition_concept_id AS cond2,
            c.condition_concept_id AS cond3
        FROM eras a
        JOIN eras b
            ON b.person_id = a.person_id
        AND b.condition_concept_id > a.condition_concept_id
        JOIN eras c
            ON c.person_id = a.person_id
        AND c.condition_concept_id > b.condition_concept_id
        WHERE GREATEST(a.start_date, b.start_date, c.start_date)
                <= LEAST(a.end_date, b.end_date, c.end_date)
        ),
        separate_triples AS (
        SELECT ct.person_id, ct.cond1, ct.cond2, ct.cond3
        FROM cooccur_triples ct
        LEFT JOIN overlapping_triples ot
            ON ot.person_id = ct.person_id
        AND ot.cond1 = ct.cond1
        AND ot.cond2 = ct.cond2
        AND ot.cond3 = ct.cond3
        WHERE ot.person_id IS NULL
        ),
        stats AS (
        SELECT cond1, cond2, cond3, COUNT(DISTINCT person_id) AS patient_count
        FROM separate_triples
        GROUP BY cond1, cond2, cond3
        )
        SELECT
        c1.concept_name AS condition1_name,
        c2.concept_name AS condition2_name,
        c3.concept_name AS condition3_name
        FROM stats s
        JOIN concept c1 ON c1.concept_id = s.cond1
        JOIN concept c2 ON c2.concept_id = s.cond2
        JOIN concept c3 ON c3.concept_id = s.cond3
        ORDER BY s.patient_count DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients with condition {0}, {1} or {2}.",
            "patients_3conditions_or"
        )

    def patients_distribution_by_birth(self):
        """
        Distribution of patients by year of birth.
        """
        text = "Distribution of patients by year of birth."
        query, params = self._get_template_sql("patients_distribution_by_birth")
        return self._add_result(text, query, params)

    def patients_condition_followed_condition(self):

        query = f"""
        WITH valid_conditions AS (
            SELECT c.concept_id
            FROM concept c
            WHERE c.domain_id = 'Condition'
                AND c.standard_concept = 'S'
                AND c.invalid_reason IS NULL
        ),
        occ AS (
            SELECT
                co.person_id,
                co.condition_concept_id,
                co.condition_start_date::date AS start_date
            FROM condition_occurrence co
            JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        ),
        condition_pairs AS (
            SELECT DISTINCT
                a.person_id,
                a.condition_concept_id AS cond1_id,
                b.condition_concept_id AS cond2_id
            FROM occ a
            JOIN occ b ON b.person_id = a.person_id
                AND b.start_date > a.start_date
                AND a.condition_concept_id <> b.condition_concept_id
        )
        SELECT
            c1.concept_name AS first_condition,
            c2.concept_name AS second_condition
            -- ,COUNT(DISTINCT cp.person_id) AS person_count
        FROM condition_pairs cp
        JOIN concept c1 ON c1.concept_id = cp.cond1_id
        JOIN concept c2 ON c2.concept_id = cp.cond2_id
        GROUP BY c1.concept_name, c2.concept_name
        ORDER BY COUNT(DISTINCT cp.person_id) DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "How many people have condition {0} followed by condition {1}?",
            "patients_condition_followed_condition"
        )

    def patients_condition_time_condition(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        occ AS (
        SELECT
            co.person_id,
            co.condition_concept_id,
            co.condition_start_date::date AS start_date
        FROM condition_occurrence co
        JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        ),
        pairs AS (
        -- one row per (person, A, B) where B happens at least 30 days after A
        SELECT DISTINCT
            o1.person_id,
            o1.condition_concept_id AS cond_a,
            o2.condition_concept_id AS cond_b
        FROM occ o1
        JOIN occ o2
            ON o2.person_id = o1.person_id
        AND o2.condition_concept_id <> o1.condition_concept_id
        AND (o2.start_date - o1.start_date) >= 30
        )
        SELECT
        c1.concept_name AS condition_a_name,
        c2.concept_name AS condition_b_name
        FROM pairs p
        JOIN concept c1 ON c1.concept_id = p.cond_a
        JOIN concept c2 ON c2.concept_id = p.cond_b
        GROUP BY c1.concept_name, c2.concept_name
        ORDER BY COUNT(DISTINCT p.person_id) DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "How many people have condition {1} more than 30 days after diagnosed by condition {0}?",
            "patients_condition_time_condition",
            30
        )

    def patients_condition_age(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        condition_ages AS (
        SELECT
            co.condition_concept_id,
            FLOOR(EXTRACT(YEAR FROM co.condition_start_date) - p.year_of_birth) AS age
        FROM condition_occurrence co
        JOIN person p
            ON p.person_id = co.person_id
        JOIN valid_conditions vc
            ON vc.concept_id = co.condition_concept_id
        ),
        counts AS (
        SELECT
            ca.condition_concept_id,
            ca.age,
            COUNT(*) AS n
        FROM condition_ages ca
        WHERE ca.age IS NOT NULL AND ca.age BETWEEN 0 AND 120
        GROUP BY ca.condition_concept_id, ca.age
        ),
        ranked AS (
        SELECT
            c1.concept_name,
            ca.age,
            ca.n,
            RANK() OVER (PARTITION BY ca.condition_concept_id ORDER BY ca.n DESC) AS rnk,
            SUM(ca.n) OVER (PARTITION BY ca.condition_concept_id) AS total_condition_count
        FROM counts ca
        JOIN concept c1 ON c1.concept_id = ca.condition_concept_id
        )
        SELECT
        concept_name AS condition_name,
        age AS most_common_age
        -- n AS age_count,
        -- total_condition_count
        FROM ranked
        WHERE rnk = 1
        ORDER BY total_condition_count DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "How many people have condition {0} at age {1}?",
            "patients_condition_age"
        )

    def patients_condition_race(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        cond_persons AS (
        SELECT DISTINCT
            co.person_id,
            co.condition_concept_id
        FROM condition_occurrence co
        JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        ),
        race_labeled AS (
        SELECT
            cp.condition_concept_id,
            cp.person_id,
            COALESCE(rc.concept_name, 'Unknown') AS race_name
        FROM cond_persons cp
        JOIN person p
            ON p.person_id = cp.person_id
        LEFT JOIN concept rc
            ON rc.concept_id = p.race_concept_id
        ),
        counts AS (
        SELECT
            condition_concept_id,
            race_name,
            COUNT(DISTINCT person_id) AS n
        FROM race_labeled
        GROUP BY condition_concept_id, race_name
        ),
        ranked AS (
        SELECT
            c1.concept_name AS condition_name,
            race_name,
            n,
            RANK() OVER (PARTITION BY condition_concept_id ORDER BY n DESC, race_name) AS rnk,
            SUM(n) OVER (PARTITION BY condition_concept_id) AS total_patients
        FROM counts
        JOIN concept c1 ON c1.concept_id = condition_concept_id
        )
        SELECT
        condition_name,
        race_name AS most_common_race
        -- n         AS race_patient_count,
        -- total_patients
        FROM ranked
        WHERE rnk = 1
        ORDER BY total_patients DESC, condition_name
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "How many people have condition {0} in the cohort of race {1}?",
            "patients_condition_race"
        )

    def patients_condition_state(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        cond_persons AS (
        SELECT DISTINCT
            co.person_id,
            co.condition_concept_id
        FROM condition_occurrence co
        JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        ),
        state_labeled AS (
        SELECT
            cp.condition_concept_id,
            cp.person_id,
            l.state AS state_name
        FROM cond_persons cp
        JOIN person p
            ON p.person_id = cp.person_id
        JOIN location l
            ON l.location_id = p.location_id
        WHERE l.state IS NOT NULL
        ),
        counts AS (
        SELECT
            condition_concept_id,
            state_name,
            COUNT(DISTINCT person_id) AS n
        FROM state_labeled
        GROUP BY condition_concept_id, state_name
        ),
        ranked AS (
        SELECT
            c1.concept_name AS condition_name,
            state_name,
            n,
            RANK() OVER (PARTITION BY condition_concept_id ORDER BY n DESC, state_name) AS rnk,
            SUM(n) OVER (PARTITION BY condition_concept_id) AS total_patients
        FROM counts
        JOIN concept c1 ON c1.concept_id = condition_concept_id
        )
        SELECT
        condition_name,
        state_name AS most_common_state
        -- n         AS state_patient_count,
        -- total_patients
        FROM ranked
        WHERE rnk = 1
        ORDER BY total_patients DESC, condition_name
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "How many people have condition {0} in the state {1}?",
            "patients_condition_state"
        )

    def patients_condition_year(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        condition_years AS (
        SELECT
            co.condition_concept_id,
            co.person_id,
            EXTRACT(YEAR FROM co.condition_start_date)::int AS year
        FROM condition_occurrence co
        JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        WHERE co.condition_start_date IS NOT NULL
        ),
        counts AS (
        SELECT
            condition_concept_id,
            year,
            COUNT(DISTINCT person_id) AS n
        FROM condition_years
        GROUP BY condition_concept_id, year
        ),
        ranked AS (
        SELECT
            c1.concept_name AS condition_name,
            year,
            n,
            RANK() OVER (PARTITION BY condition_concept_id ORDER BY n DESC, year) AS rnk,
            SUM(n) OVER (PARTITION BY condition_concept_id) AS total_patients
        FROM counts
        JOIN concept c1 ON c1.concept_id = condition_concept_id
        )
        SELECT
        condition_name,
        year AS most_common_year
        -- n         AS year_patient_count,
        -- total_patients
        FROM ranked
        WHERE rnk = 1
        ORDER BY total_patients DESC, condition_name
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "How many people were diagnosed with condition {0} in year {1}?",
            "patients_condition_year"
        )

    def patients_drug_time_drug(self):

        query = f"""
        WITH valid_drugs AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Drug'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        exp AS (
        SELECT
            de.person_id,
            de.drug_concept_id,
            de.drug_exposure_start_date::date AS start_date
        FROM drug_exposure de
        JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        pairs AS (
        -- one row per (person, A, B) where B starts at least 30 days after A
        SELECT DISTINCT
            e1.person_id,
            e1.drug_concept_id AS drug_a,
            e2.drug_concept_id AS drug_b
        FROM exp e1
        JOIN exp e2
            ON e2.person_id = e1.person_id
        AND e2.drug_concept_id <> e1.drug_concept_id
        AND (e2.start_date - e1.start_date) >= 30
        )
        SELECT
        c2.concept_name AS drug_b_name,
        c1.concept_name AS drug_a_name
        -- ,COUNT(DISTINCT p.person_id) AS patient_count
        FROM pairs p
        JOIN concept c1 ON c1.concept_id = p.drug_a
        JOIN concept c2 ON c2.concept_id = p.drug_b
        GROUP BY c1.concept_name, c2.concept_name
        ORDER BY COUNT(DISTINCT p.person_id) DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "How many people have treated by drug {1} after more than 30 days of starting with drug {0}?",
            "patients_drug_time_drug",
            30
        )

    def patients_drug_followed_drug(self):

        query = f"""
        WITH valid_drugs AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Drug'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        exp AS (
        SELECT
            de.person_id,
            de.drug_concept_id,
            de.drug_exposure_start_date::date AS start_date,
            de.drug_exposure_id
        FROM drug_exposure de
        JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        ordered AS (
        -- order exposures per patient by date (and exposure_id to break ties)
        SELECT
            person_id,
            drug_concept_id AS drug_a,
            LEAD(drug_concept_id) OVER (
            PARTITION BY person_id
            ORDER BY start_date, drug_exposure_id
            ) AS drug_b
        FROM exp
        ),
        pairs AS (
        SELECT DISTINCT
            person_id,
            drug_a,
            drug_b
        FROM ordered
        WHERE drug_b IS NOT NULL
            AND drug_a <> drug_b
        )
        SELECT
        c1.concept_name AS drug_a_name,
        c2.concept_name AS drug_b_name
        -- ,COUNT(DISTINCT p.person_id) AS patient_count
        FROM pairs p
        JOIN concept c1 ON c1.concept_id = p.drug_a
        JOIN concept c2 ON c2.concept_id = p.drug_b
        GROUP BY c1.concept_name, c2.concept_name
        ORDER BY COUNT(DISTINCT p.person_id) DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "How many people have treated by drug {0} followed by drug {1}?",
            "patients_drug_followed_drug"
        )

    def patients_condition_ethnicity(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        cond_persons AS (
        SELECT DISTINCT
            co.person_id,
            co.condition_concept_id
        FROM condition_occurrence co
        JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        ),
        ethnicity_labeled AS (
        SELECT
            cp.condition_concept_id,
            cp.person_id,
            COALESCE(ec.concept_name, 'Unknown') AS ethnicity_name
        FROM cond_persons cp
        JOIN person p
            ON p.person_id = cp.person_id
        LEFT JOIN concept ec
            ON ec.concept_id = p.ethnicity_concept_id
        ),
        counts AS (
        SELECT
            condition_concept_id,
            ethnicity_name,
            COUNT(DISTINCT person_id) AS n
        FROM ethnicity_labeled
        GROUP BY condition_concept_id, ethnicity_name
        ),
        ranked AS (
        SELECT
            c1.concept_name AS condition_name,
            ethnicity_name,
            n,
            RANK() OVER (PARTITION BY condition_concept_id ORDER BY n DESC, ethnicity_name) AS rnk,
            SUM(n) OVER (PARTITION BY condition_concept_id) AS total_patients
        FROM counts
        JOIN concept c1 ON c1.concept_id = condition_concept_id
        )
        SELECT
        condition_name,
        ethnicity_name AS most_common_ethnicity
        -- n         AS ethnicity_patient_count,
        -- total_patients
        FROM ranked
        WHERE rnk = 1
        ORDER BY total_patients DESC, condition_name
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "How many people have condition {0} in the cohort of ethnicity {1}?",
            "patients_condition_ethnicity"
        )

    def patients_drug_year(self):

        query = f"""
        WITH valid_drugs AS (
        SELECT concept_id
        FROM concept
        WHERE domain_id = 'Drug'
            AND standard_concept = 'S'
            AND invalid_reason IS NULL
        ),
        exposures AS (
        SELECT
            de.person_id,
            de.drug_concept_id,
            de.drug_exposure_start_date::date AS start_date
        FROM drug_exposure de
        JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        yearly_counts AS (
        SELECT
            EXTRACT(YEAR FROM start_date)::int AS year,
            drug_concept_id,
            COUNT(DISTINCT person_id) AS patient_count
        FROM exposures
        GROUP BY 1, 2
        ),
        ranked AS (
        SELECT
            yc.year,
            yc.drug_concept_id,
            yc.patient_count,
            ROW_NUMBER() OVER (
            PARTITION BY yc.year
            ORDER BY yc.patient_count DESC, yc.drug_concept_id
            ) AS rnum
        FROM yearly_counts yc
        )
        SELECT
        c.concept_name AS drug_name,
        r.year
        -- , r.patient_count
        FROM ranked r
        JOIN concept c ON c.concept_id = r.drug_concept_id
        WHERE r.rnum <= 20
        ORDER BY r.year, r.patient_count DESC, drug_name
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "How many people were taking drug {0} in year {1}.",
            "patients_drug_year"
        )

    def patients_drug_after_condition(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        valid_drugs AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Drug'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        cond_occ AS (
        SELECT
            co.person_id,
            co.condition_concept_id,
            co.condition_start_date::date AS cond_date
        FROM condition_occurrence co
        JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        ),
        drug_exp AS (
        SELECT
            de.person_id,
            de.drug_concept_id,
            de.drug_exposure_start_date::date AS drug_date
        FROM drug_exposure de
        JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        pairs AS (
        -- one row per (person, condition, drug) where drug starts after condition
        SELECT DISTINCT
            co.person_id,
            co.condition_concept_id,
            de.drug_concept_id
        FROM cond_occ co
        JOIN drug_exp de
            ON de.person_id = co.person_id
        AND de.drug_date > co.cond_date
        )
        SELECT
        c1.concept_name AS condition_name,
        c2.concept_name AS drug_name
        -- ,COUNT(DISTINCT p.person_id) AS patient_count
        FROM pairs p
        JOIN concept c1 ON c1.concept_id = p.condition_concept_id
        JOIN concept c2 ON c2.concept_id = p.drug_concept_id
        GROUP BY c1.concept_name, c2.concept_name
        ORDER BY COUNT(DISTINCT p.person_id) DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "How many people took drug {1} after being diagnosed with condition {0}?",
            "patients_drug_after_condition"
        )

    def patients_drug_time_after_condition(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        valid_drugs AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Drug'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        cond_occ AS (
        SELECT
            co.person_id,
            co.condition_concept_id,
            co.condition_start_date::date AS cond_date
        FROM condition_occurrence co
        JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        ),
        drug_exp AS (
        SELECT
            de.person_id,
            de.drug_concept_id,
            de.drug_exposure_start_date::date AS drug_date
        FROM drug_exposure de
        JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        pairs AS (
        -- one row per (person, condition, drug) where drug starts more than 30 days after condition
        SELECT DISTINCT
            co.person_id,
            co.condition_concept_id,
            de.drug_concept_id
        FROM cond_occ co
        JOIN drug_exp de
            ON de.person_id = co.person_id
        AND (de.drug_date - co.cond_date) > 30
        )
        SELECT
        c1.concept_name AS condition_name,
        c2.concept_name AS drug_name
        -- ,COUNT(DISTINCT p.person_id) AS patient_count
        FROM pairs p
        JOIN concept c1 ON c1.concept_id = p.condition_concept_id
        JOIN concept c2 ON c2.concept_id = p.drug_concept_id
        GROUP BY c1.concept_name, c2.concept_name
        ORDER BY COUNT(DISTINCT p.person_id) DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "How many people were treated by drug {1} more than 30 days after being diagnosed with condition {0}?",
            "patients_drug_time_after_condition",
            30
        )

    def patients_gender_condition(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT c.concept_id
        FROM concept c
        WHERE c.domain_id = 'Condition'
            AND c.standard_concept = 'S'
            AND c.invalid_reason IS NULL
        ),
        cond_persons AS (
        SELECT DISTINCT
            co.person_id,
            co.condition_concept_id
        FROM condition_occurrence co
        JOIN valid_conditions vc ON vc.concept_id = co.condition_concept_id
        ),
        gender_labeled AS (
        SELECT
            cp.condition_concept_id,
            cp.person_id,
            COALESCE(gc.concept_name, 'Unknown') AS gender_name
        FROM cond_persons cp
        JOIN person p
            ON p.person_id = cp.person_id
        LEFT JOIN concept gc
            ON gc.concept_id = p.gender_concept_id
        AND gc.domain_id = 'Gender'
        AND gc.standard_concept = 'S'
        ),
        counts AS (
        SELECT
            condition_concept_id,
            gender_name,
            COUNT(DISTINCT person_id) AS n
        FROM gender_labeled
        GROUP BY condition_concept_id, gender_name
        ),
        ranked AS (
        SELECT
            c1.concept_name AS condition_name,
            gender_name,
            n,
            RANK() OVER (PARTITION BY condition_concept_id ORDER BY n DESC, gender_name) AS rnk,
            SUM(n) OVER (PARTITION BY condition_concept_id) AS total_patients
        FROM counts
        JOIN concept c1 ON c1.concept_id = condition_concept_id
        )
        SELECT
        gender_name AS most_common_gender,
        condition_name
        -- n         AS gender_patient_count,
        -- total_patients
        FROM ranked
        WHERE rnk = 1
        ORDER BY total_patients DESC, condition_name
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Number of {0} patients with {1}.",
            "patients_gender_condition"
        )
    
    def patients_year(self):

        query = f"""
        SELECT
        p.year_of_birth AS year
        -- , COUNT(*)        AS patient_count
        FROM person p
        GROUP BY p.year_of_birth
        ORDER BY COUNT(*) DESC, year
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Number of patients born in year {0}.",
            "patients_year"
        )

    def patients_gender_state(self):
        """
        Number of patients by gender and state.
        """
        text = "Number of patients by gender and state."
        query, params = self._get_template_sql("patients_gender_state")
        return self._add_result(text, query, params)

    def patients_group_by_ethnicity_location(self):
        """
        Number of patients grouped by ethnicity and residence state location.
        """
        text = "Number of patients grouped by ethnicity and residence state location."
        query, params = self._get_template_sql("patients_group_by_ethnicity_location")
        return self._add_result(text, query, params)

    def patients_group_by_ethnicity_birth(self):
        """
        Number of patients grouped by ethnicity and year of birth.
        """
        text = "Number of patients grouped by ethnicity and year of birth."
        query, params = self._get_template_sql("patients_group_by_ethnicity_birth")
        return self._add_result(text, query, params)

    def patients_group_by_ethnicity(self):
        """
        Number of patients grouped by ethnicity.
        """
        text = "Number of patients grouped by ethnicity."
        query, params = self._get_template_sql("patients_group_by_ethnicity")
        return self._add_result(text, query, params)

    def patients_group_by_gender(self):
        """
        Number of patients grouped by gender.
        """
        text = "Number of patients grouped by gender."
        query, params = self._get_template_sql("patients_group_by_gender")
        return self._add_result(text, query, params)

    def patients_group_by_race_ethnicity(self):
        """
        Number of patients grouped by race and ethnicity.
        """
        text = "Number of patients grouped by race and ethnicity."
        query, params = self._get_template_sql("patients_group_by_race_ethnicity")
        return self._add_result(text, query, params)

    def patients_grouped_by_race_gender(self):
        """
        Number of patients grouped by race and gender.
        """
        text = "Number of patients grouped by race and gender."
        query, params = self._get_template_sql("patients_grouped_by_race_gender")
        return self._add_result(text, query, params)

    def patients_group_by_race_location(self):
        """
        Number of patients grouped by race and residence state location.
        """
        text = "Number of patients grouped by race and residence state location."
        query, params = self._get_template_sql("patients_group_by_race_location")
        return self._add_result(text, query, params)

    def patients_group_by_race_birth(self):
        """
        Number of patients grouped by race and year of birth.
        """
        text = "Number of patients grouped by race and year of birth."
        query, params = self._get_template_sql("patients_group_by_race_birth")
        return self._add_result(text, query, params)

    def patients_group_by_location(self):
        """
        Number of patients grouped by residence state location.
        """
        text = "Number of patients grouped by residence state location."
        query, params = self._get_template_sql("patients_group_by_location")
        return self._add_result(text, query, params)

    def patients_group_by_birth_gender(self):
        """
        Number of patients grouped by year of birth and gender.
        """
        text = "Number of patients grouped by year of birth and gender."
        query, params = self._get_template_sql("patients_group_by_birth_gender")
        return self._add_result(text, query, params)

    def patients_group_by_birth_location(self):
        """
        Number of patients grouped by year of birth and residence state location.
        """
        text = "Number of patients grouped by year of birth and residence state location."
        query, params = self._get_template_sql("patients_group_by_birth_location")
        return self._add_result(text, query, params)

    def patients_count(self):
        """
        Number of patients in the dataset.
        """
        text = "Number of patients in the dataset."
        query, params = self._get_template_sql("patients_count")
        return self._add_result(text, query, params)
    
    def patients_count_by_ethnicity(self):

        query = f"""
        SELECT
        COALESCE(c.concept_name, 'Unknown') AS ethnicity
        --, COUNT(*) AS patient_count
        FROM person p
        LEFT JOIN concept c
        ON c.concept_id = p.ethnicity_concept_id
        GROUP BY c.concept_name
        ORDER BY COUNT(*) DESC;
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Number of patients of ethnicity {0}.",
            "patients_count_by_ethnicity"
        )

    def patients_count_by_race(self):

        query = f"""
        SELECT
        c.concept_name AS race
        --, COUNT(*) AS patient_count
        FROM person p
        JOIN concept c
        ON c.concept_id = p.race_concept_id
        WHERE c.domain_id = 'Race'
        GROUP BY c.concept_name
        ORDER BY COUNT(*) DESC;
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Number of patients of race {0}.",
            "patients_count_by_race"
        )

    def patients_count_by_gender(self):

        query = f"""
        SELECT
        COALESCE(c.concept_name, 'Unknown') AS gender
        -- ,COUNT(*) AS patient_count
        FROM person p
        LEFT JOIN concept c
        ON c.concept_id = p.gender_concept_id
        AND c.domain_id = 'Gender'
        AND c.standard_concept = 'S'
        AND c.invalid_reason IS NULL
        GROUP BY COALESCE(c.concept_name, 'Unknown')
        ORDER BY COUNT(*) DESC;
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Number of patients of specific gender {0}.",
            "patients_count_by_gender"
        )
    
    def patients_drug(self):

        query = f"""
        WITH valid_drugs AS (
        SELECT concept_id
        FROM concept
        WHERE domain_id = 'Drug'
            AND standard_concept = 'S'
            AND invalid_reason IS NULL
        )
        SELECT
        c.concept_name AS drug_name
        -- ,COUNT(DISTINCT de.person_id) AS patient_count
        FROM drug_exposure de
        JOIN valid_drugs vd
        ON vd.concept_id = de.drug_concept_id
        JOIN concept c
        ON c.concept_id = de.drug_concept_id
        GROUP BY c.concept_name
        ORDER BY COUNT(DISTINCT de.person_id) DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Number of patients taking {0}.",
            "patients_drug"
        )

    def patients_condition(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT concept_id
        FROM concept
        WHERE domain_id = 'Condition'
            AND standard_concept = 'S'
            AND invalid_reason IS NULL
        )
        SELECT
        c.concept_name AS condition_name
        -- ,COUNT(DISTINCT co.person_id) AS patient_count
        FROM condition_occurrence co
        JOIN valid_conditions vc
        ON vc.concept_id = co.condition_concept_id
        JOIN concept c
        ON c.concept_id = co.condition_concept_id
        GROUP BY c.concept_name
        ORDER BY COUNT(DISTINCT co.person_id) DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Number of patients with {0}.",
            "patients_condition"
        )

    def patients_count_by_location(self):
        """
        Number of patients grouped by residence state location.
        """
        query = f"""
        SELECT
        l.state AS location
        -- ,COUNT(DISTINCT p.person_id) AS patient_count
        FROM person p
        JOIN location l ON p.location_id = l.location_id
        WHERE l.state IS NOT NULL
        GROUP BY l.state
        ORDER BY COUNT(DISTINCT p.person_id) DESC
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Number of patients with residence state location at {0}.",
            "patients_count_by_location"
        )

    def patients_condition_group_by_year(self):

        query = f"""
        WITH valid_conditions AS (
        SELECT concept_id
        FROM concept
        WHERE domain_id = 'Condition'
            AND standard_concept = 'S'
            AND invalid_reason IS NULL
        ),
        yearly_counts AS (
        SELECT
            EXTRACT(YEAR FROM condition_start_date)::int AS year,
            condition_concept_id,
            COUNT(DISTINCT person_id) AS patient_count
        FROM condition_occurrence
        JOIN valid_conditions vc ON vc.concept_id = condition_concept_id
        GROUP BY 1, 2
        ),
        ranked AS (
        SELECT
            yc.year,
            yc.condition_concept_id,
            yc.patient_count,
            ROW_NUMBER() OVER (
            PARTITION BY yc.year
            ORDER BY yc.patient_count DESC, yc.condition_concept_id
            ) AS rnum
        FROM yearly_counts yc
        )
        SELECT
        c.concept_name AS condition_name,
        r.year
        -- , r.patient_count
        FROM ranked r
        JOIN concept c ON c.concept_id = r.condition_concept_id
        WHERE r.rnum <= 20
        ORDER BY r.year, r.patient_count DESC, condition_name
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients with condition {0} grouped by year of diagnosis.",
            "patients_condition_group_by_year"
        )

    def patients_drug_group_by_year(self):

        query = f"""
        WITH valid_drugs AS (
        SELECT concept_id
        FROM concept
        WHERE domain_id = 'Drug'
            AND standard_concept = 'S'
            AND invalid_reason IS NULL
        ),
        exposures AS (
        SELECT
            de.person_id,
            de.drug_concept_id,
            de.drug_exposure_start_date::date AS start_date
        FROM drug_exposure de
        JOIN valid_drugs vd ON vd.concept_id = de.drug_concept_id
        ),
        yearly_counts AS (
        SELECT
            EXTRACT(YEAR FROM start_date)::int AS year,
            drug_concept_id,
            COUNT(DISTINCT person_id) AS patient_count
        FROM exposures
        GROUP BY 1, 2
        ),
        ranked AS (
        SELECT
            yc.year,
            yc.drug_concept_id,
            yc.patient_count,
            ROW_NUMBER() OVER (
            PARTITION BY yc.year
            ORDER BY yc.patient_count DESC, yc.drug_concept_id
            ) AS rnum
        FROM yearly_counts yc
        )
        SELECT
        c.concept_name AS drug_name,
        r.year
        -- , r.patient_count
        FROM ranked r
        JOIN concept c ON c.concept_id = r.drug_concept_id
        WHERE r.rnum <= 20
        ORDER BY r.year, r.patient_count DESC, drug_name
        LIMIT {self.result_limit};
        """
        results = self._run_query(query)
        return self._process_results(
            results,
            "Counts of patients taking drug {0} grouped by year of prescription.",
            "patients_drug_group_by_year"
        )

    def patients_group_by_gender_and_ethn(self):
        """
        Number of patients grouped by gender and ethnicity.
        """
        text = "Number of patients grouped by gender and ethnicity."
        query, params = self._get_template_sql("patients_group_by_gender_and_ethn")
        return self._add_result(text, query, params)

    def patients_group_by_race(self):
        """
        Count of patients grouped by race.
        """
        text = "Count of patients grouped by race."
        query, params = self._get_template_sql("patients_group_by_race")
        return self._add_result(text, query, params)
