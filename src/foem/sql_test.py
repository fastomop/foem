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
        self.template_dir = Path(__file__).parent.parent.parent / "template"
        self.query_dir = Path(__file__).parent.parent.parent / "query"
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

        # Mapping from query method names to SQL file numbers
        self.query_map = {
            "patients_2drugs_and_time": "Q0001",
            "patients_2drugs_and": "Q0002",
            "patients_2drugs_or": "Q0003",
            "patients_4drugs_and_time": "Q0004",
            "patients_4drugs_and": "Q0005",
            "patients_4drugs_or": "Q0006",
            "patients_3drugs_and_time": "Q0007",
            "patients_3drugs_and": "Q0008",
            "patients_3drugs_or": "Q0009",
            "patients_2conditions_and_time": "Q0010",
            "patients_2conditions_and": "Q0011",
            "patients_2conditions_or": "Q0012",
            "patients_4conditions_and_time": "Q0013",
            "patients_4conditions_and": "Q0014",
            "patients_4conditions_or": "Q0015",
            "patients_3conditions_and_time": "Q0016",
            "patients_3conditions_and": "Q0017",
            "patients_3conditions_or": "Q0018",
            "patients_condition_followed_condition": "Q0019",
            "patients_condition_time_condition": "Q0020",
            "patients_condition_age": "Q0021",
            "patients_condition_race": "Q0022",
            "patients_condition_state": "Q0023",
            "patients_condition_year": "Q0024",
            "patients_drug_time_drug": "Q0025",
            "patients_drug_followed_drug": "Q0026",
            "patients_condition_ethnicity": "Q0027",
            "patients_drug_year": "Q0028",
            "patients_drug_after_condition": "Q0029",
            "patients_drug_time_after_condition": "Q0030",
            "patients_gender_condition": "Q0031",
            "patients_year": "Q0032",
            "patients_count_by_ethnicity": "Q0033",
            "patients_count_by_race": "Q0034",
            "patients_count_by_gender": "Q0035",
            "patients_drug": "Q0036",
            "patients_condition": "Q0037",
            "patients_count_by_location": "Q0038",
            "patients_condition_group_by_year": "Q0039",
            "patients_drug_group_by_year": "Q0040",
        }
    
    def close(self) -> None:
        """Close the database connection."""
        try:
            if self.conn:
                self.conn.close()
        finally:
            self.conn = None

    def _read_template(self, method_name: str):
        """Read SQL template from template folder based on method name."""
        file_id = self.template_map.get(method_name)
        if not file_id:
            raise ValueError(f"No template file found for method: {method_name}")

        file_path = self.template_dir / f"{file_id}.sql"
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

    def _read_query(self, method_name: str):
        """Read SQL query from query folder based on method name."""
        file_id = self.query_map.get(method_name)
        if not file_id:
            raise ValueError(f"No query file found for method: {method_name}")

        file_path = self.query_dir / f"{file_id}.sql"
        if not file_path.exists():
            raise FileNotFoundError(f"Query file not found: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Extract SQL (skip comment lines)
        lines = content.split('\n')
        sql_lines = []

        for line in lines:
            if not line.strip().startswith('--'):
                sql_lines.append(line)

        return '\n'.join(sql_lines).strip()

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
                # Special case: if method also has 'gender', 'race', 'ethnicity', or 'state'
                # only use the LAST concept (the condition), not demographic values
                if any(keyword in template_method_name for keyword in ['gender', 'race', 'ethnicity', 'state']):
                    # Only use the last concept found (should be the condition)
                    if ids_flat:
                        vocab_id, code_id = ids_flat[-1]
                        params['v_id1'] = vocab_id
                        params['c_id1'] = code_id
                else:
                    # Normal condition processing - use all concepts
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
                # Don't create year parameter for group_by methods - they GROUP BY year, not filter by it
                if 'group_by_year' not in template_method_name:
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
                # For methods with both gender and condition, gender is typically the first string value
                if 'condition' in template_method_name:
                    # Extract first string value as gender
                    gender_values = [val for val in row if isinstance(val, str)]
                    if gender_values:
                        params['gender'] = gender_values[0]
                else:
                    # Original logic: find gender value that's not a concept
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

    def _execute_query_and_process(self, method_name, text_template, *args):
        """
        Helper method for two-phase query execution pattern.

        Reads query from query folder, executes it, and processes results through template.

        Args:
            method_name: Name of the method (used to look up query and template files)
            text_template: Format string for generating natural language description
            *args: Additional arguments to pass to _process_results (e.g., days parameter)

        Returns:
            List of result dictionaries
        """
        query = self._read_query(method_name)
        query = query.format(self=self)
        results = self._run_query(query)
        return self._process_results(results, text_template, method_name, *args)

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
        return self._execute_query_and_process(
            "patients_2drugs_and_time",
            "Counts of patients taking drug {0} and {1} within 30 days.",
            30
        )

    def patients_2drugs_and(self):
        return self._execute_query_and_process(
            "patients_2drugs_and",
            "Counts of patients taking drug {0} and {1}."
        )

    def patients_2drugs_or(self):
        return self._execute_query_and_process(
            "patients_2drugs_or",
            "Counts of patients taking drug {0} or {1}."
        )

    def patients_4drugs_and_time(self):
        return self._execute_query_and_process(
            "patients_4drugs_and_time",
            "Counts of patients taking drug {0}, {1}, {2} and {3} within 30 days.",
            30
        )

    def patients_4drugs_and(self):
        return self._execute_query_and_process(
            "patients_4drugs_and",
            "Counts of patients taking drug {0}, {1}, {2} and {3}."
        )

    def patients_4drugs_or(self):
        return self._execute_query_and_process(
            "patients_4drugs_or",
            "Counts of patients taking drug {0}, {1}, {2} or {3}."
        )

    def patients_3drugs_and_time(self):
        return self._execute_query_and_process(
            "patients_3drugs_and_time",
            "Counts of patients taking drug {0}, {1} and {2} within 30 days.",
            30
        )

    def patients_3drugs_and(self):
        return self._execute_query_and_process(
            "patients_3drugs_and",
            "Counts of patients taking drug {0}, {1} and {2}."
        )

    def patients_3drugs_or(self):
        return self._execute_query_and_process(
            "patients_3drugs_or",
            "Counts of patients taking drug {0}, {1} or {2}."
        )

    def patients_2conditions_and_time(self):
        return self._execute_query_and_process(
            "patients_2conditions_and_time",
            "Counts of patients with condition {0} and {1} within 30 days.",
            30
        )

    def patients_2conditions_and(self):
        return self._execute_query_and_process(
            "patients_2conditions_and",
            "Counts of patients with condition {0} and {1}."
        )

    def patients_2conditions_or(self):
        return self._execute_query_and_process(
            "patients_2conditions_or",
            "Counts of patients with condition {0} or {1}."
        )

    def patients_4conditions_and_time(self):
        return self._execute_query_and_process(
            "patients_4conditions_and_time",
            "Counts of patients with condition {0}, {1}, {2} and {3} within 1000 days.",
            1000
        )

    def patients_4conditions_and(self):
        return self._execute_query_and_process(
            "patients_4conditions_and",
            "Counts of patients with condition {0}, {1}, {2} and {3}."
        )

    def patients_4conditions_or(self):
        return self._execute_query_and_process(
            "patients_4conditions_or",
            "Counts of patients with condition {0}, {1}, {2} or {3}."
        )

    def patients_3conditions_and_time(self):
        return self._execute_query_and_process(
            "patients_3conditions_and_time",
            "Counts of patients with condition {0}, {1} and {2} within 300 days.",
            300
        )

    def patients_3conditions_and(self):
        return self._execute_query_and_process(
            "patients_3conditions_and",
            "Counts of patients with condition {0}, {1} and {2}."
        )

    def patients_3conditions_or(self):
        return self._execute_query_and_process(
            "patients_3conditions_or",
            "Counts of patients with condition {0}, {1} or {2}."
        )

    def patients_distribution_by_birth(self):
        text = "Distribution of patients by year of birth."
        query, params = self._get_template_sql("patients_distribution_by_birth")
        return self._add_result(text, query, params)

    def patients_condition_followed_condition(self):
        return self._execute_query_and_process(
            "patients_condition_followed_condition",
            "How many people have condition {0} followed by condition {1}?"
        )

    def patients_condition_time_condition(self):
        return self._execute_query_and_process(
            "patients_condition_time_condition",
            "How many people have condition {1} more than 30 days after diagnosed by condition {0}?",
            30
        )

    def patients_condition_age(self):
        return self._execute_query_and_process(
            "patients_condition_age",
            "How many people have condition {0} at age {1}?"
        )

    def patients_condition_race(self):
        return self._execute_query_and_process(
            "patients_condition_race",
            "How many people have condition {0} in the cohort of race {1}?"
        )

    def patients_condition_state(self):
        return self._execute_query_and_process(
            "patients_condition_state",
            "How many people have condition {0} in the state {1}?"
        )

    def patients_condition_year(self):
        return self._execute_query_and_process(
            "patients_condition_year",
            "How many people were diagnosed with condition {0} in year {1}?"
        )

    def patients_drug_time_drug(self):
        return self._execute_query_and_process(
            "patients_drug_time_drug",
            "How many people have treated by drug {1} after more than 30 days of starting with drug {0}?",
            30
        )

    def patients_drug_followed_drug(self):
        return self._execute_query_and_process(
            "patients_drug_followed_drug",
            "How many people have treated by drug {0} followed by drug {1}?"
        )

    def patients_condition_ethnicity(self):
        return self._execute_query_and_process(
            "patients_condition_ethnicity",
            "How many people have condition {0} in the cohort of ethnicity {1}?"
        )

    def patients_drug_year(self):
        return self._execute_query_and_process(
            "patients_drug_year",
            "How many people were taking drug {0} in year {1}."
        )

    def patients_drug_after_condition(self):
        return self._execute_query_and_process(
            "patients_drug_after_condition",
            "How many people took drug {1} after being diagnosed with condition {0}?"
        )

    def patients_drug_time_after_condition(self):
        return self._execute_query_and_process(
            "patients_drug_time_after_condition",
            "How many people were treated by drug {1} more than 30 days after being diagnosed with condition {0}?",
            30
        )

    def patients_gender_condition(self):
        return self._execute_query_and_process(
            "patients_gender_condition",
            "Number of {0} patients with {1}."
        )

    def patients_year(self):
        return self._execute_query_and_process(
            "patients_year",
            "Number of patients born in year {0}."
        )

    def patients_gender_state(self):
        text = "Number of patients by gender and state."
        query, params = self._get_template_sql("patients_gender_state")
        return self._add_result(text, query, params)

    def patients_group_by_ethnicity_location(self):
        text = "Number of patients grouped by ethnicity and residence state location."
        query, params = self._get_template_sql("patients_group_by_ethnicity_location")
        return self._add_result(text, query, params)

    def patients_group_by_ethnicity_birth(self):
        text = "Number of patients grouped by ethnicity and year of birth."
        query, params = self._get_template_sql("patients_group_by_ethnicity_birth")
        return self._add_result(text, query, params)

    def patients_group_by_ethnicity(self):
        text = "Number of patients grouped by ethnicity."
        query, params = self._get_template_sql("patients_group_by_ethnicity")
        return self._add_result(text, query, params)

    def patients_group_by_gender(self):
        text = "Number of patients grouped by gender."
        query, params = self._get_template_sql("patients_group_by_gender")
        return self._add_result(text, query, params)

    def patients_group_by_race_ethnicity(self):
        text = "Number of patients grouped by race and ethnicity."
        query, params = self._get_template_sql("patients_group_by_race_ethnicity")
        return self._add_result(text, query, params)

    def patients_grouped_by_race_gender(self):
        text = "Number of patients grouped by race and gender."
        query, params = self._get_template_sql("patients_grouped_by_race_gender")
        return self._add_result(text, query, params)

    def patients_group_by_race_location(self):
        text = "Number of patients grouped by race and residence state location."
        query, params = self._get_template_sql("patients_group_by_race_location")
        return self._add_result(text, query, params)

    def patients_group_by_race_birth(self):
        text = "Number of patients grouped by race and year of birth."
        query, params = self._get_template_sql("patients_group_by_race_birth")
        return self._add_result(text, query, params)

    def patients_group_by_location(self):
        text = "Number of patients grouped by residence state location."
        query, params = self._get_template_sql("patients_group_by_location")
        return self._add_result(text, query, params)

    def patients_group_by_birth_gender(self):
        text = "Number of patients grouped by year of birth and gender."
        query, params = self._get_template_sql("patients_group_by_birth_gender")
        return self._add_result(text, query, params)

    def patients_group_by_birth_location(self):
        text = "Number of patients grouped by year of birth and residence state location."
        query, params = self._get_template_sql("patients_group_by_birth_location")
        return self._add_result(text, query, params)

    def patients_count(self):
        text = "Number of patients in the dataset."
        query, params = self._get_template_sql("patients_count")
        return self._add_result(text, query, params)
    
    def patients_count_by_ethnicity(self):
        return self._execute_query_and_process(
            "patients_count_by_ethnicity",
            "Number of patients of ethnicity {0}."
        )

    def patients_count_by_race(self):
        return self._execute_query_and_process(
            "patients_count_by_race",
            "Number of patients of race {0}."
        )

    def patients_count_by_gender(self):
        return self._execute_query_and_process(
            "patients_count_by_gender",
            "Number of patients of specific gender {0}."
        )

    def patients_drug(self):
        return self._execute_query_and_process(
            "patients_drug",
            "Number of patients taking {0}."
        )

    def patients_condition(self):
        return self._execute_query_and_process(
            "patients_condition",
            "Number of patients with {0}."
        )

    def patients_count_by_location(self):
        return self._execute_query_and_process(
            "patients_count_by_location",
            "Number of patients with residence state location at {0}."
        )

    def patients_condition_group_by_year(self):
        return self._execute_query_and_process(
            "patients_condition_group_by_year",
            "Counts of patients with condition {0} grouped by year of diagnosis."
        )

    def patients_drug_group_by_year(self):
        return self._execute_query_and_process(
            "patients_drug_group_by_year",
            "Counts of patients taking drug {0} grouped by year of prescription."
        )

    def patients_group_by_gender_and_ethn(self):
        text = "Number of patients grouped by gender and ethnicity."
        query, params = self._get_template_sql("patients_group_by_gender_and_ethn")
        return self._add_result(text, query, params)

    def patients_group_by_race(self):
        text = "Count of patients grouped by race."
        query, params = self._get_template_sql("patients_group_by_race")
        return self._add_result(text, query, params)
