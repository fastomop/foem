"""
Transpile SQL queries from PostgreSQL to Databricks Spark SQL using sqlglot.
"""

import sqlglot
from sqlglot import exp
from pathlib import Path


def _create_datediff(left: exp.Expression, right: exp.Expression) -> exp.Expression:
    """Create a DATEDIFF(left, right) expression."""
    return exp.Anonymous(
        this="DATEDIFF",
        expressions=[left, right]
    )


def _unwrap_timestamp_cast(expr: exp.Expression) -> exp.Expression:
    """Remove CAST(... AS TIMESTAMP) wrapper if present, returning the inner expression."""
    if isinstance(expr, exp.Cast):
        to_type = expr.to
        if isinstance(to_type, exp.DataType) and to_type.this == exp.DataType.Type.TIMESTAMP:
            return expr.this
    return expr


def _extract_date_operands_from_sub(sub_expr: exp.Expression):
    """
    Extract the date operands from a subtraction expression.
    Handles both simple (date - date) and CAST(... AS TIMESTAMP) patterns.
    Also handles function calls like GREATEST() and LEAST().
    Returns (left, right) operands with TIMESTAMP casts removed.
    """
    if isinstance(sub_expr, exp.Sub):
        left = _unwrap_timestamp_cast(sub_expr.left)
        right = _unwrap_timestamp_cast(sub_expr.right)
        # Accept any expression (columns, functions, etc.) as valid date operands
        # DATEDIFF can handle GREATEST/LEAST and other date functions
        return left, right
    return None, None


def _is_epoch_days_pattern(node: exp.Expression):
    """
    Check if node matches: CAST(EXTRACT(epoch FROM ...) / 86400 AS BIGINT)
    This is a PostgreSQL pattern for calculating day differences.
    Returns the subtraction expression if found, None otherwise.
    """
    # Pattern: CAST(... / 86400 AS BIGINT)
    if not isinstance(node, exp.Cast):
        return None

    to_type = node.to
    if not (isinstance(to_type, exp.DataType) and to_type.this == exp.DataType.Type.BIGINT):
        return None

    inner = node.this
    # Pattern: ... / 86400
    if not isinstance(inner, exp.Div):
        return None

    divisor = inner.right
    if not (isinstance(divisor, exp.Literal) and divisor.is_number):
        return None

    # Check if divisor is 86400 (seconds in a day)
    try:
        if int(divisor.this) != 86400:
            return None
    except (ValueError, TypeError):
        return None

    extract_expr = inner.left
    # Pattern: EXTRACT(epoch FROM ...)
    if not isinstance(extract_expr, exp.Extract):
        return None

    if str(extract_expr.this).upper() != "EPOCH":
        return None

    # The expression being extracted from should be a timestamp subtraction
    return extract_expr.expression


def _is_abs_with_subtraction(node: exp.Expression):
    """
    Check if node is ABS(date1 - date2) pattern.
    Returns the subtraction expression if found, None otherwise.
    """
    if isinstance(node, exp.Abs):
        inner = node.this
        if isinstance(inner, exp.Sub):
            return inner
        # Handle ABS((date1 - date2)) with extra parens
        if isinstance(inner, exp.Paren) and isinstance(inner.this, exp.Sub):
            return inner.this
    return None


def _is_numeric_value(node: exp.Expression) -> bool:
    """
    Check if a node represents a numeric value.
    Handles literal numbers, CAST(number AS INT/INTEGER/BIGINT) patterns,
    and query parameters/placeholders (e.g., :days, $1, ?).
    """
    # Direct literal number
    if isinstance(node, exp.Literal) and node.is_number:
        return True

    # Query parameters/placeholders (e.g., :days, $1, ?)
    # These are assumed to be numeric in the context of date comparisons
    if isinstance(node, (exp.Placeholder, exp.Parameter)):
        return True

    # CAST(number AS numeric_type) - e.g., 1000::int
    if isinstance(node, exp.Cast):
        inner = node.this
        if isinstance(inner, exp.Literal) and inner.is_number:
            return True
        # Also accept CAST(placeholder AS numeric_type)
        if isinstance(inner, (exp.Placeholder, exp.Parameter)):
            return True

    return False


def _transform_date_operations(tree: exp.Expression) -> exp.Expression:
    """
    Walk the AST and convert PostgreSQL date operations to Databricks equivalents.

    Handles:
    1. Simple date subtraction: (date1 - date2) <= N
    2. Epoch extraction: CAST(EXTRACT(epoch FROM ts1 - ts2) / 86400 AS BIGINT) <= N
    3. ABS pattern: ABS(date1 - date2) <= N
    """
    def transformer(node):
        # Look for comparisons
        if isinstance(node, (exp.LTE, exp.LT, exp.GTE, exp.GT, exp.EQ, exp.NEQ)):
            left = node.left
            right = node.right

            # Only process if comparing to a number (literal or cast)
            if not _is_numeric_value(right):
                return node

            # Check for ABS(date1 - date2) pattern
            sub_expr = _is_abs_with_subtraction(left)
            if sub_expr:
                left_date, right_date = _extract_date_operands_from_sub(sub_expr)
                if left_date and right_date:
                    datediff = _create_datediff(left_date, right_date)
                    # Wrap DATEDIFF in ABS
                    abs_datediff = exp.Abs(this=datediff)
                    node.set("this", abs_datediff)
                    return node

            # Check for epoch days pattern: CAST(EXTRACT(epoch FROM ...) / 86400 AS BIGINT)
            sub_expr = _is_epoch_days_pattern(left)
            if sub_expr:
                left_date, right_date = _extract_date_operands_from_sub(sub_expr)
                if left_date and right_date:
                    datediff = _create_datediff(left_date, right_date)
                    node.set("this", datediff)
                    return node

            # Check for parenthesized subtraction: (date1 - date2)
            if isinstance(left, exp.Paren) and isinstance(left.this, exp.Sub):
                left_date, right_date = _extract_date_operands_from_sub(left.this)
                if left_date and right_date:
                    datediff = _create_datediff(left_date, right_date)
                    node.set("this", datediff)
                    return node

            # Check for direct subtraction: date1 - date2
            if isinstance(left, exp.Sub):
                left_date, right_date = _extract_date_operands_from_sub(left)
                if left_date and right_date:
                    datediff = _create_datediff(left_date, right_date)
                    node.set("this", datediff)
                    return node

        return node

    return tree.transform(transformer)


def transpile_query(sql: str, source_dialect: str = "postgres", target_dialect: str = "databricks") -> str:
    """
    Transpile a SQL query from one dialect to another.

    Args:
        sql: The SQL query to transpile
        source_dialect: The source SQL dialect (default: "postgres")
        target_dialect: The target SQL dialect (default: "databricks")

    Returns:
        The transpiled SQL query
    """
    try:
        # Parse the SQL
        tree = sqlglot.parse_one(sql, read=source_dialect)

        # Apply custom transformations for PostgreSQL -> Databricks
        if source_dialect == "postgres" and target_dialect == "databricks":
            tree = _transform_date_operations(tree)

        # Generate the target dialect SQL
        transpiled = tree.sql(dialect=target_dialect)
        return transpiled
    except Exception as e:
        raise ValueError(f"Error transpiling query: {e}") from e


def transpile_file(input_path: str, output_path: str = None, source_dialect: str = "postgres", target_dialect: str = "databricks") -> str:
    """
    Transpile SQL from a file.

    Args:
        input_path: Path to the input SQL file
        output_path: Path to save the transpiled SQL (optional)
        source_dialect: The source SQL dialect (default: "postgres")
        target_dialect: The target SQL dialect (default: "databricks")

    Returns:
        The transpiled SQL query
    """
    input_file = Path(input_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    sql = input_file.read_text()
    transpiled = transpile_query(sql, source_dialect, target_dialect)

    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(transpiled)
        print(f"Transpiled SQL saved to: {output_path}")

    return transpiled


def main():
    """
    Example usage of the transpiler.
    """
    # Example PostgreSQL query with epoch extraction pattern
    example_query = """
    WITH seed_a AS (
        SELECT c.concept_id AS src_id
        FROM concept c
        WHERE c.vocabulary_id = 'SNOMED'
          AND c.concept_code = '59621000'
          AND c.invalid_reason IS NULL
    ),
    std_a AS (
        SELECT DISTINCT COALESCE(cr.concept_id_2, s.src_id) AS standard_id
        FROM seed_a s
        LEFT JOIN concept_relationship cr
          ON cr.concept_id_1 = s.src_id
          AND cr.relationship_id = 'Maps to'
          AND cr.invalid_reason IS NULL
    ),
    desc_a AS (
        SELECT DISTINCT ca.descendant_concept_id AS concept_id
        FROM std_a sa
        JOIN concept_ancestor ca ON ca.ancestor_concept_id = sa.standard_id
        JOIN concept c ON c.concept_id = ca.descendant_concept_id
          AND c.standard_concept = 'S'
          AND c.domain_id = 'Condition'
          AND c.invalid_reason IS NULL
    ),
    condition_a_occurrences AS (
        SELECT DISTINCT co.person_id, co.condition_start_date::date AS start_date
        FROM condition_occurrence co
        JOIN desc_a da ON co.condition_concept_id = da.concept_id
    )
    SELECT COUNT(DISTINCT person_id)
    FROM condition_a_occurrences
    WHERE (start_date - '2020-01-01'::date) <= 30;
    """

    print("Original PostgreSQL query:")
    print("-" * 80)
    print(example_query)
    print("\n")

    transpiled = transpile_query(example_query)

    print("Transpiled Databricks Spark SQL:")
    print("-" * 80)
    print(transpiled)
    print("\n")


if __name__ == "__main__":
    main()
