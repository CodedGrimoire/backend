import re
import sqlglot
from sqlglot import exp


def is_safe_sql(sql: str) -> bool:
    """Gatekeep SQL statements before execution.

    Rules
    - Only allow single SELECT / INSERT / UPDATE statements (WITH is allowed).
    - Block DROP/DELETE/TRUNCATE/ALTER anywhere in the statement.
    - Reject multiple statements and comment injections.
    """

    sql_stripped = sql.strip()
    # allow one trailing semicolon
    if sql_stripped.endswith(";"):
        sql_stripped = sql_stripped[:-1].rstrip()

    lowered = sql_stripped.lower()

    if "--" in lowered or "/*" in lowered or ";" in lowered:
        return False

    try:
        parsed_list = sqlglot.parse(sql_stripped, read="postgres")
    except Exception:
        return False

    # must be exactly one statement
    if len(parsed_list) != 1:
        return False

    parsed = parsed_list[0]

    if parsed.find(exp.Drop) or parsed.find(exp.Truncate) or parsed.find(exp.Delete) or parsed.find(exp.Alter):
        return False

    # allow SELECT, INSERT, UPDATE (and CTEs which wrap SELECT)
    allowed_roots = (exp.Select, exp.Insert, exp.Update, exp.With)
    if not isinstance(parsed, allowed_roots):
        return False

    return True


def extract_columns(sql: str) -> list[str]:
    """Parse SQL with sqlglot and return column names (unaliased, without table prefixes)."""
    try:
        parsed = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return []
    cols = {c.name for c in parsed.find_all(exp.Column)}
    return list(cols)


def normalize_column(col: str) -> str:
    col = col.strip().lower()
    # remove alias
    col = re.sub(r"\s+as\s+\w+", "", col)
    # handle count(*)
    if "count(*)" in col:
        return "*"
    # extract inner of function if present
    func_match = re.search(r"\((.*?)\)", col)
    if func_match:
        col = func_match.group(1)
    col = col.strip()
    if "." in col:
        col = col.split(".")[-1]
    return col.strip()


def enforce_limit(sql: str, limit: int = 100) -> str:
    sql = sql.strip()
    # drop a single trailing semicolon before processing/appending LIMIT
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    sql_clean = sql.lower()
    if not is_safe_sql(sql):
        raise ValueError("Unsafe SQL operation detected.")
    # no limit for insert/update
    if sql_clean.startswith(("insert", "update")):
        return sql
    if re.search(r"\blimit\b", sql_clean):
        return sql
    if re.search(r"\b(avg|sum|min|max|count)\b", sql_clean) and "group by" not in sql_clean:
        return sql
    return f"{sql.rstrip()}\nLIMIT {limit}"


def ensure_limit(sql: str, limit: int = 100) -> str:
    return enforce_limit(sql, limit)


def is_safe_expression_sql(expression: str) -> bool:
    lowered = expression.lower()
    if ";" in lowered or "--" in lowered or "/*" in lowered:
        return False
    forbidden = ["drop ", "delete ", "truncate ", "alter "]
    return not any(b in lowered for b in forbidden)


def validate_columns(sql: str, allowed: set[str]) -> bool:
    try:
        cols = extract_columns(sql)
    except Exception:
        return False
    for col in cols:
        if col.lower() == "*":
            continue
        if col.lower() not in allowed:
            return False
    return True
