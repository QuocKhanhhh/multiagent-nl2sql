
import re
import sqlparse
from typing import Dict, List, Tuple

UNSUPPORTED_FUNCTION_PATTERNS = [
    r"\bWITH\s+TIES\b",
    r"\bDAYOFWEEK\s*\(",
    r"\bWEEKDAY\s*\(",
    r"\bTIMESTAMPDIFF\s*\(",
    r"\bMATCH\s*\(",
    r"\bSELECT\s+TOP\b",
    r"\bGETDATE\s*\(\)",
]

def extract_table_column_pairs(sql: str) -> List[tuple]:
    return re.findall(r'([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)', sql)

def split_statements(sql: str) -> List[str]:
    parts = sqlparse.split(sql)
    return [p.strip() for p in parts if p and p.strip()]

def validate_sql(sql: str, catalog: Dict) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    if not sql or not sql.strip():
        return False, ["SQL is empty"]

    statements = split_statements(sql)
    if len(statements) > 1:
        errors.append("Multiple SQL statements detected; only a single SELECT is allowed.")

    stmt = statements[0]

    if not stmt.strip().upper().startswith("SELECT"):
        errors.append("Only SELECT statements are allowed.")

    valid_tables = {t["name"].lower() for t in catalog.get("tables", [])}
    valid_columns = {}
    for t in catalog.get("tables", []):
        name = t["name"].lower()
        cols = set()
        for c in t.get("columns", []):
            if isinstance(c, dict):
                cols.add(list(c.keys())[0].lower())
            else:
                cols.add(str(c).lower())
        valid_columns[name] = cols

    lower_stmt = stmt.lower()
    for pat in UNSUPPORTED_FUNCTION_PATTERNS:
        if re.search(pat, lower_stmt, re.IGNORECASE):
            errors.append(f"Unsupported or DB-specific function/pattern detected: {pat}")

    if len(re.findall(r"\blimit\b", lower_stmt)) > 1:
        errors.append("Multiple LIMIT clauses detected.")

    pairs = extract_table_column_pairs(stmt)
    for tbl, col in pairs:
        tl = tbl.lower()
        cl = col.lower()
        if tl in valid_tables:
            if cl not in valid_columns.get(tl, set()):
                errors.append(f"Unknown column '{col}' on table '{tbl}'")
        else:
            # allow alias usage; not strictly an error here
            pass

    sel_match = re.search(r"select\s+(.*?)\s+from\s", stmt, re.IGNORECASE | re.DOTALL)
    if sel_match:
        select_list = sel_match.group(1)
        items = [s.strip() for s in re.split(r',(?![^(]*\))', select_list)]
        group_by_match = re.search(r"group\s+by\s+(.*?)(?:order\s+by|limit|$)", stmt, re.IGNORECASE | re.DOTALL)
        group_items = []
        if group_by_match:
            group_items = [g.strip() for g in group_by_match.group(1).split(",")]

        non_agg_items = []
        for it in items:
            # remove aliases " as x"
            t = re.sub(r"\s+as\s+[\w\"]+$", "", it, flags=re.IGNORECASE).strip()
            if not re.search(r"\b(sum|avg|count|min|max)\s*\(", t, re.IGNORECASE):
                non_agg_items.append(t)
        if non_agg_items:
            if not group_items:
                errors.append(f"Non-aggregated select columns {non_agg_items} but GROUP BY missing.")
            else:
                for n in non_agg_items:
                    matched = any(n == g or n in g or g in n for g in group_items)
                    if not matched:
                        errors.append(f"Non-aggregated select '{n}' not present in GROUP BY {group_items}.")

    is_valid = not errors
    return is_valid, errors
