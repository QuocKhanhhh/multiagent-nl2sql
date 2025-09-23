import json
import logging
import re
from typing import List, Tuple, Dict, Any

import requests
from requests.exceptions import ReadTimeout, RequestException

logger = logging.getLogger("analytics.nl2sql_generator")

# =========================
# Config
# =========================
OLLAMA_HOST = "http://host.docker.internal:11434"
OLLAMA_TIMEOUT = 60
MAX_RETRIES = 2

def intelligent_join_builder(plan: dict) -> str:
    """Xây dựng các mệnh đề JOIN dựa trên các bảng được sử dụng trong plan."""
    
    # Tập hợp tất cả các cột được đề cập trong plan
    all_columns_text = json.dumps(plan)
    
    used_aliases = set(re.findall(r'\b(fa|da|au|dt|dd)\b', all_columns_text))
    
    if "fa" not in used_aliases or len(used_aliases) <= 1:
        return "" # Không cần JOIN nếu chỉ dùng bảng fact hoặc không dùng bảng nào
        
    joins = []
    
    # Quy tắc JOIN đã được định nghĩa sẵn
    join_map = {
        "da": "INNER JOIN dw.dim_articles da ON fa.article_id = da.article_id",
        "au": "INNER JOIN dw.dim_authors au ON fa.author_id = au.author_id",
        "dt": "INNER JOIN dw.dim_topics dt ON fa.topic_id = dt.topic_id",
        "dd": "INNER JOIN dw.dim_date dd ON fa.date_id = dd.date_id",
    }
    
    for alias in ["dd", "da", "au", "dt"]: # Sắp xếp thứ tự JOIN để logic hơn
        if alias in used_aliases:
            joins.append(join_map[alias])
            
    return "\n".join(joins)

# ----- New helpers: schema index & fuzzy column matcher -----
def build_schema_index(catalog: dict) -> Dict[str, set]:
    """Trả về map: table_name -> set(columns) từ schema catalog."""
    idx = {}
    for t in catalog.get("tables", []):
        cols = set()
        for c in t.get("columns", []):
            if isinstance(c, dict):
                # c is like {col_name: description}
                cols.add(list(c.keys())[0])
            elif isinstance(c, str):
                cols.add(c)
        idx[t["name"]] = cols
    return idx

def normalize_col_name(s: str) -> str:
    """Chuẩn hóa tên cho so sánh: lowercase, chỉ chữ+chữ số."""
    if not s:
        return ""
    return re.sub(r'[^a-z0-9]', '', s.lower())

def find_best_column_match(table: str, requested: str, schema_index: Dict[str, set]) -> str | None:
    """
    Nếu requested exist -> trả về requested.
    Nếu không, thử tìm cột 'gần nhất' theo heuristic (contain / normalized equality).
    """
    if table not in schema_index:
        return None
    cols = schema_index[table]
    if requested in cols:
        return requested

    req_norm = normalize_col_name(requested)
    # exact normalized match
    for c in cols:
        if normalize_col_name(c) == req_norm:
            return c
    # contains (requested contains col_norm) or vice versa
    for c in cols:
        cn = normalize_col_name(c)
        if req_norm in cn or cn in req_norm:
            return c
    # prefix match (e.g., reading_time vs read_time)
    for c in cols:
        if cn := normalize_col_name(c):
            if cn.startswith(req_norm) or req_norm.startswith(cn):
                return c
    return None

# =========================
# Prompt cho các agent
# =========================
# Sửa PROMPT_DECONSTRUCTOR

PROMPT_DECONSTRUCTOR = """
Bạn là Deconstructor Agent. Nhiệm vụ của bạn là phân tích câu hỏi tiếng Việt và chuyển thành logical plan JSON.

YÊU CẦU:
- metric: kiểu phép tính ("count", "avg", "min", "max", "sum"...)
- metric_hint: mô tả metric bằng tiếng Việt
- dimensions: danh sách các cột hợp lệ để group by.
- filters: các điều kiện lọc dữ liệu (WHERE).
- order_by: {"column": "...", "direction": "ASC|DESC"}.
- limit: số nguyên.
- from_tables: LUÔN chứa ["dw.fact_articles"].
- aliases: LUÔN khai báo ít nhất {"fa": "dw.fact_articles"}.
  Nếu dùng cột từ dim_articles, dim_authors, dim_topics, dim_date thì thêm tương ứng:
  {"da": "dw.dim_articles"}, {"au": "dw.dim_authors"}, {"dt": "dw.dim_topics"}, {"dd": "dw.dim_date"}.

QUY TẮC QUAN TRỌNG:
1. Chỉ dùng các bảng/alias: fa, da, au, dt, dd.
2. Với câu hỏi về "cao nhất", "thấp nhất", "nhiều nhất", "ít nhất":
   - BẮT BUỘC dùng order_by + limit: 1.
   - KHÔNG được tạo filter so sánh trực tiếp với giá trị lớn nhất/nhỏ nhất.
3. Nếu liên quan đến "nguồn", dùng da.source_name.
4. Nếu liên quan đến cảm xúc: 
   - "tích cực" → "pos"
   - "tiêu cực" → "neg"
   - "trung lập" → "neu"
   - Nếu câu hỏi yêu cầu "theo từng loại cảm xúc", hãy group by fa.sentiment.
5. Nếu liên quan đến "chủ đề", hãy group by dt.topic_name (không dùng dt.topic_id).
6. Nếu liên quan đến "tác giả", hãy group by au.author_name (không dùng au.author_id).
7. Nếu liên quan đến "nguồn", hãy group by da.source_name (không dùng id).
8. Nếu câu hỏi liên quan đến "thời gian đọc" (trung bình, dài nhất, ngắn nhất, tổng), hãy dùng cột fa.read_time.
9. ❌ Không dùng fa.source_id.

VÍ DỤ:
Câu hỏi: "Nguồn nào có trung bình số từ bài viết thấp nhất?"
Plan JSON:
{
  "from_tables": ["dw.fact_articles"],
  "aliases": {"fa": "dw.fact_articles", "da": "dw.dim_articles"},
  "metric": "avg",
  "metric_hint": "Trung bình số từ theo nguồn",
  "dimensions": ["da.source_name"],
  "filters": [],
  "order_by": {"column": "avg(fa.word_count)", "direction": "ASC"},
  "limit": 1
}
"""


# Sửa PROMPT_PLANNER

PROMPT_PLANNER = """
Bạn là Planner Agent. Nhiệm vụ của bạn là chuyển logical plan JSON thành SQL hợp lệ.

QUY TẮC:
- Chỉ dùng các bảng/alias: fa (dw.fact_articles), da (dw.dim_articles), au (dw.dim_authors), dt (dw.dim_topics), dd (dw.dim_date).
- JOIN đúng khóa ngoại:
  - fa.article_id = da.article_id
  - fa.author_id = au.author_id
  - fa.topic_id = dt.topic_id
  - fa.date_id = dd.date_id
- Dựa vào các cột trong 'dimensions', 'filters', 'order_by' để quyết định JOIN bảng nào.
- Nếu có 'order_by', hãy thêm mệnh đề ORDER BY.
- Nếu có 'limit', hãy thêm mệnh đề LIMIT.
- Nếu có 'dimensions' và 'metric' (count, sum, avg...), hãy dùng GROUP BY cho tất cả các cột trong 'dimensions'.
- KHÔNG bao giờ dùng `fa.source_id`. Nếu plan yêu cầu liên quan đến "nguồn", hãy group by hoặc filter bằng **da.source_name**.
- Chỉ trả về SQL thuần, bắt đầu bằng SELECT, không giải thích.
- KHÔNG tự sinh điều kiện `IS NOT NULL` hoặc cột lọc ngoài những gì có trong plan.filters.

OUTPUT: chỉ SQL statement.
"""

# =========================
# Ollama query wrapper
# =========================
def query_ollama(model: str, role: str, user_input: str, expect_json: bool = True) -> dict | str:
    """Gửi prompt tới Ollama và trả về JSON hoặc text tùy role."""

    if role == "deconstructor":
        system_prompt = PROMPT_DECONSTRUCTOR
    elif role == "planner":
        system_prompt = PROMPT_PLANNER
    else:
        raise ValueError(f"Unknown role {role}")

    payload = {
        "model": model,
        "options": {"temperature": 0.0},
        "prompt": f"{system_prompt.strip()}\n\nCâu hỏi hoặc plan:\n{user_input}\n\nTrả lời:",
        "stream": False,
    }
    url = f"{OLLAMA_HOST}/api/generate"
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, timeout=OLLAMA_TIMEOUT)
            resp.raise_for_status()
            resp_json = resp.json()

            raw_text = resp_json.get("response", "").strip()
            logger.info("Raw Ollama response (%s, attempt %d): %s", role, attempt, raw_text[:500])

            if expect_json:
                # thử parse trực tiếp
                try:
                    return json.loads(raw_text)
                except Exception:
                    # bắt block ```json ... ```
                    m = re.search(r"```json\s*([\s\S]+?)```", raw_text, re.IGNORECASE)
                    if m:
                        candidate = m.group(1).strip()
                        try:
                            return json.loads(candidate)
                        except Exception:
                            logger.warning("Failed to parse fenced JSON block.")
                    # fallback: remove fences thủ công
                    cleaned = re.sub(r"^```(?:json)?", "", raw_text, flags=re.IGNORECASE)
                    cleaned = re.sub(r"```$", "", cleaned).strip()
                    try:
                        return json.loads(cleaned)
                    except Exception:
                        logger.error("JSON parse failed, raw_text sample: %s", raw_text[:300])
                        return {"error": "failed_parse", "raw": raw_text}
            else:
                return raw_text

        except ReadTimeout as e:
            last_err = e
            logger.warning("Ollama %s timeout (attempt %d/%d)", role, attempt, MAX_RETRIES)
        except RequestException as e:
            last_err = e
            logger.error("Ollama %s request error: %s", role, str(e))
            break

    return {"error": "request_failed", "detail": str(last_err)}


# =========================
# Schema Validation Agent
# =========================
def schema_validation_agent(plan: dict, catalog: dict) -> Tuple[bool, List[str]]:
    errors: List[str] = []

    if not plan or not isinstance(plan, dict):
        return False, ["Plan is empty or not dict."]

    valid_tables = {t["name"] for t in catalog.get("tables", [])}
    valid_columns_per_table = {
        t["name"]: {list(c.keys())[0] for c in t.get("columns", []) if isinstance(c, dict)}
        for t in catalog.get("tables", [])
    }

    alias_to_table: Dict[str, str] = {}

    for table_expr in plan.get("from_tables") or []:
        table_name = str(table_expr).strip()
        alias = table_name.split(".")[-1]
        if table_name in valid_tables:
            alias_to_table[alias] = table_name
        else:
            errors.append(f"Hallucinated table '{table_name}' in FROM.")

    for alias, table_expr in (plan.get("aliases") or {}).items():
        table_name = str(table_expr).strip()
        if table_name in valid_tables:
            alias_to_table[alias] = table_name
        else:
            errors.append(f"Hallucinated alias '{alias}' → '{table_name}'")

    def check_expr(expr: str):
        if not expr:
            return
        found = re.findall(r'([a-zA-Z0-9_\.]+)\.([a-zA-Z0-9_]+)', str(expr))
        for alias, column in found:
            if alias in alias_to_table:
                table_name = alias_to_table[alias]
                if column not in valid_columns_per_table.get(table_name, set()):
                    errors.append(f"Invalid column '{column}' in '{table_name}' (alias '{alias}')")
            elif alias in valid_tables:
                if column not in valid_columns_per_table.get(alias, set()):
                    errors.append(f"Invalid column '{column}' in table '{alias}'")
            else:
                errors.append(f"Undefined alias/table '{alias}' for column '{column}'")

    for col in plan.get("select_columns") or []:
        if isinstance(col, dict):
            check_expr(col.get("expr", ""))
        else:
            check_expr(str(col))

    for join_expr in plan.get("joins") or []:
        check_expr(join_expr)
    for where_cond in plan.get("where_conditions") or []:
        check_expr(where_cond)
    for group_expr in plan.get("group_by") or []:
        check_expr(group_expr)

    order_by = plan.get("order_by") or {}
    if isinstance(order_by, list) and order_by:
        order_by = {"column": str(order_by[0]), "direction": "ASC"}
    elif isinstance(order_by, str):
        order_by = {"column": order_by, "direction": "ASC"}
    elif not isinstance(order_by, dict):
        order_by = {}
    if order_by.get("column"):
        check_expr(order_by["column"])

    return (len(errors) == 0), list(set(errors))

# =========================
# Normalize Plan
# =========================
ALIAS_FIX_MAP = {
    "fact_articles": "dw.fact_articles",
    "articles": "dw.dim_articles",
    "authors": "dw.dim_authors",
    "topics": "dw.dim_topics",
    "date": "dw.dim_date",
}

def normalize_plan(plan: dict, valid_tables: set, schema: dict) -> dict:
    """
    Normalize plan using schema (dynamic, not hardcoded).
    - Fix alias table names if user/LLM used shorthand.
    - Replace requested id columns with corresponding *_name when appropriate.
    - Try to auto-correct similar column names using fuzzy match.
    """
    if not plan or not isinstance(plan, dict):
        return plan

    # build schema index (table -> columns set)
    schema_index = build_schema_index(schema)

    # Normalize aliases: map alias -> canonical table name if available
    aliases = plan.get("aliases") or {}
    new_aliases = {}
    for alias, table in aliases.items():
        # try exact table first; if short name like "articles" try to find a schema table that contains it
        if table in valid_tables:
            new_aliases[alias] = table
            continue
        # try to find a schema table whose name endswith the provided token
        token = str(table).split('.')[-1]
        found = None
        for t in schema_index.keys():
            if t.endswith(token) or normalize_col_name(t).endswith(normalize_col_name(token)):
                found = t
                break
        if found:
            new_aliases[alias] = found
        else:
            # keep original (schema validation will catch hallucination later)
            new_aliases[alias] = table
    plan["aliases"] = new_aliases

    # helper to resolve alias.column into best real column
    def resolve_expr(expr: str) -> str:
        if not expr or not isinstance(expr, str):
            return expr
        expr = expr.strip()
        # find alias.column patterns
        def replace_match(m):
            alias, col = m.group(1), m.group(2)
            alias_table = plan.get("aliases", {}).get(alias) or alias  # alias may be table name
            # if alias_table is like 'dw.dim_topics', use as key; else try to find in schema
            table_name = alias_table if alias_table in schema_index else None
            if not table_name:
                # maybe alias is 'dt' and plan.from_tables has a matching full table
                for t in plan.get("from_tables", []):
                    if t.endswith(alias) or normalize_col_name(t).endswith(normalize_col_name(alias)):
                        table_name = t
                        break
            # if still none, fallback to alias text
            table_name = table_name or alias_table

            # 1) if column ends with _id and table_name is a dim table, prefer *_name if exists
            if col.endswith("_id"):
                prefix = col[:-3]  # drop _id
                candidate_name = f"{prefix}_name"
                best = find_best_column_match(table_name, candidate_name, schema_index)
                if best:
                    return f"{alias}.{best}"
            # 2) exact or fuzzy match to any column in that table
            best = find_best_column_match(table_name, col, schema_index)
            if best:
                return f"{alias}.{best}"
            # 3) fallback, keep original
            return f"{alias}.{col}"

        # replace all alias.column occurrences
        out = re.sub(r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)', replace_match, expr)
        # remove empty fragments introduced earlier
        out = out.strip()
        return out

    # Process select_columns (can be dicts or strings)
    sel = []
    for col in (plan.get("select_columns") or []):
        if isinstance(col, dict):
            expr = col.get("expr", "")
            fixed = resolve_expr(expr)
            if fixed:
                sel.append({"expr": fixed, "alias": col.get("alias")})
        else:
            fixed = resolve_expr(col)
            if fixed:
                sel.append(fixed)
    plan["select_columns"] = sel

    # group_by
    fixed_group = []
    for g in (plan.get("group_by") or []):
        resolved = resolve_expr(g)
        if resolved:
            fixed_group.append(resolved)
    plan["group_by"] = fixed_group

    # where_conditions
    fixed_where = []
    for wc in (plan.get("where_conditions") or []):
        resolved = resolve_expr(wc)
        if resolved:
            fixed_where.append(resolved)
    plan["where_conditions"] = fixed_where


    # order_by can be dict or string
    if plan.get("order_by"):
        ob = plan["order_by"]
        if isinstance(ob, dict):
            col = resolve_expr(ob.get("column", ""))
            if col:
                plan["order_by"]["column"] = col
            else:
                plan["order_by"] = {}
        elif isinstance(ob, str):
            plan["order_by"] = {"column": resolve_expr(ob), "direction": "ASC"}

    # from_tables: try to normalize tokens into canonical schema table names if possible
    new_from = []
    for t in plan.get("from_tables") or []:
        if t in schema_index:
            new_from.append(t)
            continue
        token = str(t).split('.')[-1]
        matched = None
        for st in schema_index.keys():
            if st.endswith(token) or normalize_col_name(st).endswith(normalize_col_name(token)):
                matched = st
                break
        new_from.append(matched or t)
    plan["from_tables"] = new_from

    return plan

def postprocess_sql(sql: str) -> str:
    """Sửa các lỗi cú pháp thường gặp trong SQL do planner sinh ra."""
    if not sql or not isinstance(sql, str):
        return sql

    txt = sql.strip()

    # remove code fences
    txt = re.sub(r"^```(?:sql)?", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"```$", "", txt).strip("`\n\r ")

    # remove empty ORDER BY segments like 'ORDER BY ;' or 'ORDER BY'
    txt = re.sub(r'ORDER\s+BY\s*;',';', txt, flags=re.IGNORECASE)
    txt = re.sub(r'ORDER\s+BY\s*(LIMIT|$)', r'\1', txt, flags=re.IGNORECASE)

    # replace placeholders like $dt.topic_id -> dt.topic_id
    txt = re.sub(r'\$([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)', r'\1', txt)

    # ensure SQL ends with semicolon
    if not txt.endswith(';'):
        txt = txt + ';'

    # collapse multiple semicolons
    txt = re.sub(r';{2,}', ';', txt)

    return txt

# =========================
# Agents wrapper
# =========================
def query_deconstructor_agent(question: str) -> dict:
    return query_ollama("mistral:7b", "deconstructor", question, expect_json=True)

def query_planner_agent(plan_json: Any, schema: dict = None) -> str:
    plan_dict = json.loads(plan_json) if isinstance(plan_json, str) else plan_json

    # Tự động tạo phần JOIN
    join_clause = intelligent_join_builder(plan_dict)
    plan_dict["generated_joins"] = join_clause

    plan_text = json.dumps(plan_dict, ensure_ascii=False)
    out = query_ollama(model="mistral:7b", role="planner", user_input=plan_text, expect_json=False)

    if not isinstance(out, str):
        out = str(out)

    txt = out.strip()
    # loại bỏ code fences nếu có
    txt = re.sub(r"^```sql", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"```$", "", txt).strip("`\n\r ")

    # 1) block ```sql ... ```
    m = re.search(r"```sql\s*([\s\S]+?)```", txt, re.IGNORECASE)
    if m:
        sql = m.group(1).strip()
        return postprocess_sql(sql)

    # 2) Tìm SELECT ... ;
    m2 = re.search(r"(SELECT[\s\S]+?;)", txt, re.IGNORECASE)
    if m2:
        return postprocess_sql(m2.group(1).strip())

    # 3) Fallback: nếu có SELECT ở đâu đó, trả từ SELECT đến hết
    idx = txt.upper().find("SELECT")
    if idx != -1:
        sql = txt[idx:].strip()
        return postprocess_sql(sql)

    return postprocess_sql("-- PLAN_VALIDATION_ERROR: planner_failed")


# =========================
# Pipeline
# =========================
def multi_agent_pipeline(question: str, schema: dict = None) -> Tuple[str, List[str], dict]:
    # Step 1: Deconstructor
    decon = query_deconstructor_agent(question)
    if "error" in decon:
        return "-- PLAN_VALIDATION_ERROR: deconstructor_failed", [decon["error"]], None

    # Step 2: Normalize - pass schema along
    if schema:
        decon = normalize_plan(decon, {t["name"] for t in schema.get("tables", [])}, schema)

    # Step 3: Validation
    if schema:
        is_valid, plan_errors = schema_validation_agent(decon, schema)
        if not is_valid:
            return f"-- PLAN_VALIDATION_ERROR: Schema validation failed -> {'; '.join(plan_errors)}", plan_errors, decon

    # Step 4: Planner → SQL (give schema so postprocessing can be smarter if needed)
    sql_out = query_planner_agent(decon, schema=schema)
    if not isinstance(sql_out, str):
        return "-- PLAN_VALIDATION_ERROR: planner_failed", ["planner_failed"], decon

    return sql_out, [], decon


    return sql_out, [], decon

def corrector_agent(sql: str, error: str, schema_text: str, question: str, plan: dict) -> str | dict:
    prompt = f"""
Bạn là chuyên gia sửa SQL PostgreSQL. Nhiệm vụ của bạn là sửa câu SQL bị lỗi dựa trên thông tin được cung cấp.

Câu hỏi của người dùng: "{question}"
Schema các bảng: {schema_text}
Logical Plan (ý định ban đầu): {json.dumps(plan, ensure_ascii=False, indent=2)}

SQL bị lỗi:
{sql}
Lỗi từ database hoặc validator: "{error}"

Phân tích lỗi và yêu cầu sửa:

Đọc kỹ lỗi: Lỗi này có thể là do cột không tồn tại, sai tên bảng, hoặc sai logic GROUP BY.

Đối chiếu với Plan và Schema: Kiểm tra xem SQL có tuân thủ đúng các cột trong schema và ý định trong plan không. Ví dụ: plan yêu cầu 'group by' cột A, nhưng SQL lại thiếu.

Sửa SQL: Viết lại câu lệnh SQL SELECT cho đúng. Đảm bảo nó trả lời được câu hỏi ban đầu.

Chỉ trả về SQL: Kết quả cuối cùng chỉ chứa mã SQL, kết thúc bằng dấu chấm phẩy, không có giải thích hay ```sql.

Ví dụ sửa lỗi 'Non-aggregated select column not in GROUP BY':

Lỗi: Cột 'da.source_name' phải xuất hiện trong mệnh đề GROUP BY hoặc được sử dụng trong một hàm tổng hợp.

Sửa: Thêm 'da.source_name' vào mệnh đề GROUP BY.

Bây giờ, hãy sửa câu SQL trên.
"""
    # use simple prompt mode: pass prompt text and request parsed JSON only if the model returns JSON
    resp = query_ollama(prompt, model="mistral:7b", expect_json=False)
    # if model returned a JSON-like dict (rare here), return it as-is
    if isinstance(resp, dict):
        return resp
    # else resp is text; extract SQL if possible
    txt = str(resp).strip()
    txt = re.sub(r"^```(?:sql)?", "", txt, flags=re.IGNORECASE).strip("`\n ")
    m = re.search(r"(SELECT[\s\S]*?;)", txt, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    if txt.upper().startswith("SELECT"):
        return txt
    # fallback
    return {"error": "cannot_fix", "reason": "LLM did not produce valid SQL"}

def preprocess_question(q: str) -> str:
    return q.strip()