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
    all_columns_text = json.dumps(plan)
    used_aliases = set(re.findall(r'\b(fa|da|au|dt|dd)\b', all_columns_text))

    if "fa" not in used_aliases or len(used_aliases) <= 1:
        return "FROM dw.fact_articles fa"

    joins = ["FROM dw.fact_articles fa"]
    join_map = {
        "da": "INNER JOIN dw.dim_articles da ON fa.article_id = da.article_id",
        "au": "INNER JOIN dw.dim_authors au ON fa.author_id = au.author_id",
        "dt": "INNER JOIN dw.dim_topics dt ON fa.topic_id = dt.topic_id",
        "dd": "INNER JOIN dw.dim_date dd ON fa.date_id = dd.date_id",
    }

    for alias in ["da", "au", "dt", "dd"]:
        if alias in used_aliases:
            joins.append(join_map[alias])

    return "\n".join(joins)

# ----- New helpers: schema index & fuzzy column matcher -----
def build_schema_index(catalog: dict) -> Dict[str, set]:
    idx = {}
    for t in catalog.get("tables", []):
        # Đảm bảo chỉ lấy tên cột từ key 'name'
        cols = {c['name'] for c in t.get("columns", []) if isinstance(c, dict) and 'name' in c}
        idx[t["name"]] = cols
    return idx

def normalize_col_name(s: str) -> str:
    if not s:
        return ""
    return re.sub(r'[^a-z0-9]', '', s.lower())

def find_best_column_match(table: str, requested: str, schema_index: Dict[str, set]) -> str | None:
    if table not in schema_index:
        return None
    cols = schema_index[table]
    if requested in cols:
        return requested
    req_norm = normalize_col_name(requested)
    for c in cols:
        if normalize_col_name(c) == req_norm:
            return c
    for c in cols:
        cn = normalize_col_name(c)
        if req_norm in cn or cn in req_norm:
            return c
    for c in cols:
        cn = normalize_col_name(c)
        if cn.startswith(req_norm) or req_norm.startswith(cn):
            return c
    return None

def filters_to_sql_where(filters: List[Dict[str, Any]]) -> str:
    """
    Chuyển filters từ object thành chuỗi điều kiện SQL hợp lệ.
    Hỗ trợ toán tử IN với giá trị là list.
    """
    conditions = []
    for f in filters:
        col = f.get("column", "")
        op = f.get("operator", "=").upper()
        val = f.get("value")

        if not col or val is None:
            continue

        if op == "IN" and isinstance(val, list):
            # Xử lý đặc biệt cho toán tử IN với list
            if not val: continue # Bỏ qua nếu list rỗng
            # Chuyển đổi các phần tử trong list thành chuỗi có dấu nháy đơn
            formatted_vals = [f"'{str(v).strip()}'" for v in val]
            conditions.append(f"{col} IN ({', '.join(formatted_vals)})")
        else:
            # Xử lý như cũ cho các trường hợp khác
            if isinstance(val, str):
                val = val.strip()
                if not (val.startswith("'") and val.endswith("'")):
                    val = f"'{val}'"
            else:
                val = str(val)
            conditions.append(f"{col} {op} {val}")

    return " AND ".join(conditions) if conditions else ""

def conditions_to_sql(conditions: List[Dict[str, Any]]) -> str:
    # Đổi tên hàm cũ để tái sử dụng
    return filters_to_sql_where(conditions)

# =========================
# Prompt cho các agent
# =========================
# Sửa PROMPT_DECONSTRUCTOR

# Cập nhật PROMPT_DECONSTRUCTOR
PROMPT_DECONSTRUCTOR = """
Bạn là Deconstructor Agent. Nhiệm vụ của bạn là phân tích câu hỏi tiếng Việt và chuyển thành logical plan JSON.

YÊU CẦU:
- metric: kiểu phép tính ("count", "avg", "min", "max", "sum"...)
- metric_hint: mô tả metric bằng tiếng Việt
- dimensions: danh sách các cột hợp lệ để group by.
- filters: danh sách các điều kiện lọc. Mỗi điều kiện LÀ MỘT OBJECT có dạng {"column": "...", "operator": "=", "value": "..."}.
- order_by: {"column": "...", "direction": "ASC|DESC"}.
- limit: số nguyên.
- from_tables: LUÔN chứa ["dw.fact_articles"].
- aliases: LUÔN khai báo ít nhất {"fa": "dw.fact_articles"}.
  Nếu dùng cột từ dim_articles, dim_authors, dim_topics, dim_date thì thêm tương ứng:
  {"da": "dw.dim_articles"}, {"au": "dw.dim_authors"}, {"dt": "dw.dim_topics"}, {"dd": "dw.dim_date"}.
- filters: danh sách **TẤT CẢ** các điều kiện lọc (WHERE). Mỗi điều kiện là một object.
- having: dùng cho điều kiện trên các cột đã gộp nhóm (HAVING, ví dụ: COUNT(*) > 100).

HƯỚNG DẪN VỊ TRÍ CỘT (QUAN TRỌNG):
- Các cột mô tả nội dung bài viết nằm ở `da` (dim_articles): da.title, da.source_name, da.content.
- Các cột mô tả tác giả nằm ở `au` (dim_authors): au.author_name.
- Các cột mô tả chủ đề nằm ở `dt` (dim_topics): dt.topic_name.
- Các cột mô tả ngày tháng nằm ở `dd` (dim_date): dd.year, dd.month, dd.day.
- Các chỉ số đo lường nằm ở `fa` (fact_articles): fa.word_count, fa.read_time, fa.sentiment.

QUY TẮC QUAN TRỌNG:
1. Chỉ dùng các bảng/alias: fa, da, au, dt, dd.
2. Với câu hỏi về "cao nhất", "thấp nhất", "nhiều nhất", "ít nhất":
   - BẮT BUỘC dùng order_by + limit: 1.
   - KHÔNG được tạo filter so sánh trực tiếp với giá trị lớn nhất/nhỏ nhất.
3. Nếu liên quan đến cảm xúc "tích cực", filter là: [{"column": "fa.sentiment", "operator": "=", "value": "pos"}].
4. Nếu liên quan đến "chủ đề", hãy group by dt.topic_name (không dùng dt.topic_id).
5. Nếu liên quan đến "tác giả", hãy group by au.author_name (không dùng au.author_id).
6. Tuyệt đối KHÔNG thêm bất kỳ điều kiện nào vào "filters" nếu câu hỏi không yêu cầu rõ ràng. Ví dụ: câu hỏi "chủ đề có số từ cao nhất" thì "filters" phải là [].
7. Khi câu hỏi nhắc đến "thời gian đọc", BẮT BUỘC dùng cột `fa.read_time`, không dùng tên nào khác.
8. Nếu liên quan đến cảm xúc "tích cực", filter là: [{"column": "fa.sentiment", "operator": "=", "value": "pos"}].
9.  Gộp TẤT CẢ các điều kiện lọc vào chung một danh sách "filters". Ví dụ: lọc theo sentiment VÀ năm, thì "filters" sẽ là một danh sách chứa hai object.
10. Sử dụng "having" cho các điều kiện lọc sau khi đã GROUP BY. Ví dụ: "chủ đề có ít nhất 100 bài viết" -> `GROUP BY dt.topic_name`, `having: [{"column": "COUNT(fa.article_id)", "operator": ">=", "value": 100}]`.
11. KHÔNG THÊM `filters` NẾU CÂU HỎI KHÔNG YÊU CẦU.
12. Khi hỏi về "cao nhất", "tổng", ... không có nghĩa là phải lọc theo "tích cực". `filters` phải là [].

QUY TẮC VÀNG (BẮT BUỘC TUÂN THỦ):
- 🛑 **SELECT CHỈ CÁC CỘT TRONG `dimensions` VÀ `metric`:** Câu lệnh SELECT chỉ được chứa các cột trong `dimensions` và phép tính `metric`. KHÔNG thêm các cột khác.
- 🛑 **GROUP BY CHỈ CÁC CỘT TRONG `dimensions`:** Mệnh đề GROUP BY phải chứa TẤT CẢ và CHỈ các cột trong `dimensions`.
- 🛑 **ĐỌC KỸ CÂU HỎI ĐỂ XÁC ĐỊNH `dimensions`:** Nếu câu hỏi là "Tác giả nào...", thì `dimensions` phải là `[au.author_name]`. Nếu câu hỏi là "Ngày nào...", thì `dimensions` phải là `[dd.full_date]`.
- 🛑 **Lọc ngày đầy đủ:** Khi câu hỏi có ngày cụ thể (ví dụ: "15/6/2022"), hãy lọc theo cả 3 cột: `dd.day=15`, `dd.month=6`, `dd.year=2022`.
- 🛑 **Hiểu "A so với B":** Khi so sánh (ví dụ: "năm 2019 so với 2020"), hãy dùng toán tử `IN` cho `filters`, ví dụ: `{"column": "dd.year", "operator": "IN", "value": [2019, 2020]}`.
- 🛑 **Hiểu "...nhất":** Khi hỏi "Ai/Cái gì ... nhất" (ví dụ: "tiêu cực nhất"), hãy hiểu là đếm số lượng (`COUNT`) và sắp xếp giảm dần (`DESC`), không phải lấy `MAX` của một cột khác.
- 🛑 **Chọn đúng phép tính:** "Tổng thấp nhất" nghĩa là tính `SUM` rồi `ORDER BY ... ASC`. "Giá trị thấp nhất" mới là dùng `MIN`. Tương tự với "cao nhất".
- 🛑 **Đếm số lượng đối tượng:** Nếu câu hỏi là "Có bao nhiêu tác giả/chủ đề...", `metric` phải là `COUNT(DISTINCT ...)`, và `dimensions` phải để trống `[]`.

VÍ DỤ 1:
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

VÍ DỤ 2:
Câu hỏi: "Có bao nhiêu bài viết về chủ đề 'the-thao'?"
Plan JSON:
{
  "from_tables": ["dw.fact_articles"],
  "aliases": {"fa": "dw.fact_articles", "dt": "dw.dim_topics"},
  "metric": "count",
  "metric_hint": "Số bài viết về thể thao",
  "dimensions": [],
  "filters": [{"column": "dt.topic_name", "operator": "=", "value": "the-thao"}],
  "order_by": {},
  "limit": null
}

VÍ DỤ 3:
Câu hỏi: "Chủ đề nào có hơn 500 bài viết?"
Plan JSON:
{
  "from_tables": ["dw.fact_articles", "dw.dim_topics"],
  "aliases": {"fa": "dw.fact_articles", "dt": "dw.dim_topics"},
  "metric": "count",
  "metric_hint": "Số bài viết theo chủ đề",
  "dimensions": ["dt.topic_name"],
  "filters": [],
  "having": [{"column": "COUNT(fa.article_id)", "operator": ">", "value": 500}],
  "order_by": {},
  "limit": null
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
    valid_roles = {"deconstructor", "planner", "corrector"}
    if role not in valid_roles:
        raise ValueError(f"Unknown role {role}")

    # Chọn prompt dựa role (giữ nguyên hoặc tuỳ chỉnh nếu cần)
    if role == "deconstructor":
        system_prompt = PROMPT_DECONSTRUCTOR
    elif role == "planner":
        system_prompt = PROMPT_PLANNER
    else:
        system_prompt = ""

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
                # Biểu thức chính quy mới: tìm khối JSON nằm giữa ```json và ``` hoặc chỉ ``` và ```
                match = re.search(r"```(?:json)?\s*({[\s\S]*?})\s*```", raw_text)
                candidate = ""
                if match:
                    candidate = match.group(1).strip()
                else:
                    # Fallback: nếu không có ```, thử tìm JSON đầu tiên trong chuỗi
                    start_index = raw_text.find('{')
                    if start_index != -1:
                        # Tìm dấu ngoặc nhọn đóng tương ứng
                        brace_count = 0
                        json_end = -1
                        for i, char in enumerate(raw_text[start_index:]):
                            if char == '{':
                                brace_count += 1
                            elif char == '}':
                                brace_count -= 1
                                if brace_count == 0:
                                    json_end = start_index + i + 1
                                    break
                        if json_end != -1:
                            candidate = raw_text[start_index:json_end]

                if candidate:
                    try:
                        parsed = json.loads(candidate)
                        return parsed
                    except json.JSONDecodeError as e:
                        logger.error("JSON parse failed after cleaning: %s. Raw candidate: %s", str(e), candidate[:300])
                        return {"error": "failed_parse", "raw": raw_text}
                else:
                    logger.error("Could not extract any JSON from raw response. Raw: %s", raw_text[:300])
                    return {"error": "no_json_found", "raw": raw_text}
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

    # Xây dựng index schema một cách chính xác
    valid_columns_per_table = build_schema_index(catalog)
    valid_tables = set(valid_columns_per_table.keys())

    alias_to_table: Dict[str, str] = plan.get("aliases", {})

    # Kiểm tra xem alias có trỏ đến bảng hợp lệ không
    for alias, table_name in alias_to_table.items():
        if table_name not in valid_tables:
            errors.append(f"Alias '{alias}' points to an invalid table '{table_name}'")

    def check_expr(expr: str):
        if not expr: return
        # Tìm các cặp alias.column trong biểu thức
        found = re.findall(r'\b([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b', str(expr))
        for alias, column in found:
            if alias in alias_to_table:
                table_name = alias_to_table[alias]
                if table_name in valid_columns_per_table and column not in valid_columns_per_table[table_name]:
                    errors.append(f"Invalid column '{column}' in '{table_name}' (alias '{alias}')")
            else:
                errors.append(f"Undefined alias '{alias}' used for column '{column}'")

    # Kiểm tra các trường trong plan
    for col in plan.get("dimensions", []):
        check_expr(str(col))

    for f in plan.get("filters", []):
        if isinstance(f, dict):
            check_expr(f.get("column", ""))

    order_by = plan.get("order_by") or {}
    if isinstance(order_by, dict) and order_by.get("column"):
        check_expr(order_by["column"])

    # Kiểm tra metric_col nếu có
    check_expr(plan.get("metric_col"))

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
    if not plan or not isinstance(plan, dict):
        return plan

    schema_index = build_schema_index(schema)

    aliases = plan.get("aliases") or {}
    new_aliases = {}

    # Chuyển alias bảng thành tên bảng chuẩn xác
    for alias, table in aliases.items():
        if table in valid_tables:
            new_aliases[alias] = table
        else:
            token = str(table).split('.')[-1]
            matched = None
            for t in schema_index.keys():
                if t.endswith(token) or normalize_col_name(t).endswith(normalize_col_name(token)):
                    matched = t
                    break
            new_aliases[alias] = matched or table
    plan["aliases"] = new_aliases

    # Ánh xạ metric_hint sang cột thực tế
    metric_hint = plan.get("metric_hint", "").lower()
    if "số từ" in metric_hint or "word_count" in metric_hint:
        plan["metric_col"] = "fa.word_count"
    elif "thời gian đọc" in metric_hint or "read_time" in metric_hint:
        plan["metric_col"] = "fa.read_time"
    else:
        plan["metric_col"] = None

    # Chỉnh sửa order_by column nếu có metric_col + metric aggregation
    if plan.get("order_by") and plan["order_by"]:
        ob = plan["order_by"]
        col = ob.get("column")
        metric = plan.get("metric")
        if col and metric and metric in ("sum", "avg", "max", "min") and plan.get("metric_col"):
            plan["order_by"]["column"] = f"{metric.upper()}({plan['metric_col']})"

    # Chuyển filters thành điều kiện WHERE hợp lệ
    filters_raw = plan.get("filters", [])
    if filters_raw and isinstance(filters_raw, list) and filters_raw and isinstance(filters_raw[0], dict):
        where_clause = filters_to_sql_where(filters_raw)
        plan["where_conditions"] = [where_clause] if where_clause else []
    else:
        plan["where_conditions"] = filters_raw

    # Chuẩn alias.column hợp lệ trong các trường: select_columns, group_by, where_conditions, having, order_by...
    def resolve_expr(expr: str) -> str:
        if not expr or not isinstance(expr, str):
            return expr
        expr = expr.strip()
        def replace_match(m):
            alias, col = m.group(1), m.group(2)
            alias_table = plan.get("aliases", {}).get(alias) or alias
            table_name = alias_table if alias_table in schema_index else None
            if not table_name:
                for t in plan.get("from_tables", []):
                    if t.endswith(alias) or normalize_col_name(t).endswith(normalize_col_name(alias)):
                        table_name = t
                        break
            table_name = table_name or alias_table

            # Thay thế _id bằng _name nếu phù hợp
            if col.endswith("_id"):
                prefix = col[:-3]
                candidate_name = f"{prefix}_name"
                best = find_best_column_match(table_name, candidate_name, schema_index)
                if best:
                    return f"{alias}.{best}"
            best = find_best_column_match(table_name, col, schema_index)
            if best:
                return f"{alias}.{best}"
            return f"{alias}.{col}"

        out = re.sub(r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)', replace_match, expr)
        return out.strip()

    # Chuẩn hóa các trường trong plan
    if "select_columns" in plan:
        sel = []
        for col in plan["select_columns"]:
            if isinstance(col, dict):
                expr = col.get("expr", "")
                fixed = resolve_expr(expr)
                sel.append({"expr": fixed, "alias": col.get("alias")})
            else:
                fixed = resolve_expr(col)
                sel.append(fixed)
        plan["select_columns"] = sel

    plan["group_by"] = [resolve_expr(g) for g in (plan.get("group_by") or [])]
    plan["where_conditions"] = [resolve_expr(w) for w in (plan.get("where_conditions") or [])]
    plan["having"] = [resolve_expr(h) for h in (plan.get("having") or [])]

    if plan.get("order_by"):
        ob = plan["order_by"]
        if isinstance(ob, dict):
            col = resolve_expr(ob.get("column", ""))
            if col:
                plan["order_by"]["column"] = col
            else:
                plan["order_by"] = {}

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
    if not sql or not isinstance(sql, str):
        return sql
    txt = sql.strip()
    txt = re.sub(r"```$", "", txt).strip("`\n\r ")
    txt = re.sub(r'ORDER\s+BY\s*;', ';', txt, flags=re.IGNORECASE)
    txt = re.sub(r'ORDER\s+BY\s*(LIMIT|$)', r'\1', txt, flags=re.IGNORECASE)
    txt = re.sub(r'\$([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)', r'\1', txt)
    txt = re.sub(r';{2,}', ';', txt)
    if not txt.endswith(';'):
        txt += ';'
    return txt

# =========================
# Agents wrapper
# =========================
def query_deconstructor_agent(question: str) -> dict:
    return query_ollama("mistral:7b", "deconstructor", question, expect_json=True)

def query_planner_agent(plan_json: Any, schema: dict = None) -> str:
    plan_dict = json.loads(plan_json) if isinstance(plan_json, str) else plan_json
    join_clause = intelligent_join_builder(plan_dict)

    select_clause = []
    metric = plan_dict.get("metric")
    metric_col = plan_dict.get("metric_col")
    dimensions = plan_dict.get("dimensions", [])

    # Luôn thêm dimensions vào SELECT nếu có
    if dimensions:
        select_clause.extend(dimensions)

    # Thêm metric vào SELECT
    if metric == "count":
        # Nếu chỉ có metric count, không có dimension, thì chỉ cần COUNT(*)
        if not dimensions:
            select_clause = ["COUNT(*) AS count_result"]
        else: # Nếu có dimension, thêm COUNT(*) bên cạnh
            select_clause.append("COUNT(*) AS count_result")
    elif metric and metric_col:
        select_clause.append(f"{metric.upper()}({metric_col}) AS {metric}_result")

    # Fallback: nếu select clause vẫn rỗng, mặc định là COUNT(*)
    if not select_clause:
        select_clause.append("COUNT(*) AS count_result")

    # Ghép lại thành câu SQL
    sql = f"SELECT {', '.join(select_clause)}\n{join_clause}"

    filters = plan_dict.get("where_conditions", [])
    # Đảm bảo filters không rỗng và phần tử đầu tiên không rỗng
    if filters and filters[0]:
        sql += f"\nWHERE {filters[0]}"

    if dimensions:
        sql += f"\nGROUP BY {', '.join(dimensions)}"
        
    having_conditions_raw = plan_dict.get("having", [])    
    if having_conditions_raw:
        having_clause = conditions_to_sql(having_conditions_raw)
        if having_clause:
            sql += f"\nHAVING {having_clause}"

    if plan_dict.get("order_by"):
        ob = plan_dict["order_by"]
        if isinstance(ob, dict) and ob.get("column"):
            sql += f"\nORDER BY {ob['column']} {ob.get('direction', 'ASC')}"

    if plan_dict.get("limit"):
        sql += f"\nLIMIT {plan_dict['limit']}"

    return postprocess_sql(sql)

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