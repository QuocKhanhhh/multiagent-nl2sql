# analytics_api.py
import os
import psycopg2
import re
import time
import logging
from fastapi import FastAPI
from pydantic import BaseModel
import yaml
import json

from .nl2sql_generator import multi_agent_pipeline, query_ollama, preprocess_question, corrector_agent
from .sql_validate import validate_sql

# ====== Setup ======
app = FastAPI()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
)

# Load schema
with open("semantic_model.yaml", "r", encoding="utf-8") as f:
    SCHEMA = yaml.safe_load(f)
SCHEMA_TEXT = "\n".join([
    f"{t['name']}({', '.join([list(c.keys())[0] for c in t['columns']])})"
    for t in SCHEMA["tables"]
])

# DB config
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASS", "postgres"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

class QueryPayload(BaseModel):
    question: str | None = None
    sql: str | None = None

# ====== Helpers ======
def run_sql(sql: str):
    if not sql or not sql.strip().upper().startswith("SELECT"):
        raise ValueError("Invalid query provided. Must be a SELECT statement.")

    logging.info("Executing SQL: %s", sql[:160] + ("..." if len(sql) > 160 else ""))
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description] if cur.description else []
        return {"columns": cols, "rows": rows}
    finally:
        cur.close()
        conn.close()

def extract_sql(text: str) -> str:
    if not text:
        return ""
    t = str(text).strip()
    t = re.sub(r"^```(?:sql[:\w-]*)\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"```$", "", t).strip()
    t = t.strip("` \n\r\t")

    m = re.search(r"(SELECT\b[\s\S]*?;)", t, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    idx = re.search(r"\bSELECT\b", t, re.IGNORECASE)
    if idx:
        return t[idx.start():].strip()
    return ""

def _format_result_for_prompt(result: dict, max_rows: int = 6) -> str:
    cols = result.get('columns', []) or []
    rows = result.get('rows', []) or []
    n_rows = len(rows)

    lines = [f"columns: {', '.join(cols)}", f"rows_count: {n_rows}"]
    if n_rows > 0:
        lines.append("sample_rows:")
        for r in rows[:max_rows]:
            safe_vals = []
            for v in r:
                if v is None:
                    safe_vals.append('')
                else:
                    s = str(v)
                    if len(s) > 100:
                        s = s[:100] + '...'
                    safe_vals.append(s.replace('\n', ' '))
            lines.append(", ".join(safe_vals))
        if n_rows > max_rows:
            lines.append(f"... and {n_rows - max_rows} more rows")
    return "\n".join(lines)

def _extract_text_from_ollama(resp) -> str:
    if isinstance(resp, str):
        return resp
    if isinstance(resp, dict):
        for k in ("output", "response", "text"):
            if k in resp and isinstance(resp[k], str):
                return resp[k]
    return str(resp)

def _contains_raw_sql_or_data(text: str, sql: str, result_repr: str) -> bool:
    if not text:
        return False
    low = text.lower()
    suspects = ['select ', ' from ', ' where ', ' join ', ' columns', "'columns'", 'rows_count', 'rows:', 'dw.']
    if any(s in low for s in suspects):
        return True
    if sql and sql.strip() and sql.strip().lower() in low:
        return True
    if result_repr and result_repr.strip() and result_repr.strip().lower() in low:
        return True
    return False

def create_fallback_response(question: str, columns: list, rows: list) -> str:
    if not rows:
        return "Không tìm thấy dữ liệu phù hợp với câu hỏi của bạn."
    try:
        first_row = rows[0]
        if isinstance(first_row, (list, tuple)):
            main_value = first_row[0]
        elif isinstance(first_row, dict):
            main_value = list(first_row.values())[0]
        else:
            main_value = first_row
        if isinstance(main_value, (int, float)):
            return f"Có {int(main_value)} kết quả phù hợp với câu hỏi của bạn."
        return f"Kết quả chính: {str(main_value)}."
    except Exception:
        return "Kết quả có nhưng không thể diễn giải chi tiết."

def summarize_with_llm(question: str, sql: str, result: dict | None, sql_success: bool):
    if not sql_success:
        return f"Xin lỗi, tôi không thể lấy dữ liệu cho câu hỏi '{question}'."
    if not result or not result.get("rows"):
        return "Không tìm thấy dữ liệu phù hợp."

    safe_result = _format_result_for_prompt(result, max_rows=3)
    prompt = f"""
Bạn là một trợ lý phân tích dữ liệu.
Người dùng hỏi: "{question}"

Kết quả truy vấn tóm tắt:
{safe_result}

Hãy trả lời bằng tiếng Việt, tự nhiên, ngắn gọn (1–2 câu).
- Không lặp lại SQL hoặc từ 'columns', 'rows'.
- Nếu có số liệu, hãy chèn trực tiếp vào câu trả lời.
- Không trả về JSON hoặc dict.
"""
    try:
        # use legacy simple prompt mode: prompt text + model
        raw = query_ollama("mistral:7b", "summarizer", prompt, expect_json=False)

        text = _extract_text_from_ollama(raw).strip()
    except Exception as e:
        logging.error(f"LLM error in summarizer: {e}")
        return create_fallback_response(question, result["columns"], result["rows"])

    if _contains_raw_sql_or_data(text, sql, safe_result):
        return create_fallback_response(question, result["columns"], result["rows"])
    return text

def corrector_agent(sql: str, error: str,
                    schema_text: str, question: str, plan: dict) -> str | dict:
    prompt = f"""
Bạn là chuyên gia sửa SQL PostgreSQL.

Câu hỏi: {question}
Lỗi: {error}
Schema: {schema_text}
Plan: {json.dumps(plan, ensure_ascii=False, indent=2)}

SQL hiện tại:
{sql}

Yêu cầu:
1. Nếu sửa được → trả về 1 câu SQL SELECT hợp lệ, kết thúc bằng dấu chấm phẩy. Không bao quanh bằng ```sql.
2. Nếu không sửa được → trả JSON: {{ "error":"cannot_fix", "reason":"..." }}.
"""
    # use legacy simple prompt mode and expect text back
    resp = query_ollama(prompt, model=os.getenv("SUMMARIZER_MODEL", "mistral:7b"), expect_json=False)
    if isinstance(resp, dict):
        return resp

    txt = str(resp).strip()
    txt = re.sub(r"^```(?:sql)?", "", txt, flags=re.IGNORECASE).strip("`\n ")
    m = re.search(r"(SELECT[\s\S]*?;)", txt, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    if txt.upper().startswith("SELECT"):
        return txt
    return {"error": "cannot_fix", "reason": "LLM did not produce valid SQL"}

# ====== Endpoints ======
@app.post('/ask')
def ask(payload: QueryPayload):
    question = payload.question or ""
    sql = ""
    result = None
    corrections: list = []
    sql_success = False

    if payload.sql:
        sql = extract_sql(payload.sql) or payload.sql.strip()
        try:
            result = run_sql(sql)
            sql_success = True
        except Exception as e:
            logging.exception("Direct SQL execution failed")
            corrections.append(str(e))

    elif question.strip():
        try:
            sql, corr, plan = multi_agent_pipeline(question, schema=SCHEMA)
            corrections.extend(corr)
        except Exception as e:
            logging.exception("SQL generation error")
            corrections.append(f"SQL generation error: {str(e)}")
            sql = ""
            plan = {}

        if sql and sql.strip().upper().startswith("SELECT"):
            valid, errors = validate_sql(sql, SCHEMA)
            if valid:
                try:
                    result = run_sql(sql)
                    sql_success = True
                except Exception as e:
                    corrections.append(str(e))
            else:
                corrections.extend(errors)
                try:
                    fixed = corrector_agent(sql, "; ".join(errors), SCHEMA_TEXT, question, plan)
                    if isinstance(fixed, dict):  # model trả JSON lỗi
                        corrections.append(json.dumps(fixed, ensure_ascii=False))
                    else:
                        fixed_sql = extract_sql(fixed)
                        if fixed_sql and fixed_sql.upper().startswith("SELECT"):
                            sql = fixed_sql
                            result = run_sql(sql)
                            sql_success = True
                        else:
                            corrections.append("Corrector failed to produce valid SQL.")
                except Exception as e:
                    corrections.append(f"Corrector exception: {e}")

    analysis = summarize_with_llm(question or "Câu hỏi mặc định", sql, result, sql_success)
    return {
        "sql": sql,
        "raw_result": result,
        "analysis": analysis,
        "corrections": corrections,
        "sql_success": sql_success,
    }


# def corrector_agent(sql: str, error: str, schema_text: str, question: str, plan: dict) -> Any:
#     prompt = f"""
# Bạn là chuyên gia sửa SQL PostgreSQL.
# Câu hỏi: {question}
# Lỗi: {error}
# Schema: {schema_text}
# Plan: {json.dumps(plan, ensure_ascii=False, indent=2)}

# SQL hiện tại:
# {sql}

# Yêu cầu:
# 1. Nếu sửa được → trả về 1 câu SQL SELECT hợp lệ, kết thúc bằng dấu chấm phẩy. Không bao quanh bằng ```sql.
# 2. Nếu không sửa được → trả JSON: {{ "error": "cannot_fix", "reason": "..." }}.
# """
#     # use simple prompt mode: pass prompt text and request parsed JSON only if the model returns JSON
#     resp = query_ollama(prompt, model="mistral:7b", expect_json=False)
#     # if model returned a JSON-like dict (rare here), return it as-is
#     if isinstance(resp, dict):
#         return resp
#     # else resp is text; extract SQL if possible
#     txt = str(resp).strip()
#     txt = re.sub(r"^```(?:sql)?", "", txt, flags=re.IGNORECASE).strip("`\n ")
#     m = re.search(r"(SELECT[\s\S]*?;)", txt, re.IGNORECASE)
#     if m:
#         return m.group(1).strip()
#     if txt.upper().startswith("SELECT"):
#         return txt
#     # fallback
#     return {"error": "cannot_fix", "reason": "LLM did not produce valid SQL"}

# def preprocess_question(q: str) -> str:
#     return q.strip()