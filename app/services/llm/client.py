import json
import os
from typing import Any, Dict, List

import httpx
from groq import Groq

from app.services.sql.safety import ensure_limit
from app.services.datasets.schema_context import build_schema_context
from app.core.config import settings
from app.services.sql.safety import is_safe_sql


class LLMClient:
    def __init__(self):
        self.provider = os.getenv("LLM_PROVIDER", "mock").lower()
        self.groq_key = os.getenv("GROQ_API_KEY")
        self.gemini_key = os.getenv("GEMINI_API_KEY")
        self.groq_model = os.getenv("GROQ_MODEL", "llama-3.1-70b-versatile")
        self.gemini_model = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

    def _schema_to_str(self, schema: Dict[str, Any]) -> str:
        cols = ", ".join([f'{c["name"]} ({c["type"]})' for c in schema["columns"]])
        return f'Table {schema["table"]} with columns: {cols}'

    async def generate_sql(self, session, dataset_id, question: str, schema_dict: Dict[str, Any]) -> str:
        table = schema_dict["table"]
        if self.provider == "mock":
            return ensure_limit(f'SELECT * FROM "{table}"', 10)
        schema_ctx = await build_schema_context(session, dataset_id)
        prompt = f"""You are an expert data analyst writing SQL queries.
You are working with a spreadsheet dataset stored in PostgreSQL.

SCHEMA
{schema_ctx}

USER QUESTION
{question}

RULES

1. Only generate a valid PostgreSQL SELECT query.
2. Use ONLY columns that exist in the schema.
3. Use the table name exactly as given.
4. Do not generate explanations.
5. Do not generate markdown.
6. Always include LIMIT 100 unless using aggregation.
7. If the question asks for averages, totals, counts, or comparisons, use SQL aggregate functions.
8. Never use SELECT * unless explicitly requested.

Return ONLY the SQL query."""
        text = await self._call_model(prompt, expect_json=False)
        return text.strip() if text else ensure_limit(f'SELECT * FROM "{table}"', 10)

    async def generate_actions(self, session, dataset_id, message: str, schema_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        if self.provider == "mock":
            return []
        schema_ctx = await build_schema_context(session, dataset_id)
        prompt = (
            "You are a data analyst working with a spreadsheet dataset.\n"
            "You must return ONLY JSON array named actions, matching these types:\n"
            "1) {\"type\":\"add_column\",\"name\":\"col\",\"db_type\":\"TEXT\",\"fill\":{\"formula_sql\":\"...\"}}\n"
            "2) {\"type\":\"write_cell\",\"row_id\":1,\"column\":\"col\",\"value\":123}\n"
            "3) {\"type\":\"fill_column\",\"column\":\"col\",\"expression_sql\":\"col1+col2\"}\n"
            "4) {\"type\":\"add_summary_row\",\"values\":{\"col\":123}}\n"
            "Rules: no DML besides these; expressions must be SELECT-safe; no semicolons.\n"
            f"Schema:\n{schema_ctx}\n"
            f"User message: {message}\n"
            "Return JSON only."
        )
        text = await self._call_model(prompt, expect_json=True)
        if not text:
            return []
        try:
            data = json.loads(text)
            if isinstance(data, dict) and "actions" in data:
                return data["actions"]
            if isinstance(data, list):
                return data
            return []
        except json.JSONDecodeError:
            return []

    async def _call_model(self, prompt: str, expect_json: bool) -> str | None:
        if self.provider == "groq" and self.groq_key:
            return await self._call_groq(prompt, expect_json)
        if self.provider == "gemini" and self.gemini_key:
            return await self._call_gemini(prompt, expect_json)
        return None

    async def _call_groq(self, prompt: str, expect_json: bool) -> str | None:
        headers = {
            "Authorization": f"Bearer {self.groq_key}",
            "Content-Type": "application/json",
        }
        body = {
            "model": self.groq_model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
        }
        async with httpx.AsyncClient(timeout=30, base_url="https://api.groq.com") as client:
            resp = await client.post("/openai/v1/chat/completions", json=body)
            if resp.status_code != 200:
                return None
            data = resp.json()
            return data["choices"][0]["message"]["content"]

    async def _call_gemini(self, prompt: str, expect_json: bool) -> str | None:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.gemini_model}:generateContent"
        params = {"key": self.gemini_key}
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0, "responseMimeType": "application/json" if expect_json else "text/plain"},
        }
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, params=params, json=payload)
            if resp.status_code != 200:
                return None
            data = resp.json()
            try:
                return data["candidates"][0]["content"]["parts"][0]["text"]
            except Exception:
                return None


llm_client = LLMClient()

# Simple Groq-backed SQL generator for NL questions
groq_client = Groq(api_key=settings.groq_api_key) if settings.groq_api_key else None


async def generate_sql(schema_context: str, question: str) -> str:
    print("LLM generating SQL for:", question)
    if not groq_client:
        raise RuntimeError("Groq client not configured")
    prompt = f"""You are an expert data analyst writing PostgreSQL queries.

You are working with a spreadsheet-style dataset stored in PostgreSQL.

Database schema:
{schema_context}

User request:
{question}

Instructions:
1. Generate a valid PostgreSQL query that answers the user's request.
2. Only use columns that exist in the dataset schema.
3. Use the appropriate tables from the schema when answering the query.
4. Never assume columns that do not exist.
5. Prefer explicit column selection instead of SELECT * unless the user explicitly requests all data.
6. You may query multiple tables and use JOIN when necessary.
7. Use table aliases when joining.
8. Infer relationships using common foreign keys such as:
   orders.customer_id → customers.id
   order_items.order_id → orders.id
   order_items.product_id → products.id
   payments.order_id → orders.id

Safety rules:
* NEVER generate DROP statements.
* NEVER generate DELETE statements.
* Never remove tables or rows from the dataset.

Query quality rules:
* Use aggregation functions when appropriate (SUM, AVG, COUNT, MIN, MAX).
* Provide clear aliases for aggregated values.
* Use GROUP BY when aggregating by category.
* Use ORDER BY when sorting results.
* Use LIMIT when returning ranked or large result sets.

Edge case handling:
If the question cannot be answered using the available columns in the schema, return:
-- unsupported_query

Output rules:
Return ONLY the SQL query.
Do not include explanations.
Do not include markdown.
Do not include multiple SQL statements.
Do NOT include markdown code fences or ```sql tags. Output must be a single executable SQL statement."""
    response = groq_client.chat.completions.create(
        model=settings.groq_model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    sql = response.choices[0].message.content.strip()
    return sql


async def generate_answer(question: str, sql: str, rows) -> str:
    """Generate a concise NL answer using ONLY executed query results."""
    print("LLM generating NL answer for:", question)
    if not groq_client:
        return ""
    prompt = f"""You are a data assistant.

A SQL query has already been executed.
Do NOT generate SQL. Use ONLY the rows provided.

User question:
{question}

SQL executed:
{sql}

Query result rows:
{rows}

Rules:
- If the result has exactly one value, respond with that value in a direct sentence.
- If there are multiple rows, summarize the findings briefly.
- Never invent data or run new queries."""
    resp = groq_client.chat.completions.create(
        model=settings.groq_model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    return resp.choices[0].message.content.strip()


async def repair_sql(sql: str, error: str) -> str | None:
    """Ask the LLM to repair a failed SQL query."""
    if not groq_client:
        return None
    prompt = f"""The following SQL query failed to execute. Fix the SQL and return ONLY the corrected query.

Original SQL:
{sql}

Error message:
{error}

Return only valid PostgreSQL SQL. Do not include explanations or markdown."""
    resp = groq_client.chat.completions.create(
        model=settings.groq_model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    fixed = resp.choices[0].message.content.strip()
    return fixed if fixed and is_safe_sql(fixed) else None
