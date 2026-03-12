# AI Spreadsheet Backend

FastAPI service that turns natural language questions into safe SQL, runs them on user datasets, and returns UI-ready answers.

## Features
- NL → SQL with schema-aware LLM prompts (joins supported).
- SQL safety guard (sqlglot parse, blocks DROP/DELETE/TRUNCATE/ALTER, single-statement only).
- Automatic SQL repair on execution failure (one retry).
- Result typing (`scalar`, `table`, `ranking`, `aggregation`) plus accurate column metadata from cursor.
- NL answer generation after execution only.
- In-memory query cache keyed by normalized question + live schema hash.
- Standardized response shape for frontend:
  `{type, sql, rows, columns, row_count, answer}`.
- Hybrid suggestions (rule-based + LLM, deduped, capped at 3, never leaks table/IDs).
- Case-insensitive, flexible text filters; string comparisons auto-ILIKE; validation blocks unknown tables/columns.
- Schema-aware SQL guardrail: validates tables/columns, blocks unsafe ops, strips hallucinated joins, regenerates once if invalid.
- Dataset ingestion optimization: column profiling, role detection (metrics/dimensions/time/identifiers), automatic indexes on dimensions/time; semantic layer (metric aggregations, filter values, synonyms) cached for LLM context.
- Paginated dataset browser with sorting/filtering; rows endpoint supports sort/filter/limit/offset.
- Intent-based NL→SQL: questions -> structured intent (metric/aggregation/group/filter/order/limit) -> SQL builder -> validated execution, reducing hallucinations.

## Prerequisites
- Python 3.12+
- PostgreSQL (or Docker)

## Quick start
```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# env
cat > .env <<'EOF'
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:55432/ss
FIREBASE_PROJECT_ID=your-project-id
LOG_LEVEL=INFO
DEV_MODE=True
GROQ_API_KEY=sk-...
GROQ_MODEL=llama-3.3-70b-versatile
EOF

# DB (docker)
docker-compose up -d db

# Migrations (metadata tables only)
alembic upgrade head

# Run API
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Important runtime notes
- DEV_MODE=True bypasses Firebase auth; set False to enforce tokens.
- Query cache is in-memory and per-process; restart clears it. Future: swap to Redis.
- LIMIT is enforced automatically unless aggregation requires full set.
- Column validation uses live `information_schema` to allow multi-table joins safely.

## Development tips
- Common entrypoint: `POST /api/v1/datasets/{dataset_id}/query` (see `docs/api.md`).
- LLM provider defaults to mock; set `LLM_PROVIDER=groq` and keys to enable real calls.
- For quick sample data: `POST /api/v1/datasets/dev-seed`.

## Requirements (if not using requirements.txt)
```
fastapi==0.110.*
uvicorn[standard]==0.27.*
sqlalchemy[asyncio]==2.0.*
asyncpg==0.29.*
pydantic==2.*
pydantic-settings==2.*
alembic==1.13.*
pandas==2.2.*
openpyxl==3.1.*
httpx==0.27.*
firebase-admin==6.*   # optional for token verification
sqlglot==23.*
groq==0.11.*
```

### Auth note
Auth is temporarily disabled in DEV_MODE. Set DEV_MODE=False later to re-enable Firebase authentication.
