# API Reference

Base URL: `/api/v1`

Authentication: Firebase bearer token when `DEV_MODE=False`; skipped when `DEV_MODE=True`.

## Datasets

### POST `/datasets/upload`
Upload a spreadsheet file. Accepts `multipart/form-data` with `file` and optional `sheet_name`.

**Response**
```json
{ "dataset_id": "uuid", "rows": 123 }
```

### POST `/datasets/dev-seed`
Seeds a demo dataset for the current user. Returns existing one if already present.

**Response**
```json
{ "dataset_id": "uuid", "rows": 12 }
```

### GET `/datasets`
List datasets for the authenticated user.

### GET `/datasets/{dataset_id}`
Get dataset metadata (name, table name, status, row_count).

## Querying

### POST `/datasets/{dataset_id}/query`
Execute SQL or NL question against the dataset (joins allowed across public schema).

**Body**
```json
{
  "question": "total revenue last month",
  "sql": null,
  "mode": "auto"   // auto|sql (reserved)
}
```
`question` or `sql` is required.

**Behavior**
- NL question → LLM generates SQL with full schema context.
- SQL safety: blocks DROP/DELETE/TRUNCATE/ALTER/UPDATE/INSERT, single statement, validates columns and tables against live schema; strips hallucinated joins if columns belong to one table; regenerates once if invalid.
- Auto LIMIT unless aggregation.
- On execution error, one repair attempt via LLM with error context.
- Results classified as `scalar | table | ranking | aggregation`.
- Column metadata taken from the DB cursor (actual result set).
- Result answer generated *after* execution using rows only.
- In-memory cache key: `normalized_question + schema_hash`.
- String filters are case-insensitive and flexible (`ILIKE '%value%'`).
- Dataset metadata (metrics/dimensions/time columns/indexed columns) is injected into LLM context for better SQL.
- Intent layer: NL question → structured intent (metric/aggregation/group/filter/order/limit) → SQL builder → validation.

**Response (standardized)**
```json
{
  "type": "scalar",
  "sql": "select avg(revenue) as avg_revenue from \"42\"",
  "rows": [{"avg_revenue": 18083.33}],
  "columns": [
    {"name": "avg_revenue", "type": "numeric"}
  ],
  "row_count": 1,
  "answer": "The average revenue is 18,083.33."
}
```

**Types**
- `scalar`: one row, one column
- `table`: default multi-row/column result
- `ranking`: ordered result (`ORDER BY` present)
- `aggregation`: grouped data (`GROUP BY` present)

### GET `/datasets/{dataset_id}/rows`
Paginated dataset browser with sorting and filtering.

Query params: `page`, `limit`, `sort_by`, `sort_order`, `filter_column`, `filter_value`

Returns: `columns`, `rows`, `page`, `limit`, `total_rows`

## Dashboard

### POST `/datasets/{dataset_id}/dashboard/rebuild`
Queues dashboard stub rebuild.

### GET `/datasets/{dataset_id}/dashboard`
Fetches charts/metrics/insights snapshot.

## Actions (experimental)

### POST `/datasets/{dataset_id}/actions`
LLM-generated spreadsheet-style actions (add/fill columns, write cells, add summary row). Validates expressions for safety.

**Response**
```json
{
  "actions": [...],
  "applied": true,
  "notes": "Applied",
  "preview": null
}
```
