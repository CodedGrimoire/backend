from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class DatasetOut(BaseModel):
    id: str
    name: str
    status: str


class DatasetDetail(BaseModel):
    id: str
    name: str
    table: str
    status: str
    row_count: Optional[int] = None


class UploadResponse(BaseModel):
    dataset_id: str
    rows: int


class QueryRequest(BaseModel):
    question: Optional[str] = None
    sql: Optional[str] = None
    mode: str = "auto"  # auto|sql


class ColumnInfo(BaseModel):
    name: str
    type: str


class QueryResponse(BaseModel):
    type: str
    sql: str
    rows: List[Dict[str, Any]]
    columns: List[ColumnInfo]
    row_count: int
    answer: str | None = None


class EditCellRequest(BaseModel):
    id: int
    column: str
    value: Any


class EditCellResponse(BaseModel):
    ok: bool = True


class ActionAddColumn(BaseModel):
    type: str = "add_column"
    name: str
    db_type: str
    fill: Optional[Dict[str, Any]] = None


class ActionWriteCell(BaseModel):
    type: str = "write_cell"
    row_id: int
    column: str
    value: Any


class ActionFillColumn(BaseModel):
    type: str = "fill_column"
    column: str
    expression_sql: str


class ActionAddSummaryRow(BaseModel):
    type: str = "add_summary_row"
    values: Dict[str, Any]


class ActionsRequest(BaseModel):
    message: str


class ActionsResponse(BaseModel):
    actions: List[Dict[str, Any]]
    applied: bool
    notes: str | None = None
    preview: Dict[str, Any] | None = None


class DashboardResponse(BaseModel):
    charts: List[Dict[str, Any]]
    metrics: List[Dict[str, Any]]
    insights: List[Dict[str, Any]]


class DashboardStatusResponse(BaseModel):
    status: str = Field("queued")
