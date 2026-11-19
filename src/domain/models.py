from dataclasses import dataclass, field
from typing import Any, Dict

@dataclass(frozen=True)
class BatchMetadata:
    run_id: str
    trace_id: str
    source_uri: str
    owner: str
    event_timestamp: str
    extra_attributes: Dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class ProcessingResult:
    run_id: str
    rows_in: int
    rows_valid: int
    rows_quarantined: int
    lake_path: str
    quarantine_path: str
    status: str
    idempotency_status: str