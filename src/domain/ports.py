from typing import Protocol, Tuple, Dict, Any, List
import pandas as pd
from src.domain.models import BatchMetadata, ProcessingResult

class ObjectReader(Protocol):
    def read_csv(self, source_uri: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        ...

class LakeWriter(Protocol):
    def write_parquet(self, df: pd.DataFrame, partition_path: str) -> str:
        ...

class QuarantineWriter(Protocol):
    def write_quarantine(self, df: pd.DataFrame, quarantine_uri: str) -> str:
        ...

class IdempotencyStore(Protocol):
    def is_processed(self, run_id: str) -> bool:
        ...
    def mark_processed(self, run_id: str, result_summary: Dict[str, Any]) -> None:
        ...

class Clock(Protocol):
    def now_iso(self) -> str:
        ...

class IdGenerator(Protocol):
    def generate_id(self) -> str:
        ...
