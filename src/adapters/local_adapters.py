import os
import json
import uuid
from datetime import datetime, timezone
import pandas as pd
from src.domain.ports import ObjectReader, LakeWriter, QuarantineWriter, IdempotencyStore, Clock, IdGenerator
from src.utils.logger import get_logger

logger = get_logger("adapters.local")

class LocalObjectReader(ObjectReader):
    def read_csv(self, source_uri: str) -> tuple[pd.DataFrame, dict]:
        size_bytes = os.path.getsize(source_uri) if os.path.exists(source_uri) else 0
        logger.info(f"Reading CSV from {source_uri}", extra={"telemetry": {
            "source_uri": source_uri,
            "byte_size": size_bytes,
            "delimiter": ","
        }})
        df = pd.read_csv(source_uri)
        return df, {"source_uri": source_uri, "byte_size": size_bytes}

class LocalLakeWriter(LakeWriter):
    def __init__(self, base_lake_dir: str = "./data/lake"):
        self.base_lake_dir = base_lake_dir
        os.makedirs(self.base_lake_dir, exist_ok=True)

    def write_parquet(self, df: pd.DataFrame, partition_path: str) -> str:
        full_path = os.path.join(self.base_lake_dir, partition_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        df.to_parquet(full_path, index=False)
        logger.info("Wrote Parquet dataset", extra={"telemetry": {
            "lake_partition_path": full_path,
            "rows_written": len(df)
        }})
        return full_path

class LocalQuarantineWriter(QuarantineWriter):
    def __init__(self, base_quarantine_dir: str = "./data/quarantine"):
        self.base_quarantine_dir = base_quarantine_dir
        os.makedirs(self.base_quarantine_dir, exist_ok=True)

    def write_quarantine(self, df: pd.DataFrame, quarantine_uri: str) -> str:
        full_path = os.path.join(self.base_quarantine_dir, quarantine_uri)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        df.to_parquet(full_path, index=False)
        logger.info("Wrote Quarantine dataset", extra={"telemetry": {
            "quarantine_uri": full_path,
            "rows_quarantined": len(df)
        }})
        return full_path

class LocalIdempotencyStore(IdempotencyStore):
    def __init__(self, ledger_path: str = "./data/ledger.json"):
        self.ledger_path = ledger_path
        os.makedirs(os.path.dirname(ledger_path), exist_ok=True)
        if not os.path.exists(self.ledger_path):
            with open(self.ledger_path, "w") as f:
                json.dump({}, f)

    def _load_ledger(self) -> dict:
        with open(self.ledger_path, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}

    def is_processed(self, run_id: str) -> bool:
        ledger = self._load_ledger()
        return run_id in ledger

    def mark_processed(self, run_id: str, result_summary: dict) -> None:
        ledger = self._load_ledger()
        ledger[run_id] = {
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "summary": result_summary
        }
        with open(self.ledger_path, "w") as f:
            json.dump(ledger, f, indent=2)
        logger.info("Recorded idempotency state", extra={"telemetry": {"run_id": run_id, "status": "processed"}})

class SystemClock(Clock):
    def now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

class UUIDGenerator(IdGenerator):
    def generate_id(self) -> str:
        return str(uuid.uuid4())