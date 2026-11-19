import os
import pandas as pd
from src.adapters.local_adapters import (
    LocalObjectReader, LocalLakeWriter, LocalQuarantineWriter,
    LocalIdempotencyStore, SystemClock, UUIDGenerator
)
from src.transform.engine import SchemaGateEngine
from src.orchestration.prefect_flows import IngestionPipelineService, ingest_csv_to_lake_flow

def create_sample_csv(file_path: str = "./data/sample_orders.csv") -> str:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    data = {
        "Record ID ": ["REC-001", "REC-002", "REC-003", "   "],
        "Event Timestamp": ["2026-03-30T10:00:00Z", "2026-03-30T10:05:00Z", "invalid_date", "2026-03-30T10:10:00Z"],
        "Owner": ["alice", "bob", "alice", "charlie"],
        "Order Title": ["Laptop", "Mouse", "Keyboard", "Monitor"],
        "Grand Total": [1200.50, 25.00, "invalid_price", 300.00]
    }
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    return file_path

if __name__ == "__main__":
    csv_path = create_sample_csv()

    # Composition Root (Environment wiring)
    reader = LocalObjectReader()
    lake_writer = LocalLakeWriter()
    quarantine_writer = LocalQuarantineWriter()
    ledger = LocalIdempotencyStore()
    clock = SystemClock()
    id_gen = UUIDGenerator()
    schema_engine = SchemaGateEngine()

    pipeline_service = IngestionPipelineService(
        reader=reader,
        lake_writer=lake_writer,
        quarantine_writer=quarantine_writer,
        ledger=ledger,
        clock=clock,
        id_gen=id_gen,
        schema_engine=schema_engine
    )

    print("--- Executing Prefect Flow Locally ---")
    result = ingest_csv_to_lake_flow(pipeline_service, source_uri=csv_path, owner="alice_team")
    print(f"Pipeline Result: {result}")