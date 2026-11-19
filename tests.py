import os
import shutil
import pandas as pd
from src.adapters.local_adapters import (
    LocalObjectReader, LocalLakeWriter, LocalQuarantineWriter,
    LocalIdempotencyStore, SystemClock, UUIDGenerator
)
from src.transform.engine import SchemaGateEngine
from src.orchestration.prefect_flows import IngestionPipelineService

def setup_test_environment():
    test_dir = "./test_data_sandbox"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)
    return test_dir

def test_pipeline_happy_and_quarantine_path():
    print("Running test_pipeline_happy_and_quarantine_path...")
    sandbox = setup_test_environment()
    csv_path = os.path.join(sandbox, "test_input.csv")

    # CSV with 2 valid rows and 2 invalid rows (null ID, invalid price)
    df_raw = pd.DataFrame({
        "Record ID": ["ID-1", "ID-2", "", "ID-4"],
        "Event Timestamp": ["2026-03-30", "2026-03-30", "2026-03-30", "2026-03-30"],
        "Owner": ["team_a", "team_a", "team_a", "team_a"],
        "Order Title": ["Book", "Pen", "Eraser", "Notebook"],
        "Grand_Total": [15.99, 2.50, 5.00, "not_a_float"]
    })
    df_raw.to_csv(csv_path, index=False)

    reader = LocalObjectReader()
    lake_writer = LocalLakeWriter(base_lake_dir=os.path.join(sandbox, "lake"))
    quarantine_writer = LocalQuarantineWriter(base_quarantine_dir=os.path.join(sandbox, "quarantine"))
    ledger = LocalIdempotencyStore(ledger_path=os.path.join(sandbox, "ledger.json"))
    clock = SystemClock()
    id_gen = UUIDGenerator()
    schema_engine = SchemaGateEngine()

    service = IngestionPipelineService(
        reader=reader,
        lake_writer=lake_writer,
        quarantine_writer=quarantine_writer,
        ledger=ledger,
        clock=clock,
        id_gen=id_gen,
        schema_engine=schema_engine
    )

    run_id = "run-test-123"
    result = service.execute_pipeline(source_uri=csv_path, owner="team_a", run_id=run_id)
    from pprint import pprint
    pprint(result)
    # Assertions
    assert result.status == "SUCCESS", f"Expected SUCCESS, got {result.status}"
    assert result.rows_in == 4, f"Expected 4 rows in, got {result.rows_in}"
    print("print 'result.rows_valid': -----> ", result.rows_valid)
    assert result.rows_valid == 2, f"Expected 2 valid rows, got {result.rows_valid}"
    assert result.rows_quarantined == 2, f"Expected 2 quarantined rows, got {result.rows_quarantined}"
    assert os.path.exists(result.lake_path), "Lake Parquet file should exist"
    assert os.path.exists(result.quarantine_path), "Quarantine Parquet file should exist"

    # Test Idempotency / Re-run Safety
    result_rerun = service.execute_pipeline(source_uri=csv_path, owner="team_a", run_id=run_id)
    assert result_rerun.status == "SKIPPED_DUPLICATE", "Re-run with same run_id must be skipped as duplicate"
    assert result_rerun.idempotency_status == "skipped_duplicate"

    shutil.rmtree(sandbox)
    print("test_pipeline_happy_and_quarantine_path PASSED.")

if __name__ == "__main__":
    print("=== STARTING TEST SUITE ===")
    test_pipeline_happy_and_quarantine_path()
    print("=== ALL TESTS PASSED SUCCESSFULLY ===")