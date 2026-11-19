import time
from typing import Optional
from prefect import flow, task
from src.domain.models import ProcessingResult
from src.transform.engine import SchemaGateEngine
from src.domain.ports import ObjectReader, LakeWriter, QuarantineWriter, IdempotencyStore, Clock, IdGenerator
from src.utils.logger import get_logger, run_id_var, trace_id_var, orchestrator_var, step_name_var, owner_var

logger = get_logger("orchestration.prefect")

class IngestionPipelineService:
    def __init__(
        self,
        reader: ObjectReader,
        lake_writer: LakeWriter,
        quarantine_writer: QuarantineWriter,
        ledger: IdempotencyStore,
        clock: Clock,
        id_gen: IdGenerator,
        schema_engine: SchemaGateEngine
    ):
        self.reader = reader
        self.lake_writer = lake_writer
        self.quarantine_writer = quarantine_writer
        self.ledger = ledger
        self.clock = clock
        self.id_gen = id_gen
        self.schema_engine = schema_engine

    def execute_pipeline(self, source_uri: str, owner: str, run_id: Optional[str] = None, trace_id: Optional[str] = None) -> ProcessingResult:
        r_id = run_id or self.id_gen.generate_id()
        t_id = trace_id or self.id_gen.generate_id()
        
        run_id_var.set(r_id)
        trace_id_var.set(t_id)
        orchestrator_var.set("prefect")
        owner_var.set(owner)

        logger.info("Pipeline execution started", extra={"telemetry": {"source_uri": source_uri}})

        # Idempotency check
        if self.ledger.is_processed(r_id):
            logger.info("Pipeline skipped due to idempotency ledger hit", extra={"telemetry": {"run_id": r_id, "idempotency_status": "skipped_duplicate"}})
            return ProcessingResult(
                run_id=r_id, rows_in=0, rows_valid=0, rows_quarantined=0,
                lake_path="", quarantine_path="", status="SKIPPED_DUPLICATE", idempotency_status="skipped_duplicate"
            )

        # Step 1: Read
        step_name_var.set("read_csv")
        t0 = time.time()
        try:
            df, read_meta = self.reader.read_csv(source_uri)
            logger.info("Step succeeded", extra={"telemetry": {"step": "read_csv", "duration_sec": round(time.time() - t0, 4)}})
        except Exception as e:
            logger.error(f"Step failed: {str(e)}", extra={"telemetry": {"step": "read_csv", "exception": type(e).__name__}})
            raise

        # Step 2: Transform & Schema Gate
        df_valid, df_quarantine, transform_metrics = self.schema_engine.process_and_validate(df)

        # Step 3: Write Lake & Quarantine
        step_name_var.set("write_lake_and_quarantine")
        t0 = time.time()
        lake_path = ""
        quarantine_path = ""

        if not df_valid.empty:
            lake_path = self.lake_writer.write_parquet(df_valid, f"owner={owner}/run_id={r_id}/data.parquet")

        if not df_quarantine.empty:
            quarantine_path = self.quarantine_writer.write_quarantine(df_quarantine, f"owner={owner}/run_id={r_id}/bad_records.parquet")

        logger.info("Step succeeded", extra={"telemetry": {"step": "write_lake_and_quarantine", "duration_sec": round(time.time() - t0, 4)}})

        result = ProcessingResult(
            run_id=r_id,
            rows_in=transform_metrics["rows_in"],
            rows_valid=transform_metrics["rows_valid"],
            rows_quarantined=transform_metrics["rows_quarantined"],
            lake_path=lake_path,
            quarantine_path=quarantine_path,
            status="SUCCESS",
            idempotency_status="inserted"
        )

        self.ledger.mark_processed(r_id, {
            "rows_valid": result.rows_valid,
            "rows_quarantined": result.rows_quarantined,
            "lake_path": lake_path
        })

        logger.info("Pipeline execution completed successfully", extra={"telemetry": result.__dict__})
        return result

# Prefect Flow Wrappers (Thin Orchestrator)
@task(retries=2, retry_delay_seconds=1)
def run_ingestion_task(service: IngestionPipelineService, source_uri: str, owner: str, run_id: Optional[str] = None) -> ProcessingResult:
    return service.execute_pipeline(source_uri=source_uri, owner=owner, run_id=run_id)

@flow(name="ingest_csv_to_lake_flow")
def ingest_csv_to_lake_flow(service: IngestionPipelineService, source_uri: str, owner: str, run_id: Optional[str] = None) -> ProcessingResult:
    return run_ingestion_task(service, source_uri, owner, run_id)