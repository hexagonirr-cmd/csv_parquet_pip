import re
import time
import pandas as pd
import numpy as np
import pandera.pandas as pa
from typing import Optional
from pandera.typing import Series
from src.utils.logger import get_logger, step_name_var

logger = get_logger("transform.engine")

class SchemaGateEngine:
    def __init__(self, custom_schema: Optional[pa.DataFrameModel] = None):
        # Canonical dynamic schema definition (variable fields like order_title and grand_total included)
        self.schema = custom_schema or pa.DataFrameSchema({
            "record_id": pa.Column(str, nullable=False, checks=pa.Check.str_length(min_value=1)),
            "event_timestamp": pa.Column(str, nullable=False, checks=pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}")),
            "owner": pa.Column(str, nullable=False),
            "order_title": pa.Column(str, nullable=False),
            "grand_total": pa.Column(float, checks=pa.Check.ge(0.0)),
        })

    def normalize_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        raw_count = len(df.columns)
        normalized_cols = []
        for col in df.columns:
            clean = str(col).strip().lower()
            clean = re.sub(r'[\s\-_]+', '_', clean)
            clean = re.sub(r'[^\w]', '', clean)
            normalized_cols.append(clean)
        
        df = df.copy()
        df.columns = normalized_cols
        logger.info("Headers normalized", extra={"telemetry": {
            "raw_column_count": raw_count,
            "normalized_columns": normalized_cols
        }})
        return df

    def coerce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Safe type casting using numpy/pandas coercions
        if "record_id" in df.columns:
            df["record_id"] = df["record_id"].astype(str).str.strip()
            df.loc[df["record_id"].isin(["", "nan", "None"]), "record_id"] = np.nan

        if "event_timestamp" in df.columns:
            df["event_timestamp"] = df["event_timestamp"].astype(str).str.strip()

        if "owner" in df.columns:
            df["owner"] = df["owner"].astype(str).str.strip()

        if "order_title" in df.columns:
            df["order_title"] = df["order_title"].astype(str).str.strip()

        if "grand_total" in df.columns:
            df["grand_total"] = pd.to_numeric(df["grand_total"], errors="coerce")

        return df

    def process_and_validate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        start_time = time.time()
        step_name_var.set("schema_gate_validation")
        
        rows_in = len(df)
        df_norm = self.normalize_headers(df)
        df_coerced = self.coerce_types(df_norm)

        valid_rows = []
        quarantine_rows = []
        failure_counts = {"null_ids": 0, "type_mismatch": 0, "other": 0}

        for idx, row in df_coerced.iterrows():
            row_df = pd.DataFrame([row])
            try:
                self.schema.validate(row_df, lazy=False)
                valid_rows.append(row)
            except pa.errors.SchemaError as e:
                row_dict = row.to_dict()
                row_dict["schema_error"] = str(e)
                if pd.isna(row.get("record_id")) or not str(row.get("record_id")).strip():
                    failure_counts["null_ids"] += 1
                elif pd.isna(row.get("grand_total")):
                    failure_counts["type_mismatch"] += 1
                else:
                    failure_counts["other"] += 1
                quarantine_rows.append(row_dict)

        df_valid = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame(columns=self.schema.columns.keys())
        df_quarantine = pd.DataFrame(quarantine_rows) if quarantine_rows else pd.DataFrame()

        elapsed = time.time() - start_time
        metrics = {
            "rows_in": rows_in,
            "rows_valid": len(df_valid),
            "rows_quarantined": len(df_quarantine),
            "failure_rule_counts": failure_counts,
            "elapsed_seconds": round(elapsed, 4)
        }

        logger.info("Schema gate processed", extra={"telemetry": metrics})
        return df_valid, df_quarantine, metrics