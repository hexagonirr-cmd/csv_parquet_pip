import json
import logging
import sys
from contextvars import ContextVar
from typing import Any, Dict, Optional

# Context Variables for Correlation
run_id_var: ContextVar[Optional[str]] = ContextVar("run_id", default=None)
trace_id_var: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)
orchestrator_var: ContextVar[Optional[str]] = ContextVar("orchestrator", default="local")
step_name_var: ContextVar[Optional[str]] = ContextVar("step_name", default=None)
owner_var: ContextVar[Optional[str]] = ContextVar("owner", default=None)

class StructuredJSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "run_id": run_id_var.get(),
            "trace_id": trace_id_var.get(),
            "orchestrator": orchestrator_var.get(),
            "step_name": step_name_var.get(),
            "owner": owner_var.get(),
        }
        if hasattr(record, "telemetry") and isinstance(record.telemetry, dict):
            log_entry.update(record.telemetry)
        return json.dumps(log_entry)

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredJSONFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger