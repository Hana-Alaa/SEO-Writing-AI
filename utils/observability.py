# utils/observability.py

import time
import json
import logging
from typing import Dict, Any

logger = logging.getLogger("seo_engine")

class ObservabilityTracker:

    def __init__(self):
        self.step_metrics = []

    def log_model_call(
        self,
        step: str,
        model: str,
        start_time: float,
        end_time: float,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
    ):
        latency = round(end_time - start_time, 3)

        record = {
            "event": "model_call",
            "step": step,
            "model": model,
            "latency_seconds": latency,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens
        }

        logger.info(json.dumps(record, ensure_ascii=False))
        self.step_metrics.append(record)

    def log_workflow_step(self, step_name: str, duration: float):
        record = {
            "event": "workflow_step",
            "step": step_name,
            "duration_seconds": round(duration, 3)
        }
        logger.info(json.dumps(record, ensure_ascii=False))
        self.step_metrics.append(record)

    def summarize_model_calls(self):
        model_calls = [r for r in self.step_metrics if r["event"] == "model_call"]

        total_tokens = sum(r["total_tokens"] for r in model_calls)
        total_latency = sum(r["latency_seconds"] for r in model_calls)

        return {
            "total_tokens": total_tokens,
            "total_latency": round(total_latency, 2),
            "calls": len(model_calls)
        }


    def reset(self):
        self.step_metrics = []

