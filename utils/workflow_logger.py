import os
import csv
import time
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class WorkflowLogger:
    """
    Tracks and exports metrics for each step of the article generation workflow.
    """
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.metrics: List[Dict[str, Any]] = []
        os.makedirs(self.output_dir, exist_ok=True)
        self.log_file = os.path.join(self.output_dir, "workflow.log")
        self.csv_file = os.path.join(self.output_dir, "metrics.csv")
        
    def start_step(self, step_name: str) -> float:
        """Returns the start time of a step."""
        logger.info(f"Starting workflow step: {step_name}")
        return time.time()
    
    def end_step(self, 
                 step_name: str, 
                 start_time: float, 
                 prompt: Optional[str] = None, 
                 response: Optional[Any] = None,
                 tokens: Optional[Dict[str, int]] = None):
        """Records metrics for a completed step."""
        duration = time.time() - start_time
        
        # Normalize response for logging (handle dict/list)
        resp_str = ""
        if response:
            if isinstance(response, (dict, list)):
                resp_str = json.dumps(response, ensure_ascii=False, indent=2)
            else:
                resp_str = str(response)
                
        metric = {
            "timestamp": datetime.now().isoformat(),
            "step_name": step_name,
            "duration_sec": round(duration, 3),
            "prompt_tokens": tokens.get("prompt_tokens", 0) if tokens else 0,
            "completion_tokens": tokens.get("completion_tokens", 0) if tokens else 0,
            "total_tokens": tokens.get("total_tokens", 0) if tokens else 0,
            "prompt_text": prompt or "N/A",
            "response_text": resp_str or "N/A"
        }
        
        self.metrics.append(metric)
        self._append_to_csv(metric)
        self._log_to_file(step_name, prompt, resp_str, duration)
        logger.info(f"Finished step: {step_name} in {duration:.2f}s")

    def log_ai_call(self, step_name: str, prompt: str, response: Any, tokens: Dict[str, int], duration: float):
        """Logs an AI call immediately, useful for nested or parallel steps."""
        resp_str = ""
        if isinstance(response, (dict, list)):
            resp_str = json.dumps(response, ensure_ascii=False, indent=2)
        else:
            resp_str = str(response)

        metric = {
            "timestamp": datetime.now().isoformat(),
            "step_name": step_name,
            "duration_sec": round(duration, 3),
            "prompt_tokens": tokens.get("prompt_tokens", 0),
            "completion_tokens": tokens.get("completion_tokens", 0),
            "total_tokens": tokens.get("total_tokens", 0),
            "prompt_text": prompt,
            "response_text": resp_str
        }
        
        self.metrics.append(metric)
        self._append_to_csv(metric)
        self._log_to_file(step_name, prompt, resp_str, duration)

    def _log_to_file(self, step_name: str, prompt: str, response: str, duration: float):
        """Logs detailed step info to the workflow log file and a truncated version to console."""
        # 1. Write FULL details to the log file (for deep debugging)
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*20} STEP: {step_name} ({duration:.2f}s) {'='*20}\n")
            f.write(f"PROMPT:\n{prompt}\n")
            f.write(f"{'-'*20} RESPONSE {'-'*20}\n")
            f.write(f"{response}\n")
            f.write(f"{'='*60}\n")

        # 2. Log TRUNCATED version to console to avoid terminal scrambling
        trunc_prompt = (prompt[:150] + "...") if prompt and len(prompt) > 150 else prompt
        trunc_resp = (response[:250] + "...") if response and len(response) > 250 else response
        
        logger.info(f"--- AI Step: {step_name} ({duration:.2f}s) ---")
        logger.debug(f"Prompt (trunc): {trunc_prompt}")
        logger.debug(f"Response (trunc): {trunc_resp}")

    def _append_to_csv(self, metric: Dict[str, Any]):
        """Append a single metric line to the CSV file."""
        file_exists = os.path.isfile(self.csv_file)
        with open(self.csv_file, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=metric.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(metric)
        
    def export_csv(self, filename: str = "metrics.csv"):
        """Exports all collected metrics to a CSV file."""
        filepath = os.path.join(self.output_dir, filename)
        
        if not self.metrics:
            logger.warning("No metrics to export.")
            return

        keys = self.metrics[0].keys()
        try:
            with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                dict_writer = csv.DictWriter(f, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(self.metrics)
            logger.info(f"Exported metrics to: {filepath}")
        except Exception as e:
            logger.error(f"Failed to export CSV: {e}")

    def log_event(self, event_name: str, data: Any):
        """Helper to log non-AI events (like file saving)."""
        self.metrics.append({
            "timestamp": datetime.now().isoformat(),
            "step_name": f"EVENT: {event_name}",
            "duration_sec": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "prompt_text": "N/A",
            "response_text": str(data)
        })
