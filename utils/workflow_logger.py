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
            
            # Auto-generate summaries
            self.export_text_summary()
            self.export_manager_summary()
            
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

    def export_text_summary(self, filename: str = "metrics_summary.txt"):
        """Generates a clean, readable text summary of step times and AI tokens."""
        if not self.metrics:
            return

        filepath = os.path.join(self.output_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("===================================================\n")
                f.write("        AI Workflow Execution Metrics Summary      \n")
                f.write("===================================================\n\n")

                for metric in self.metrics:
                    step = metric.get("step_name", "Unknown")
                    duration = float(metric.get("duration_sec", 0))
                    tokens = int(metric.get("total_tokens", 0))

                    f.write(f"Step: {step.upper()}\n")
                    f.write(f"  -- Duration: {duration:.2f} seconds\n")
                    f.write(f"  -- Tokens Used: {tokens:,}\n")
                    f.write("-" * 50 + "\n")

            logger.info(f"Exported metrics summary text to: {filepath}")
        except Exception as e:
            logger.error(f"Failed to export text summary: {e}")

    def export_manager_summary(self, filename: str = "manager_report.txt"):
        """
        Generates a simplified, executive-level report grouped by phases.
        Hides technical internal events and uses friendly terminology.
        """
        if not self.metrics: return
        
        # Step -> (Phase, Friendly Name)
        STEP_MAP = {
            "analysis_init": ("Phase 1: Project Setup", "Project Initialization"),
            "brand_discovery": ("Phase 1: Project Setup", "Brand Intelligence Gathering"),
            "local_neighborhoods": ("Phase 1: Project Setup", "Regional Market Research"),
            "web_research": ("Phase 2: Market Analysis", "Live Web & Competitor Search"),
            "serp_analysis": ("Phase 2: Market Analysis", "Google Search Intent Analysis"),
            "intent_title": ("Phase 3: Strategy & Design", "User Intent Calibration"),
            "style_analysis": ("Phase 3: Strategy & Design", "Persona & Tone Design"),
            "content_strategy": ("Phase 3: Strategy & Design", "SEO Strategic Roadmap"),
            "outline_generation": ("Phase 3: Strategy & Design", "Article Structure Design"),
            "content_writing": ("Phase 4: Content Production", "Main Article Generation"),
            "image_prompting": ("Phase 5: Visuals & Finalization", "Creative Concept Planning"),
            "image_generation": ("Phase 5: Visuals & Finalization", "AI Visual Creation (7 Images)"),
            "assembly": ("Phase 5: Visuals & Finalization", "Technical Content Assembly"),
            "image_inserter": ("Phase 5: Visuals & Finalization", "Image & Visual Integration"),
            "meta_schema": ("Phase 5: Visuals & Finalization", "SEO Metadata & Schema Markup"),
            "render_html": ("Phase 5: Visuals & Finalization", "Final Web Page Generation")
        }

        # Handle dynamics like SECTION_...
        def get_friendly_info(raw_name):
            is_total = raw_name.startswith("STEP_TOTAL: ")
            clean_name = raw_name.replace("STEP_TOTAL: ", "").strip()
            
            # Prioritize individual production steps for detail, or totals for research
            if clean_name in STEP_MAP:
                # For high-level phases like Research/Strategy, use the total if available
                # But if it's content_writing or image_generation, we want the individual "fizz"
                if clean_name in ["content_writing", "image_generation", "image_prompting"]:
                    return None # Skip the generic total, we'll see the individual sections/images
                if is_total:
                    return STEP_MAP[clean_name]
                return None
                
            if raw_name.startswith("SECTION_"): 
                return ("Phase 4: Content Production", f"Writing: {raw_name.replace('SECTION_', '').replace('_', ' ').title()}")
            if raw_name.startswith("IMAGE_"): 
                # Keep individual image logs as they show the 7 images requirement
                step_parts = raw_name.replace('IMAGE_', '').split('_')
                img_type = step_parts[0].title()
                img_loc = step_parts[-1] if len(step_parts) > 1 else "Gen"
                return ("Phase 5: Visuals & Finalization", f"Creating Image: {img_type} ({img_loc})")
            
            return None

        phases = {}
        total_time = 0
        total_units = 0

        for m in self.metrics:
            info = get_friendly_info(m["step_name"])
            if not info: continue 
            
            phase_name, friendly_name = info
            if phase_name not in phases: 
                phases[phase_name] = {"steps": [], "time": 0, "units": 0}
            
            dur = float(m["duration_sec"])
            # Fallback for tokens if 0 (e.g. if the image model didn't report them but we know it's AI)
            units_val = int(m["total_tokens"])
            if units_val > 0:
                units_display = f"{units_val:,}"
            elif "Image" in friendly_name:
                units_display = "AI Generated"
            else:
                units_display = "Local Process"
            
            phases[phase_name]["steps"].append({
                "name": friendly_name,
                "time": f"{dur:.1f}s" if dur >= 1 else "< 1s",
                "units": units_display
            })
            phases[phase_name]["time"] += dur
            phases[phase_name]["units"] += units_val
            
            total_time += dur
            total_units += units_val

        filepath = os.path.join(self.output_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("╔" + "═"*58 + "╗\n")
                f.write("║" + " EXECUTIVE ARTICLE GENERATION REPORT ".center(58) + "║\n")
                f.write("╚" + "═"*58 + "╝\n\n")
                
                f.write(f"● OVERALL EXECUTION TIME: {total_time/60:.1f} minutes\n")
                f.write(f"● TOTAL AI PROCESSING UNITS: {total_units:,}\n")
                f.write(f"● PROJECT STATUS: COMPLETED SUCCESSFULLY\n")
                f.write(f"● GENERATION DATE: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
                
                f.write("PHASE BREAKDOWN\n")
                f.write("=" * 60 + "\n\n")
                
                for phase, data in phases.items():
                    f.write(f"▶ {phase}\n")
                    f.write("  " + "─"*56 + "\n")
                    for s in data["steps"]:
                        f.write(f"  • {s['name']:<38} | {s['time']:>8} | AI: {s['units']}\n")
                    
                    # Phase Total
                    phase_time = f"{data['time']/60:.1f}m" if data['time'] > 60 else f"{data['time']:.1f}s"
                    f.write("  " + "─"*56 + "\n")
                    f.write(f"  SUB-TOTAL {phase.split(':')[-1].upper():<28} | {phase_time:>8} | AI: {data['units']:,}\n\n")
                
                f.write("=" * 60 + "\n")
                f.write("Note: 'AI Processing Units' represent the computational effort \n")
                f.write("expended by the AI models. 'Local Process' indicates \n")
                f.write("technical assembly tasks performed without external AI costs.\n")
                f.write("'AI Generated' indicates high-compute visual creation steps.\n")

            logger.info(f"Exported Manager Report to: {filepath}")
        except Exception as e:
            logger.error(f"Failed to export manager report: {e}")
