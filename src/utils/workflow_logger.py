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
        
        # OpenRouter pricing per 1k tokens (simplified)
        self.PRICING_MAP = {
            "google/gemini-3-flash-preview": {"prompt": 0.0001, "completion": 0.0003},
            "openai/o4-mini:online": {"prompt": 0.00015, "completion": 0.0006},
            "black-forest-labs/flux.2-pro": {"image": 0.02},
            "google/gemini-3.1-flash-image-preview": {"image": 0.005},
            "google/gemini-2.0-flash-001": {"prompt": 0.0001, "completion": 0.0003}
        }
        
    def start_step(self, step_name: str) -> float:
        """Returns the start time of a step."""
        logger.info(f"Starting workflow step: {step_name}")
        return time.time()
    
    def end_step(self, 
                 step_name: str, 
                 start_time: float, 
                 prompt: Optional[str] = None, 
                 response: Optional[Any] = None,
                 tokens: Optional[Dict[str, int]] = None,
                 model: str = "unknown"):
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
            "model": model,
            "estimated_cost": self._calculate_cost(model, tokens),
            "is_google": "google" in model.lower(),
            "prompt_text": prompt or "N/A",
            "response_text": resp_str or "N/A"
        }
        
        self.metrics.append(metric)
        self._append_to_csv(metric)
        self._log_to_file(step_name, prompt, resp_str, duration)
        logger.info(f"Finished step: {step_name} in {duration:.2f}s")

    def log_ai_call(self, step_name: str, prompt: str, response: Any, tokens: Dict[str, int], duration: float, model: str = "unknown"):
        """Logs an AI call immediately, useful for nested or parallel steps."""
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
            "model": model,
            "estimated_cost": self._calculate_cost(model, tokens),
            "is_google": "google" in model.lower(),
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

        # Collect all unique keys from all logic
        all_keys = set()
        for m in self.metrics:
            all_keys.update(m.keys())
        keys = sorted(list(all_keys))
        try:
            with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                dict_writer = csv.DictWriter(f, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(self.metrics)
            logger.info(f"Exported metrics to: {filepath}")
            
            # Auto-generate summaries
            self.export_text_summary()
            self.export_manager_summary()
            self.export_consumption_reports()
            
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
            "model": "N/A",
            "estimated_cost": 0.0,
            "is_google": False,
            "prompt_text": "N/A",
            "response_text": str(data)
        })

    def log_step_details(self, step_name: str, duration: float, input_data: Any = None, output_data: Any = None, error: str = None):
        """Logs comprehensive step details including inputs, outputs, and errors."""
        
        def _serialize(obj):
            try:
                if isinstance(obj, (dict, list)):
                    return json.dumps(obj, ensure_ascii=False, indent=2)
                return str(obj)
            except:
                return "Unserializable Data"

        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'#'*30} WORKFLOW STEP: {step_name} ({duration:.2f}s) {'#'*30}\n")
            
            if input_data:
                # Truncate large data blocks for the log if needed, but here we want "real log"
                # We'll log a filtered version of state to avoid 1GB logs
                filtered_input = self._filter_state_for_log(input_data)
                f.write(f"STEP_INPUT:\n{_serialize(filtered_input)}\n")
            
            if error:
                f.write(f"ERROR:\n{error}\n")
            elif output_data:
                filtered_output = self._filter_state_for_log(output_data)
                f.write(f"STEP_OUTPUT:\n{_serialize(filtered_output)}\n")
                
            f.write(f"{'#'*80}\n")

    def _filter_state_for_log(self, state: Any) -> Any:
        """Filters out massive binary or redundant data from state for logging."""
        if not isinstance(state, dict):
            return state
            
        filtered = {}
        # List of keys to truncate or skip if they are usually massive
        SKIP_KEYS = {'brand_pages_index', 'inline_svg_content', 'semantic_model'}
        TRUNCATE_KEYS = {'sections', 'final_output', 'internal_resources'}

        for k, v in state.items():
            if k in SKIP_KEYS:
                filtered[k] = f"<{type(v).__name__} (Hidden for brevity)>"
            elif k in TRUNCATE_KEYS:
                if isinstance(v, dict):
                    filtered[k] = {sk: (str(sv)[:200] + "...") if len(str(sv)) > 200 else sv for sk, sv in v.items()}
                elif isinstance(v, list):
                    filtered[k] = [(str(i)[:200] + "...") if len(str(i)) > 200 else i for i in v[:5]] + ([f"... and {len(v)-5} more items"] if len(v) > 5 else [])
                else:
                    filtered[k] = (str(v)[:500] + "...") if len(str(v)) > 500 else v
            else:
                filtered[k] = v
        return filtered

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
        total_time_start = None
        total_time_end = None
        total_units = 0

        # Pre-process metrics to calculate real durations
        processed_metrics = []
        for m in self.metrics:
            try:
                end_dt = datetime.fromisoformat(m["timestamp"])
                duration = float(m["duration_sec"])
                start_dt = end_dt.timestamp() - duration
                processed_metrics.append({
                    **m,
                    "start_ts": start_dt,
                    "end_ts": end_dt.timestamp()
                })
            except Exception:
                continue

        for m in processed_metrics:
            raw_name = m["step_name"]
            info = get_friendly_info(raw_name)
            if not info:
                # Fallback for lowercase section_
                if raw_name.lower().startswith("section_"):
                    step_parts = raw_name.split('_')
                    friendly_name = f"Writing: {' '.join(step_parts[2:]).title()}"
                    info = ("Phase 4: Content Production", friendly_name)
                else:
                    continue 
            
            phase_name, friendly_name = info
            if phase_name not in phases: 
                phases[phase_name] = {"steps": [], "start_ts": float('inf'), "end_ts": 0, "units": 0}
            
            dur = float(m["duration_sec"])
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
            
            # Track wall-clock range for phase
            phases[phase_name]["start_ts"] = min(phases[phase_name]["start_ts"], m["start_ts"])
            phases[phase_name]["end_ts"] = max(phases[phase_name]["end_ts"], m["end_ts"])
            phases[phase_name]["units"] += units_val
            
            # Track global wall-clock range
            if total_time_start is None or m["start_ts"] < total_time_start:
                total_time_start = m["start_ts"]
            if total_time_end is None or m["end_ts"] > total_time_end:
                total_time_end = m["end_ts"]
            
            total_units += units_val

        overall_duration = (total_time_end - total_time_start) if total_time_start and total_time_end else 0

        filepath = os.path.join(self.output_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("╔" + "═"*58 + "╗\n")
                f.write("║" + " EXECUTIVE ARTICLE GENERATION REPORT ".center(58) + "║\n")
                f.write("╚" + "═"*58 + "╝\n\n")
                
                f.write(f"● OVERALL EXECUTION TIME: {overall_duration/60:.1f} minutes\n")
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
                    
                    # Phase Total (Real Duration)
                    p_dur = data["end_ts"] - data["start_ts"]
                    phase_time = f"{p_dur/60:.1f}m" if p_dur > 60 else f"{p_dur:.1f}s"
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

    def _calculate_cost(self, model: str, tokens: Optional[Dict[str, int]]) -> float:
        """Calculates estimated cost for a step."""
        if not tokens or model == "unknown":
            return 0.0
        
        rates = self.PRICING_MAP.get(model, {"prompt": 0, "completion": 0})
        
        # Handle Image Costs
        if "image" in model.lower() or "flux" in model.lower():
            return rates.get("image", 0.0)

        p_tokens = tokens.get("prompt_tokens", 0)
        c_tokens = tokens.get("completion_tokens", 0)
        
        cost = (p_tokens / 1000) * rates.get("prompt", 0) + (c_tokens / 1000) * rates.get("completion", 0)
        return round(cost, 6)

    def export_consumption_reports(self):
        """Generates consumption reports in both MD and TXT formats."""
        if not self.metrics: return
        
        total_cost = sum(m.get("estimated_cost", 0) for m in self.metrics)
        total_tokens = sum(m.get("total_tokens", 0) for m in self.metrics)
        google_cost = sum(m.get("estimated_cost", 0) for m in self.metrics if m.get("is_google"))
        other_cost = total_cost - google_cost

        # Generate Markdown Version
        md_content = [
            "# API Consumption & Cost Report",
            f"\n**Total Estimated Cost:** ${total_cost:.4f}",
            f"\n**Total Tokens:** {total_tokens:,}",
            "\n## Cost Breakdown by Provider",
            f"- **Google (Gemini):** ${google_cost:.4f}",
            f"- **Others:** ${other_cost:.4f}",
            "\n## Detailed Step Usage",
            "| Step | Model | Tokens | Duration | Cost |",
            "| :--- | :--- | :--- | :--- | :--- |"
        ]

        for m in self.metrics:
            step_name = m.get('step_name', 'Unknown')
            model = m.get('model', 'N/A')
            total_tokens_val = m.get('total_tokens', 0)
            duration_sec = m.get('duration_sec', 0)
            est_cost = m.get('estimated_cost', 0.0)
            md_content.append(f"| {step_name} | {model} | {total_tokens_val:,} | {duration_sec}s | ${est_cost:.6f} |")

        md_path = os.path.join(self.output_dir, "consumption_report.md")
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(md_content))

        # Generate TXT Version
        txt_content = [
            "==================================================",
            "        API CONSUMPTION & COST REPORT             ",
            "==================================================",
            f"\nTotal Estimated Cost: ${total_cost:.4f}",
            f"Total Tokens: {total_tokens:,}",
            f"\nGoogle (Gemini) Cost: ${google_cost:.4f}",
            f"Other Costs: ${other_cost:.4f}",
            "\n--------------------------------------------------",
            f"{'Step':<25} | {'Model':<25} | {'Cost':<10}",
            "--------------------------------------------------"
        ]

        for m in self.metrics:
            step_name = str(m.get('step_name', 'Unknown'))[:24]
            model = str(m.get('model', 'N/A'))[:24]
            est_cost = m.get('estimated_cost', 0.0)
            txt_content.append(f"{step_name:<25} | {model:<25} | ${est_cost:.6f}")

        txt_path = os.path.join(self.output_dir, "consumption_report.txt")
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(txt_content))

        logger.info(f"Exported consumption reports to {md_path} and {txt_path}")
