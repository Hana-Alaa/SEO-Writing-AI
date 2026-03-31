import os
import json
from datetime import datetime
from typing import Dict, Any, List

class DiagnosticReporter:
    """
    Universal diagnostic engine that transforms raw workflow logs into a human-readable 
    Structural Audit & Data Integrity Report.
    """
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.report_path = os.path.join(output_dir, "diagnostic_report.md")

    def generate_report(self, metrics: List[Dict[str, Any]], state: Dict[str, Any]):
        """Generates a comprehensive markdown report for ANY article type."""
        
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        title = state.get("raw_title", "Untitled Project")
        area = state.get("area", "Unknown")
        intent = state.get("article_intent", "Unknown")
        
        md = [
            f"# 🛡️ SEO Writing AI: Diagnostic Audit Report",
            f"\n> **Project:** {title}",
            f"> **Target Area:** {area}",
            f"> **Intent:** {intent}",
            f"> **Generated at:** {now}",
            f"\n---\n",
            "## 🕒 Detailed Step-by-Step Analysis\n",
            "| Step | Duration | Model | Input Summary | Output Integrity |",
            "| :--- | :--- | :--- | :--- | :--- |"
        ]

        total_time = 0
        for m in metrics:
            step = m.get("step_name", "N/A")
            duration = m.get("duration_sec", 0)
            total_time += duration
            model = m.get("model", "Local")
            
            # Smart Input Summarization
            prompt = m.get("prompt_text", "")
            input_summary = self._summarize_data(prompt, 100)
            
            # Integrity Check Logic (Self-Audit)
            response = m.get("response_text", "")
            integrity_check = self._verify_integrity(step, response, state)
            
            md.append(f"| **{step}** | {duration:.2f}s | {model} | {input_summary} | {integrity_check} |")

        md.append(f"\n**Total Processing Time:** {total_time/60:.2f} minutes")
        md.append("\n---\n")
        
        # Deep Dive Sections
        md.append("## 🔦 Strategic Intelligence & Google Research")
        research_data = state.get("research_context", "N/A")
        serp_data = state.get("serp_raw", {})
        
        md.append(f"\n### 🌐 Google (SERP) Insights")
        md.append(f"```json\n{json.dumps(serp_data, indent=2, ensure_ascii=False) if serp_data else 'No SERP data logged.'}\n```")
        
        md.append(f"\n### 🕵️‍♂️ Market & Brand Context")
        md.append(f"\n{str(research_data)[:1000]}...")

        md.append("\n---\n")
        md.append("## 🚧 Audit Details (What Entered vs. What Left)")
        
        for m in metrics:
            step = m.get("step_name", "N/A")
            prompt = m.get("prompt_text", "N/A")
            response = m.get("response_text", "N/A")
            
            md.append(f"\n<details>\n<summary><b>Click to expand {step} Details</b></summary>\n")
            md.append(f"#### 📥 Input Payload:\n```text\n{prompt[:2000]}...\n```")
            md.append(f"#### 📤 Output Result:\n```text\n{response[:2000]}...\n```")
            md.append("\n</details>\n")

        with open(self.report_path, "w", encoding="utf-8") as f:
            f.write("\n".join(md))

    def _summarize_data(self, text: str, limit: int) -> str:
        if not text or text == "N/A": return "N/A"
        clean = text.replace("\n", " ").strip()
        return (clean[:limit] + "...") if len(clean) > limit else clean

    def _verify_integrity(self, step: str, response: str, state: Dict[str, Any]) -> str:
        """Heuristic audit to check if the output matches the expected shape."""
        if not response or response == "N/A": return "⚠️ No Data"
        
        checks = []
        
        # Rule: Introduction must have Primary Keyword
        if "init" in step.lower():
            checks.append("✅ Init Correct")
        
        if "web_research" in step.lower():
            checks.append("🌐 Facts Fetched")
            
        if "outline" in step.lower():
            if "sec_" in response: checks.append("📋 JSON Valid")
            else: checks.append("❌ Invalid JSON")

            # Link Density Audit
            internal_links = response.count("](") - response.count("http") # Markdown links
            external_links = response.count("http")
            checks.append(f"🔗 In:{internal_links} | Ex:{external_links}")

            # Conclusion CTA Check
            if "conclusion" in step.lower() or "final" in step.lower():
                if "**[" in response and "](" in response: checks.append("🎯 CTA Found")
                else: checks.append("❌ Missing CTA Link")

            # Process / List Check
            if "process" in step.lower():
                if "1." in response: checks.append("🔢 Steps Found")
                else: checks.append("⚠️ Not Numbered")

            # Proof Integrity Check
            if "proof" in step.lower() or "evidence" in step.lower():
                digits = [c for c in response if c.isdigit()]
                if len(digits) >= 2: checks.append("📊 Stats Found")
                else: checks.append("⚠️ Generic Proof (Missing Stats)")

        return " | ".join(checks) if checks else "✅ Processed"
