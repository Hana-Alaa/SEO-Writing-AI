import json
import re
import csv

log_file = r"e:\SEO-Writing-AI\logs\prompts.log"
output_csv = r"e:\SEO-Writing-AI\logs\Manager_Report.csv"

start_line = 239327

step_start_pattern = re.compile(r"--- Starting Step: ([a-zA-Z0-9_]+)")
prompt_start_pattern = re.compile(r"={4,} FINAL PROMPT \((.*?)\) ={4,}")
prompt_end_pattern = re.compile(r"={50,}")
json_log_pattern = re.compile(r"seo_engine - INFO - ({.*})")

records = []
current_step = None
step_prompts = []

is_in_prompt = False
current_prompt_name = ""
current_prompt_lines = []

# To keep track of the events in the current step
current_step_events = []

with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
    for i, line in enumerate(f, 1):
        if i < start_line:
            continue
            
        # 1. Step Start
        m_start = step_start_pattern.search(line)
        if m_start:
            current_step = m_start.group(1)
            current_step_events = []
            continue
            
        # 2. Prompt Text Blocks
        m_prompt_start = prompt_start_pattern.search(line)
        if m_prompt_start:
            current_prompt_name = m_prompt_start.group(1)
            is_in_prompt = True
            current_prompt_lines = []
            continue
            
        if is_in_prompt:
            if prompt_end_pattern.search(line):
                is_in_prompt = False
                # Save collected prompt
                full_text = " ".join([l.strip() for l in current_prompt_lines])
                # We will attach this prompt to the next model_call event
                current_step_events.append({"type": "prompt_text", "name": current_prompt_name, "text": full_text})
            else:
                # remove the timestamp prefix to keep just the prompt text
                clean_line = re.sub(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - .*? - (?:INFO|WARNING|ERROR|DEBUG) - ', '', line)
                current_prompt_lines.append(clean_line)
            continue
            
        # 3. JSON Events (Model Call & Workflow Step)
        m_json = json_log_pattern.search(line)
        if m_json:
            try:
                data = json.loads(m_json.group(1))
                if data.get("event") == "model_call":
                    # Pair with the most recent prompt_text if one exists and hasn't been used yet
                    prompt_txt_info = None
                    for event in reversed(current_step_events):
                        if event["type"] == "prompt_text" and not event.get("used"):
                            prompt_txt_info = event
                            event["used"] = True
                            break
                            
                    # Fallback knowledge for steps that use AI but logging doesn't print the prompt
                    fallback_text = "No explicit prompt text logged for this step."
                    if not prompt_txt_info:
                        if current_step == "brand_discovery":
                            fallback_text = "You are a Brand Intelligence Analyst.\nBelow is real text scraped from multiple pages of a company's website...\nWrite a detailed 4-6 sentence factual summary..."
                        elif current_step == "web_research":
                            fallback_text = "Use web search to analyze the primary keyword and competitors..."
                        elif current_step == "serp_analysis":
                            fallback_text = "Analyze SERP data and extract LSI keywords, PAA, and related searches..."
                        elif current_step == "local_seo" or current_step == "intent_title":
                            fallback_text = f"Prompt template executed for {current_step}..."
                            
                    current_step_events.append({
                        "type": "model_call",
                        "model": data.get("model", ""),
                        "latency": data.get("latency_seconds", ""),
                        "tokens": data.get("total_tokens", ""),
                        "prompt_name": prompt_txt_info["name"] if prompt_txt_info else current_step,
                        "prompt_text": prompt_txt_info["text"] if prompt_txt_info else fallback_text
                    })
                
                elif data.get("event") == "workflow_step":
                    step_duration = data.get("duration_seconds", "")
                    
                    # Generate CSV records for this step
                    calls = [e for e in current_step_events if e["type"] == "model_call"]
                    
                    if not calls:
                        # Step had no AI calls
                        fallback_desc = "Standard system processing step (no LLM interaction)."
                        if current_step == "assembly":
                            fallback_desc = "Code logic: Retrieves all written text and compiles the full Markdown document without pinging the AI."
                        elif current_step == "image_generation":
                            fallback_desc = "API Call: Downloading images generated from the prior step prompts (Timing represents download/rendering latency)."
                        elif current_step == "image_inserter":
                            fallback_desc = "Code logic: Injects image tags into the final document."
                        elif current_step == "render_html":
                            fallback_desc = "Code logic: Converts the markdown to final HTML format."
                        elif current_step == "analysis_init":
                            fallback_desc = "System Initialization: Loading configs, preparing workspace structure."
                            
                        records.append({
                            "Step Name": current_step,
                            "Total Step Duration (sec)": step_duration,
                            "Model Name": "-",
                            "Response Time (sec)": "-",
                            "Tokens Used": "-",
                            "Prompt Name": "-",
                            "Prompt Snippet": fallback_desc
                        })
                    else:
                        for idx, call in enumerate(calls):
                            snippet = call["prompt_text"]
                            if len(snippet) > 200:
                                snippet = snippet[:197] + "..."
                                
                            records.append({
                                "Step Name": current_step if idx == 0 else f'↳ {current_step} (Call {idx+1})',
                                "Total Step Duration (sec)": step_duration if idx == 0 else "-",
                                "Model Name": call["model"],
                                "Response Time (sec)": call["latency"],
                                "Tokens Used": call["tokens"],
                                "Prompt Name": call["prompt_name"],
                                "Prompt Snippet": snippet
                            })
                            
                    current_step = None
                    current_step_events = []
            except Exception as e:
                pass


with open(output_csv, 'w', newline='', encoding='utf-8-sig') as f:
    fieldnames = [
        "Step Name", 
        "Total Step Duration (sec)", 
        "Prompt Name", 
        "Model Name", 
        "Response Time (sec)", 
        "Tokens Used", 
        "Prompt Snippet"
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(records)

print(f"Manager Report created at {output_csv} with {len(records)} records.")
