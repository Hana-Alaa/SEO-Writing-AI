import re
import csv
from datetime import datetime

log_path = r"e:\SEO-Writing-AI\logs\prompts.log"
csv_path = r"e:\SEO-Writing-AI\logs\prompts_analysis.csv"

time_pattern = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")
step_start_pattern = re.compile(r"--- Starting Step: ([a-zA-Z_]+)(?: |\()")
step_finish_pattern = re.compile(r"--- Finished Step: ([a-zA-Z_]+)(?: |\()")
prompt_pattern = re.compile(r"================ FINAL PROMPT \((.*?)\) ================")

run_id = 0
current_step = "unknown"
active_prompt_type = None
active_prompt_start_time = None
last_timestamp = None
last_timestamp_str = None

data = []

with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
    for line in f:
        # Match timestamp
        t_match = time_pattern.search(line)
        if t_match:
            last_timestamp_str = t_match.group(1)
            last_timestamp = datetime.strptime(last_timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
        
        # Check step start
        s_start = step_start_pattern.search(line)
        if s_start:
            step_name = s_start.group(1)
            if step_name == 'analysis':
                run_id += 1
            # If run_id is still 0 (e.g., started mid-run in the log), set it to 1
            if run_id == 0:
                run_id = 1
                
            current_step = step_name
            # If a prompt was active, it shouldn't cross steps, close it just in case
            if active_prompt_type and active_prompt_start_time and last_timestamp:
                duration = (last_timestamp - active_prompt_start_time).total_seconds()
                data.append([run_id, current_step, active_prompt_type, active_prompt_start_time.strftime("%Y-%m-%d %H:%M:%S"), last_timestamp.strftime("%Y-%m-%d %H:%M:%S"), duration])
                active_prompt_type = None
            continue
            
        # Check step finish
        s_finish = step_finish_pattern.search(line)
        if s_finish:
            if active_prompt_type and active_prompt_start_time and last_timestamp:
                duration = (last_timestamp - active_prompt_start_time).total_seconds()
                data.append([run_id, current_step, active_prompt_type, active_prompt_start_time.strftime("%Y-%m-%d %H:%M:%S"), last_timestamp.strftime("%Y-%m-%d %H:%M:%S"), duration])
                active_prompt_type = None
            continue
            
        # Check prompt
        p_match = prompt_pattern.search(line)
        if p_match:
            new_prompt_type = p_match.group(1)
            # If there was an active prompt previously, close it
            if active_prompt_type and active_prompt_start_time and last_timestamp:
                duration = (last_timestamp - active_prompt_start_time).total_seconds()
                data.append([run_id, current_step, active_prompt_type, active_prompt_start_time.strftime("%Y-%m-%d %H:%M:%S"), last_timestamp.strftime("%Y-%m-%d %H:%M:%S"), duration])
            
            # Start new prompt
            active_prompt_type = new_prompt_type
            active_prompt_start_time = last_timestamp

# Write to CSV
with open(csv_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['Run_ID', 'Step', 'Prompt_Type', 'Start_Time', 'End_Time', 'Duration_Seconds'])
    writer.writerows(data)

print(f"Extraction complete! Found {len(data)} prompts across {run_id} runs.")
print(f"Saved to {csv_path}")
