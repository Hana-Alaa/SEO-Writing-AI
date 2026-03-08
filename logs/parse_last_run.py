import re
import csv
from datetime import datetime
import json

log_file = r"e:\SEO-Writing-AI\logs\prompts.log"
csv_file = r"e:\SEO-Writing-AI\logs\last_run_analysis.csv"
start_line_num = 239327

time_pattern = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")
step_start_pattern = re.compile(r"--- Starting Step: ([a-zA-Z0-9_]+)")
step_finish_pattern = re.compile(r"--- Finished Step: ([a-zA-Z0-9_]+) \(Duration: (.*?)s\)")
prompt_start_pattern = re.compile(r"={4,} FINAL PROMPT \((.*?)\) ={4,}")
prompt_end_pattern = re.compile(r"={50,}")
http_ok_pattern = re.compile(r"HTTP/1\.1 200 OK")
json_log_pattern = re.compile(r"({.*\"event\":\s*\"model_call\".*})")

rows = []

current_step = None
is_in_prompt = False
prompt_name = None
prompt_lines = []
prompt_start_time = None
last_time = None

current_step_prompts = []

with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
    for i, line in enumerate(f, 1):
        if i < start_line_num:
            continue
            
        t_match = time_pattern.search(line)
        if t_match:
            try:
                last_time = datetime.strptime(t_match.group(1), "%Y-%m-%d %H:%M:%S,%f")
            except:
                pass
                
        # 1. Check Step Start
        m_start = step_start_pattern.search(line)
        if m_start:
            current_step = m_start.group(1)
            current_step_prompts = []
            continue
            
        # 2. Check Prompt Start
        m_prompt_start = prompt_start_pattern.search(line)
        if m_prompt_start:
            prompt_name = m_prompt_start.group(1)
            is_in_prompt = True
            prompt_lines = []
            prompt_start_time = last_time
            continue
            
        # 3. Check Prompt End
        if is_in_prompt:
            if prompt_end_pattern.search(line):
                is_in_prompt = False
                current_step_prompts.append({
                    'prompt_name': prompt_name,
                    'prompt_text': "".join(prompt_lines).strip(),
                    'start_time': prompt_start_time,
                    'duration': None,
                    'is_completed': False
                })
            else:
                clean_line = re.sub(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - .*? - (?:INFO|WARNING|ERROR|DEBUG) - ', '', line)
                prompt_lines.append(clean_line)
            continue
            
        # 4. Check for prompt completion
        m_json = json_log_pattern.search(line)
        if m_json:
            try:
                data = json.loads(m_json.group(1))
                if 'latency_seconds' in data:
                    for p in reversed(current_step_prompts):
                        if not p['is_completed']:
                            p['duration'] = data['latency_seconds']
                            p['is_completed'] = True
                            break
            except:
                pass
            continue
            
        if http_ok_pattern.search(line):
            for p in reversed(current_step_prompts):
                if not p['is_completed'] and last_time and p['start_time']:
                    p['duration'] = (last_time - p['start_time']).total_seconds()
                    p['is_completed'] = True
                    break
        
        # 5. Check Step Finish
        m_finish = step_finish_pattern.search(line)
        if m_finish:
            finished_step = m_finish.group(1)
            step_duration = m_finish.group(2)
            if current_step_prompts:
                for p in current_step_prompts:
                    rows.append({
                        'Step': finished_step,
                        'Step_Duration_sec': step_duration,
                        'Prompt_Name': p['prompt_name'],
                        'Prompt_Duration_sec': round(p['duration'], 2) if p['duration'] else '',
                        'Prompt_Text': p['prompt_text']
                    })
            else:
                rows.append({
                    'Step': finished_step,
                    'Step_Duration_sec': step_duration,
                    'Prompt_Name': '',
                    'Prompt_Duration_sec': '',
                    'Prompt_Text': ''
                })
            current_step = None

with open(csv_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['Step', 'Step_Duration_sec', 'Prompt_Name', 'Prompt_Duration_sec', 'Prompt_Text'])
    writer.writeheader()
    writer.writerows(rows)

print(f"Generated CSV with {len(rows)} records at {csv_file}")
