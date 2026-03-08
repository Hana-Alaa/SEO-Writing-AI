import re
import json

log_file = r"e:\SEO-Writing-AI\logs\prompts.log"
start_line = 239327

pattern = re.compile(r"seo_engine - INFO - ({.*})")

count = 0
with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
    for i, line in enumerate(f, 1):
        if i < start_line:
            continue
        match = pattern.search(line)
        if match:
            print(match.group(1))
            count += 1
            if count > 20:
                break
