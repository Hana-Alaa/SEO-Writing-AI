import re
import json

log_path = r"f:\SEO-Writing-AI\output\شقق-للايجار-في-الرياض_20260518_163646\workflow.log"
output_path = r"f:\SEO-Writing-AI\scratch\visual_format_output.txt"

with open(log_path, "r", encoding="utf-8") as f:
    log_content = f.read()

# Let's search for "approved_outline" or search for "sec_06" inside the outline array
matches = [m.start() for m in re.finditer('"outline":', log_content)]

with open(output_path, "w", encoding="utf-8") as out:
    out.write(f"Found {len(matches)} occurrences of 'outline':\n")
    for idx, pos in enumerate(matches):
        out.write(f"\n--- Occurrence {idx+1} ---\n")
        start = pos
        end = min(len(log_content), pos + 4000)
        chunk = log_content[start:end]
        bracket_pos = chunk.find('],')
        if bracket_pos != -1:
            out.write(chunk[:bracket_pos+2])
        else:
            out.write(chunk[:1000])
        out.write("\n" + "="*50 + "\n")

print("Done check_visual_format")
