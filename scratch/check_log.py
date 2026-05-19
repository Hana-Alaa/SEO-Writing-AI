import re

log_path = r"f:\SEO-Writing-AI\output\شقق-للايجار-في-الرياض_20260518_163646\workflow.log"
output_path = r"f:\SEO-Writing-AI\scratch\log_output.txt"

with open(log_path, "r", encoding="utf-8") as f:
    log_content = f.read()

# Let's find occurrences of sec_09
matches = [m.start() for m in re.finditer("sec_09", log_content)]

with open(output_path, "w", encoding="utf-8") as out:
    out.write(f"Found {len(matches)} occurrences of 'sec_09'.\n")
    for idx, pos in enumerate(matches):
        start = max(0, pos - 800)
        end = min(len(log_content), pos + 2500)
        out.write(f"\n--- Occurrence {idx+1} at position {pos} ---\n")
        out.write(log_content[start:end])
        out.write("\n" + "-" * 50 + "\n")

print("Done writing to scratch/log_output.txt")
