import re

log_path = r"f:\SEO-Writing-AI\output\شقق-للايجار-في-الرياض_20260518_163646\workflow.log"
output_path = r"f:\SEO-Writing-AI\scratch\writer_output.txt"

with open(log_path, "r", encoding="utf-8") as f:
    log_content = f.read()

# Let's search for "sec_09" specifically inside the content_writing step.
# Or let's search for "sec_09" followed by LLM calls.
# Let's search for a block containing "sec_09" and "generated_content" or similar.
pattern = re.compile(r"sec_09", re.IGNORECASE)
matches = [m.start() for m in pattern.finditer(log_content)]

with open(output_path, "w", encoding="utf-8") as out:
    out.write(f"Total matches: {len(matches)}\n")
    for idx, pos in enumerate(matches):
        start = max(0, pos - 1500)
        end = min(len(log_content), pos + 3000)
        out.write(f"\n================ Match {idx+1} ================\n")
        out.write(log_content[start:end])
        out.write("\n=============================================\n")

print("Done check_writer")
