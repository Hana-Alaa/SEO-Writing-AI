with open(r"f:\SEO-Writing-AI\src\services\workflow_controller.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

print(f"Total lines: {len(lines)}")

# Let's search for "requires_table"
found_req = []
for idx, line in enumerate(lines):
    if "requires_table" in line:
        found_req.append((idx + 1, line.strip()))

print(f"Found 'requires_table' in {len(found_req)} lines:")
for lno, content in found_req[:30]:
    print(f"Line {lno}: {content}")

# Let's search for "infer_contract"
found_infer = []
for idx, line in enumerate(lines):
    if "infer_contract" in line or "infer_format" in line:
        found_infer.append((idx + 1, line.strip()))

print(f"\nFound 'infer' in {len(found_infer)} lines:")
for lno, content in found_infer[:30]:
    print(f"Line {lno}: {content}")
