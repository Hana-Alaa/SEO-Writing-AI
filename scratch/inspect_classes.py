with open(r"f:\SEO-Writing-AI\src\services\workflow_controller.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

print(f"Total lines: {len(lines)}")

found = []
for idx, line in enumerate(lines):
    if "class " in line or "def " in line:
        found.append((idx + 1, line.strip()))

with open(r"f:\SEO-Writing-AI\scratch\controller_structure.txt", "w", encoding="utf-8") as out:
    for lno, content in found:
        out.write(f"Line {lno}: {content}\n")

print("Done. Controller structure written to scratch/controller_structure.txt")
