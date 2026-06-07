with open("src/services/workflow_controller.py", "r", encoding="utf-8", errors="ignore") as f:
    for idx, line in enumerate(f, 1):
        if "def run_workflow" in line:
            print(f"Found on line {idx}: {line.strip()}")
