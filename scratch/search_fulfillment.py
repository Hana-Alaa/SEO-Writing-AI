with open("src/services/workflow_controller.py", "r", encoding="utf-8", errors="ignore") as f:
    for idx, line in enumerate(f, 1):
        if "def " in line and ("fulfillment" in line.lower() or "rewrite" in line.lower() or "revision" in line.lower() or "eval" in line.lower()):
            print(f"Line {idx}: {line.strip()}")
