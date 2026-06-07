with open("src/services/validation_service.py", "r", encoding="utf-8", errors="ignore") as f:
    for idx, line in enumerate(f, 1):
        if "def " in line:
            print(f"Line {idx}: {line.strip()}")
