with open("src/services/brand_evidence_service.py", "r", encoding="utf-8", errors="ignore") as f:
    for idx, line in enumerate(f, 1):
        if "def evaluate_brand_section_fulfillment" in line:
            print(f"Line {idx}: {line.strip()}")
