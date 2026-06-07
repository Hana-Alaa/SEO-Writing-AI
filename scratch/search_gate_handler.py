with open("src/services/workflow_controller.py", "r", encoding="utf-8", errors="ignore") as f:
    for idx, line in enumerate(f, 1):
        if "_evaluate_brand_owned_section_fulfillment" in line or "fulfillment_status" in line:
            print(f"Line {idx}: {line.strip()}")
