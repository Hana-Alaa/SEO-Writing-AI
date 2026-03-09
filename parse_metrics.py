import csv
import sys
import os

def generate_summary(csv_path, output_path):
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        return

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("===============================\n")
        f.write("      WORKFLOW METRICS SUMMARY       \n")
        f.write("===============================\n\n")
        
        for row in rows:
            step = row.get("step_name", "Unknown")
            duration = float(row.get("duration_sec", 0))
            tokens = int(row.get("total_tokens", 0))
            
            # Skip the super long text fields
            f.write(f"Step: {step.upper()}\n")
            f.write(f"  -- Time taken: {duration:.2f} seconds\n")
            f.write(f"  -- AI Tokens Used: {tokens:,}\n")
            f.write("-" * 40 + "\n")
            
    print(f"Summary written to {output_path}")

if __name__ == "__main__":
    latest_csv = r"e:\SEO-Writing-AI\output\web-design-agency_20260309_091330\metrics.csv"
    output = r"e:\SEO-Writing-AI\output\web-design-agency_20260309_091330\metrics_summary.txt"
    generate_summary(latest_csv, output)
