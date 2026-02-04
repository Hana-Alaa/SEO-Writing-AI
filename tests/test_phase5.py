import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.workflow_controller import WorkflowController

# ---------- تعريف state تجريبي ----------
state = {
    "input_data": {
        "title": "Test Article",
        "primary_keyword": "test keyword"
    },
    "seo_meta": {
        "primary_keyword": "test keyword"
    },
    "outline": [
        {"id": "intro", "title": "Introduction"},
        {"id": "h2_1", "title": "First Section"}
    ],
    "final_output": {
        "final_markdown": "# Test Article\n\n## Introduction\nSome text.",
        "meta_title": "Test Article",
        "meta_description": "Test description"
    },
    "image_prompts": [],
    "images": []
}

# ---------- تشغيل Phase 5 ----------
controller = WorkflowController()
result = controller.run_workflow(state)

print("=== FINAL OUTPUT ===")
print(result.keys())
print("\nImages:", result["images"])
print("\nMarkdown:\n", result["final_markdown"])
