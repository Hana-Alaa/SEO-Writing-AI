import sys
import os
import asyncio
import logging

# Configure logging to see the speedup
logging.basicConfig(level=logging.INFO)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.workflow_controller import AsyncWorkflowController

# ---------- تجريبي state ----------
test_state = {
    "input_data": {
        "title": "مستقبل تعلم البرمجة مع الذكاء الاصطناعي",
        "keywords": ["تعلم البرمجة", "الذكاء الاصطناعي", "Python", "AI", "Software Development", "Programming", "Machine Learning"]
    }
}

async def main():
    # ---------- تشغيل Async Workflow ----------
    controller = AsyncWorkflowController(work_dir=".")
    
    print("\n🚀 Starting Async Workflow Execution...")
    result = await controller.run_workflow(test_state)
    
    print("\n=== FINAL OUTPUT ===")
    print(f"Title: {result['title']}")
    print(f"Slug: {result['slug']}")
    print(f"Images: {len(result['images'])}")
    print(f"SEO Report Score: {result.get('seo_report', {}).get('score', 'N/A')}")
    
    if result["final_markdown"]:
        print("\n✅ Article Generated Successfully!")
    else:
        print("\n❌ Article Generation Failed.")

if __name__ == "__main__":
    asyncio.run(main())
