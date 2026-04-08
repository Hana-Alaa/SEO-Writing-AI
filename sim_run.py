import asyncio
import os
import shutil
from src.services.workflow_controller import AsyncWorkflowController
from src.services.mock_ai_client import MockAIClient

async def run_simulation():
    print("=== Starting ZERO-COST Simulation Run ===")
    
    # 1. Setup Simulation Directory
    sim_id = "test_simulation_1"
    work_dir = os.path.join("output", "simulated_runs", sim_id)
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)
    os.makedirs(work_dir, exist_ok=True)
    
    # 2. Setup Mock AI Client
    mock_client = MockAIClient()
    
    # 3. Setup Workflow Controller with Mock Client
    controller = AsyncWorkflowController(work_dir=work_dir, ai_client=mock_client)
    
    # 4. Input State
    state = {
        "input_data": {
            "title": "How to Save Money with AI Simulations",
            "keywords": ["AI testing", "Mock client", "cost reduction"],
            "area": "Global Marketing",
            "brand_name": "TestBrand",
            "brand_context": "We provide AI testing tools.",
            "article_size": 800,
            "include_faq": True,
            "include_tables": True
        }
    }
    
    # 5. Run Workflow
    try:
        final_state = await controller.run_workflow(state)
        print("\n=== Simulation Completed Successfully! ===")
        
        # Result Extraction (Workflow Controller returns a flattened dict)
        title = final_state.get("title") or "Title Not Found"
        final_md = final_state.get("final_markdown", "")
        word_count = len(final_md.split()) if final_md else 0
        meta_desc = final_state.get("meta_description") or "N/A"
        
        print(f"\nFINAL SIMULATION REPORT:")
        print(f"--------------------------")
        print(f"- Article Title:    {title}")
        print(f"- Total Word Count: {word_count} words")
        print(f"- Meta Description: {meta_desc}")
        print(f"- Output Folder:    {os.path.abspath(work_dir)}")
        print(f"--------------------------")
        
    except Exception as e:
        print(f"\n!!! Simulation Failed Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_simulation())
