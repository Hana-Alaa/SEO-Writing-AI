import asyncio
import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.append(r"e:\SEO-Writing-AI")

from utils.workflow_logger import WorkflowLogger
from services.workflow_controller import AsyncExecutor

async def test_logging():
    output_dir = "tmp_test_logs"
    os.makedirs(output_dir, exist_ok=True)
    
    logger = WorkflowLogger(output_dir)
    executor = AsyncExecutor()
    
    state = {
        "workflow_logger": logger,
        "input_data": {"key": "initial_value"},
        "test_val": 1
    }
    
    async def success_step(s):
        s["test_val"] += 1
        return s
        
    async def error_step(s):
        raise ValueError("Simulated failure")

    print("Running success step...")
    await executor.run_step("SuccessStep", success_step, state)
    
    print("Running error step...")
    await executor.run_step("ErrorStep", error_step, state)
    
    log_path = os.path.join(output_dir, "workflow.log")
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            content = f.read()
            print("\n--- Log Content ---")
            print(content)
            
            # Check for expected markers
            if "WORKFLOW STEP: SuccessStep" in content and "STEP_INPUT" in content and "STEP_OUTPUT" in content:
                print("\nSUCCESS: SuccessStep logged correctly.")
            else:
                print("\nFAILURE: SuccessStep missing details.")
                
            if "WORKFLOW STEP: ErrorStep" in content and "ERROR: Simulated failure" in content:
                print("SUCCESS: ErrorStep logged correctly.")
            else:
                print("FAILURE: ErrorStep missing details.")
    else:
        print("FAILURE: workflow.log not created.")

if __name__ == "__main__":
    asyncio.run(test_logging())
