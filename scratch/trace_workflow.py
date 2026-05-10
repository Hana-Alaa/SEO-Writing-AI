import asyncio
import logging
import json
import os
import sys
from unittest.mock import MagicMock, AsyncMock

# Setup logging to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
logging.root.addHandler(handler)
logging.root.setLevel(logging.DEBUG)

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.services.workflow_controller import AsyncWorkflowController

async def main():
    # Mock AI Client
    ai_client = AsyncMock()
    ai_client.send.return_value = {
        "content": json.dumps({"outline": []}),
        "metadata": {"prompt": "MOCKED_PROMPT", "response": "{}", "tokens": {}, "duration": 0, "model": "mock"}
    }
    
    # Initialize controller
    controller = AsyncWorkflowController(ai_client=ai_client)
    
    # Mock SERP data
    serp_data = {
        "top_results": [
            {
                "title": "Boulevard City Riyadh Guide",
                "headings": {"h2": ["Tickets", "Hours"]}
            }
        ],
        "lsi_keywords": ["Boulevard City events"]
    }
    
    state = {
        "raw_title": "Boulevard City Riyadh",
        "primary_keyword": "Boulevard City Riyadh",
        "article_language": "en",
        "brand_name": "Tikevent",
        "intent": "informational",
        "heading_only_mode": True,
        "serp_data": serp_data,
        "seo_intelligence": {
            "serp_raw": serp_data,
            "market_analysis": {
                "intent_analysis": {"confirmed_intent": "informational", "dominant_page_type": "guide"},
                "structural_intelligence": {"dominant_heading_pattern": "Guide structure"},
                "market_insights": {
                    "writing_guide": "Write a clear guide.", 
                    "topic_observations": {"core_recurring_topics": [], "secondary_mentions": []}
                }
            }
        }
    }
    
    print("\n--- STEP 0: Analysis/Research ---")
    state = await controller._step_serp_analysis_router(state)
    print(f"Brief in state: {bool(state.get('serp_outline_brief'))}")
    
    print("\n--- STEP 1: Outline Generation ---")
    
    try:
        await controller._step_1_outline(state)
    except Exception as e:
        print(f"Workflow failed (expected): {e}")

if __name__ == "__main__":
    asyncio.run(main())
