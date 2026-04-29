import asyncio
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.services.research_service import ResearchService
from src.utils.link_manager import LinkManager

class MockAI:
    async def send(self, prompt, step=None):
        return {"content": "{}", "metadata": {}}

async def test_canonicalization():
    ai = MockAI()
    service = ResearchService(ai, "output")
    
    brand_url = "https://cems-it.com/"
    # Scenario: Domain suggests "Cems It", but visible brand is "Creative Minds (CEMS)"
    candidates = {
        "visible": ["Creative Minds (CEMS)"],
        "metadata": ["Creative Minds"],
        "domain": ["Cems It"]
    }
    
    print(f"Testing canonicalization for candidates: {candidates}")
    result = service._canonicalize_brand_name(candidates, brand_url)
    print(f"Result: {result}")
    
    # Expected:
    # display_brand_name: Creative Minds
    # official_brand_name: Creative Minds (CEMS)
    # brand_aliases: ['CEMS', 'Cems It']

if __name__ == "__main__":
    asyncio.run(test_canonicalization())
