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

async def run_tests():
    ai = MockAI()
    service = ResearchService(ai, "output")
    
    print("--- Test 1: Priority (Visible > Metadata > Domain) ---")
    brand_url = "https://cems-it.com/"
    candidates1 = {
        "visible": ["Visible Brand"],
        "metadata": ["Metadata Brand"],
        "domain": ["Cems It"]
    }
    res1 = service._canonicalize_brand_name(candidates1, brand_url)
    print(f"Result: {res1['display_brand_name']}")
    assert res1['display_brand_name'] == "Visible Brand"
    
    print("\n--- Test 2: Fallback to Metadata ---")
    candidates2 = {
        "metadata": ["Metadata Brand"],
        "domain": ["Cems It"]
    }
    res2 = service._canonicalize_brand_name(candidates2, brand_url)
    print(f"Result: {res2['display_brand_name']}")
    assert res2['display_brand_name'] == "Metadata Brand"
    
    print("\n--- Test 3: Fallback to Domain ---")
    candidates3 = {
        "domain": ["Cems It"]
    }
    res3 = service._canonicalize_brand_name(candidates3, brand_url)
    print(f"Result: {res3['display_brand_name']}")
    assert res3['display_brand_name'] == "Cems It"
    
    print("\n--- Test 4: Acronym Pattern ---")
    candidates4 = {
        "visible": ["Creative Minds (CEMS)"]
    }
    res4 = service._canonicalize_brand_name(candidates4, brand_url)
    print(f"Display: {res4['display_brand_name']}, Aliases: {res4['brand_aliases']}")
    assert res4['display_brand_name'] == "Creative Minds"
    assert "CEMS" in res4['brand_aliases']

    print("\nAll brand canonicalization tests passed!")

if __name__ == "__main__":
    asyncio.run(run_tests())
