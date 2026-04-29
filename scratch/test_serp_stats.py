import asyncio
import json
import logging
import sys
import os
from typing import Dict, Any

# Mocking parts of the system for testing
class MockAIClient:
    async def send(self, prompt: str, step: str) -> Dict[str, Any]:
        # Return a sample JSON response
        return {
            "content": json.dumps({
                "structural_intelligence": {
                    "avg_h2_count": 0, # Should be overridden
                    "avg_h3_count": 0  # Should be overridden
                }
            }),
            "metadata": {"tokens": {}, "duration": 0}
        }

class MockScraperUtils:
    @staticmethod
    async def fetch_headings_from_url(url: str, timeout: int = 15):
        if "comp1" in url:
            return [{"tag": "H2", "text": "H2-1"}, {"tag": "H2", "text": "H2-2"}, {"tag": "H3", "text": "H3-1"}]
        if "comp2" in url:
            return [{"tag": "H2", "text": "H2-1"}, {"tag": "H3", "text": "H3-1"}, {"tag": "H3", "text": "H3-2"}]
        # Simulate scraping failure for other URLs
        return []

# Adjust path to import the actual ResearchService
sys.path.append(os.getcwd())
from src.services.research_service import ResearchService

# Monkeypatching ScraperUtils for the test
import src.services.research_service
import src.utils.scraper_utils
src.services.research_service.ScraperUtils = MockScraperUtils

async def test_serp_stats_with_fallback():
    logging.basicConfig(level=logging.INFO)
    ai_client = MockAIClient()
    service = ResearchService(ai_client, "test_workdir")
    
    # Test 1: Successful scraping (Primary source)
    state = {
        "primary_keyword": "test keyword",
        "serp_data": {
            "top_results": [
                {"url": "https://comp1.com", "title": "Comp 1"},
                {"url": "https://comp2.com", "title": "Comp 2"}
            ]
        }
    }
    
    print("Running Test 1: Successful scraping...")
    new_state = await service.run_serp_analysis(state)
    stats = new_state["seo_intelligence"]["market_analysis"]["structural_intelligence"]
    print(f"Stats (Primary): {json.dumps(stats, indent=2)}")
    assert stats["avg_h2_count"] == 1.5
    assert stats["heading_data_missing"] == False

    # Test 2: Failed scraping but data exists in serp_data (Fallback)
    state_fallback = {
        "primary_keyword": "test keyword",
        "serp_data": {
            "top_results": [
                {
                    "url": "https://fail.com", 
                    "title": "Failed Scrape",
                    "headings": {
                        "h2": ["AI-H2-1", "AI-H2-2"],
                        "h3": ["AI-H3-1"]
                    }
                }
            ]
        }
    }
    print("\nRunning Test 2: Failed scraping with serp_data fallback...")
    new_state_fb = await service.run_serp_analysis(state_fallback)
    stats_fb = new_state_fb["seo_intelligence"]["market_analysis"]["structural_intelligence"]
    print(f"Stats (Fallback): {json.dumps(stats_fb, indent=2)}")
    assert stats_fb["avg_h2_count"] == 2.0
    assert stats_fb["heading_data_missing"] == False
    print("Test 2 Passed!")

    # Test 3: Both fail
    state_empty = {
        "primary_keyword": "test keyword",
        "serp_data": {
            "top_results": [
                {"url": "https://empty.com", "title": "Empty"}
            ]
        }
    }
    print("\nRunning Test 3: Total failure (Zero stats)...")
    new_state_empty = await service.run_serp_analysis(state_empty)
    stats_empty = new_state_empty["seo_intelligence"]["market_analysis"]["structural_intelligence"]
    print(f"Stats Empty: {json.dumps(stats_empty, indent=2)}")
    assert stats_empty["avg_h2_count"] == 0
    assert stats_empty["heading_data_missing"] == True
    print("Test 3 Passed!")

if __name__ == "__main__":
    asyncio.run(test_serp_stats_with_fallback())
