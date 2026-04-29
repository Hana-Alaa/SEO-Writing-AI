import asyncio
import json
import unittest
from unittest.mock import MagicMock, AsyncMock
from src.services.research_service import ResearchService

class TestSerpContracts(unittest.TestCase):
    def setUp(self):
        self.mock_ai = MagicMock()
        self.mock_ai.send = AsyncMock()
        self.research_service = ResearchService(self.mock_ai, ".")

    async def async_test_enrichment_source_preservation(self):
        # AI returns specific sources
        serp_data = {
            "paa_questions": ["Q1"],
            "serp_enrichment_sources": {
                "paa_questions": "scraping_failed", # AI says scraping failed but somehow has Q1 (maybe cached)
                "related_searches": "not_observed"
            },
            "top_results": []
        }
        
        enriched = self.research_service._enrich_serp_enrichment_signals(serp_data)
        # Should preserve "scraping_failed" even though paa_questions exists
        self.assertEqual(enriched["serp_enrichment_sources"]["paa_questions"], "scraping_failed")
        self.assertEqual(enriched["serp_enrichment_sources"]["related_searches"], "not_observed")

    async def async_test_structural_stats_override(self):
        # Pre-computed stats
        state = {
            "primary_keyword": "coffee",
            "serp_data": {
                "top_results": [],
                "structural_stats": {
                    "avg_h2_count": 10.5,
                    "avg_h3_count": 5.0,
                    "total_h2_count": 31,
                    "total_h3_count": 15,
                    "heading_data_missing": False
                }
            }
        }
        
        # AI returns hallucinated stats
        self.mock_ai.send.return_value = {
            "content": json.dumps({
                "structural_intelligence": {
                    "avg_h2_count": 2, # Hallucination
                    "avg_h3_count": 1, # Hallucination
                    "dominant_page_type": "guide"
                }
            }),
            "metadata": {}
        }

        await self.research_service.run_serp_analysis(state)
        
        intelligence = state["seo_intelligence"]["market_analysis"]["structural_intelligence"]
        # Should be overridden by deterministic values
        self.assertEqual(intelligence["avg_h2_count"], 10.5)
        self.assertEqual(intelligence["avg_h3_count"], 5.0)
        self.assertEqual(intelligence["total_h2_count"], 31)
        self.assertEqual(intelligence["dominant_page_type"], "guide") # Non-heading field preserved

    def test_enrichment_source_preservation(self):
        asyncio.run(self.async_test_enrichment_source_preservation())

    def test_structural_stats_override(self):
        asyncio.run(self.async_test_structural_stats_override())

if __name__ == "__main__":
    unittest.main()
