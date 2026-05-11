import unittest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from src.services.research_service import ResearchService

class TestResearchFallback(unittest.TestCase):
    def setUp(self):
        self.mock_ai_client = MagicMock()
        # Mock send_with_web to return no results
        self.mock_ai_client.send_with_web = AsyncMock(return_value={
            "content": "{}",
            "metadata": {"tokens": {}, "duration": 0}
        })
        self.research_service = ResearchService(self.mock_ai_client, "output")

    def test_run_web_research_fallback(self):
        state = {
            "primary_keyword": "الفرق بين SEO و SEM",
            "article_language": "ar",
            "competitor_count": 3
        }
        
        # Run research
        loop = asyncio.get_event_loop()
        state = loop.run_until_complete(self.research_service.run_web_research(state))
        
        # Verify no crash
        self.assertIn("serp_data", state)
        serp_data = state["serp_data"]
        
        # Verify fallback markers
        self.assertTrue(serp_data.get("serp_data_unavailable"))
        self.assertEqual(serp_data.get("serp_fallback_reason"), "SERP returned no top results")
        self.assertEqual(serp_data.get("top_results"), [])

    def test_build_serp_outline_brief_fallback(self):
        state = {
            "primary_keyword": "الفرق بين SEO و SEM",
            "article_language": "ar",
            "seo_intelligence": {
                "serp_raw": {
                    "serp_data_unavailable": True,
                    "serp_fallback_reason": "SERP returned no top results"
                }
            }
        }
        
        brief = self.research_service.build_serp_outline_brief(state)
        
        # Verify fallback brief
        self.assertEqual(brief["dominant_search_intent"], "informational")
        self.assertEqual(brief["observed_page_type"], "guide")
        self.assertIn("الفرق بين SEO و SEM", brief["observed_topics"])
        self.assertTrue(brief.get("serp_data_unavailable"))
        self.assertEqual(len(brief["heading_generation_guidance"]), 1)
        self.assertIn("Educational comparison guide", brief["heading_generation_guidance"][0])

if __name__ == "__main__":
    unittest.main()
