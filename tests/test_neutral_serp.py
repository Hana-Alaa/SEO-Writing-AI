import asyncio
import json
import unittest
from unittest.mock import MagicMock, AsyncMock
from src.services.research_service import ResearchService

class TestNeutralSerp(unittest.TestCase):
    def setUp(self):
        self.mock_ai = MagicMock()
        self.mock_ai.send = AsyncMock(return_value={"content": "{}", "metadata": {}})
        self.research_service = ResearchService(self.mock_ai, ".")

    async def async_test_neutral_payload_isolation(self):
        # State with brand data
        state = {
            "primary_keyword": "best coffee machine",
            "brand_name": "PremiumCoffee",
            "brand_context": "A leading coffee brand in Riyadh.",
            "serp_data": {
                "top_results": [{"title": "Competitor 1", "url": "https://comp1.com"}],
                "paa_questions": ["How to clean coffee machine?"],
                "lsi_keywords": ["espresso maker", "brew quality"],
                "structural_stats": {"avg_h2": 5}
            }
        }

        # Run analysis
        await self.research_service.run_serp_analysis(state)

        # Capture the prompt sent to AI
        call_args = self.mock_ai.send.call_args
        prompt_content = call_args[0][0]

        # Verify: No brand info in prompt
        self.assertNotIn("PremiumCoffee", prompt_content)
        self.assertNotIn("Riyadh", prompt_content)
        self.assertIn("best coffee machine", prompt_content)
        
        # Verify: Downstream brand still exists in state
        self.assertEqual(state["brand_name"], "PremiumCoffee")

    async def async_test_intent_firewall_neutrality(self):
        # Keyword with clear commercial intent ("best", "company")
        state = {
            "primary_keyword": "افضل شركة تصميم مواقع", # Best web design company
            "serp_data": {"top_results": []}
        }
        
        # Mock AI returning informational intent
        self.mock_ai.send = AsyncMock(return_value={
            "content": json.dumps({"intent_analysis": {"confirmed_intent": "informational"}}),
            "metadata": {}
        })

        await self.research_service.run_serp_analysis(state)
        
        # Verify: Firewall forced it to commercial due to keyword
        market_analysis = state["seo_intelligence"]["market_analysis"]
        self.assertEqual(market_analysis["intent_analysis"]["confirmed_intent"], "commercial")

    def test_neutral_payload_isolation(self):
        asyncio.run(self.async_test_neutral_payload_isolation())

    def test_intent_firewall_neutrality(self):
        asyncio.run(self.async_test_intent_firewall_neutrality())

if __name__ == "__main__":
    unittest.main()
