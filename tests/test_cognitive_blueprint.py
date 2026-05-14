import unittest
import asyncio
from typing import Dict, Any
from src.services.content_generator import SectionWriter

class MockAIClient:
    async def send(self, *args, **kwargs):
        return {"content": "{}", "metadata": {}}

class TestCognitiveBlueprint(unittest.TestCase):
    def setUp(self):
        self.writer = SectionWriter(MockAIClient())

    def test_market_practical_blueprint(self):
        section = {
            "execution_mode": "market_practical",
            "taxonomy_axis": "area",
            "observed_data_mentions": ["35000 SAR"]
        }
        blueprint = self.writer._build_cognitive_blueprint(section)
        
        self.assertIn("cost-to-value", blueprint["section_thesis"])
        self.assertTrue(any("budget tradeoffs" in d for d in blueprint["decision_logic"]))
        self.assertTrue(any("35000 SAR" in e for e in blueprint["evidence_plan"]))
        self.assertIn("unsupported exact prices", blueprint["avoid_patterns"])

    def test_locality_analysis_blueprint(self):
        section = {
            "execution_mode": "locality_analysis",
            "must_include_details": ["Metro Access"]
        }
        blueprint = self.writer._build_cognitive_blueprint(section)
        
        self.assertIn("lifestyle and needs", blueprint["section_thesis"])
        self.assertTrue(any("service accessibility" in d for d in blueprint["decision_logic"]))
        self.assertTrue(any("Metro Access" in e for e in blueprint["evidence_plan"]))
        self.assertIn("pure geographic distance lists", blueprint["avoid_patterns"])

    def test_taxonomy_breakdown_blueprint(self):
        section = {
            "execution_mode": "taxonomy_breakdown"
        }
        blueprint = self.writer._build_cognitive_blueprint(section)
        
        self.assertIn("functional and situational fit", blueprint["section_thesis"])
        self.assertTrue(any("user situation" in d for d in blueprint["decision_logic"]))
        self.assertIn("overlapping category definitions", blueprint["avoid_patterns"])

    def test_comparison_decision_blueprint(self):
        section = {
            "execution_mode": "comparison_decision"
        }
        blueprint = self.writer._build_cognitive_blueprint(section)
        
        self.assertIn("core tradeoffs", blueprint["section_thesis"])
        self.assertTrue(any("win' conditions" in d for d in blueprint["decision_logic"]))
        self.assertIn("vague 'both are good' conclusions", blueprint["avoid_patterns"])

    def test_buyer_guidance_blueprint(self):
        section = {
            "execution_mode": "buyer_guidance"
        }
        blueprint = self.writer._build_cognitive_blueprint(section)
        
        self.assertIn("reduce selection friction", blueprint["section_thesis"])
        self.assertTrue(any("Process walk-through" in d for d in blueprint["decision_logic"]))
        self.assertIn("encyclopedic advice", blueprint["avoid_patterns"])

    def test_missing_metadata_fallback(self):
        # Empty metadata should safely fall back to default taxonomy behavior
        section = {}
        blueprint = self.writer._build_cognitive_blueprint(section)
        self.assertEqual(blueprint["section_thesis"], "Explain the topic to help the reader understand their options.")
        self.assertIn("Clear classification", blueprint["decision_logic"])

    def test_payload_injection(self):
        # Verify that write() injects the blueprint into the template kwargs
        section = {"execution_mode": "market_practical"}
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        class MockTemplate:
            def render(self, **kwargs):
                self.last_kwargs = kwargs
                return "Rendered"
        
        mock_template = MockTemplate()
        self.writer.env.get_template = lambda x: mock_template
        
        try:
            loop.run_until_complete(self.writer.write(
                title="Test", global_keywords={}, section=section,
                article_intent="Commercial", seo_intelligence={}, content_type="brand_commercial",
                link_strategy="internal", brand_url="", brand_link_used=False, brand_link_allowed=True,
                allow_external_links=True, execution_plan={}, area="الرياض"
            ))
            
            self.assertIn("cognitive_blueprint", mock_template.last_kwargs)
            self.assertEqual(mock_template.last_kwargs["cognitive_blueprint"]["section_thesis"], 
                            "Clarify the realistic cost-to-value relationship for the reader's budget.")
        finally:
            loop.close()

if __name__ == "__main__":
    unittest.main()
