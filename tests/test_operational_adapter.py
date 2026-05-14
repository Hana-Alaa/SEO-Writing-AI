import unittest
import asyncio
from typing import Dict, Any
from src.services.content_generator import SectionWriter

class MockAIClient:
    async def send(self, *args, **kwargs):
        return {"content": "{}", "metadata": {}}

class TestOperationalAdapter(unittest.TestCase):
    def setUp(self):
        self.writer = SectionWriter(MockAIClient())

    def test_market_practical_pricing_section(self):
        section = {
            "execution_mode": "market_practical",
            "taxonomy_axis": "area",
            "forbidden_taxonomy_axis": "category_or_type",
            "observed_data_mentions": ["35000 SAR"]
        }
        instr = self.writer._build_operational_instructions(section)
        
        # Verify pricing-specific instructions
        self.assertTrue(any("prices vary" in i for i in instr))
        self.assertTrue(any("affordability primarily by the area axis" in i for i in instr))
        self.assertTrue(any("cautious grounding hints" in i for i in instr))
        self.assertTrue(any("Strictly avoid using the 'category_or_type' axis" in i for i in instr))

    def test_locality_analysis_section(self):
        section = {
            "execution_mode": "locality_analysis"
        }
        instr = self.writer._build_operational_instructions(section)
        
        # Verify locality-specific instructions
        self.assertTrue(any("resident lifestyle needs" in i for i in instr))
        self.assertTrue(any("accessibility, services, commute" in i for i in instr))
        self.assertTrue(any("Avoid pure geographic listing" in i for i in instr))

    def test_taxonomy_breakdown_section(self):
        section = {
            "execution_mode": "taxonomy_breakdown",
            "heading_text": "Apartment Options"
        }
        instr = self.writer._build_operational_instructions(section)
        
        # Verify taxonomy-specific instructions
        self.assertTrue(any("clear, non-overlapping logic" in i for i in instr))
        self.assertTrue(any("differences between categories" in i for i in instr))
        self.assertTrue(any("match each category to a specific user situation" in i for i in instr))
        self.assertTrue(any("Avoid pricing-first logic" in i for i in instr))

    def test_observed_data_grounding(self):
        section = {
            "observed_data_mentions": ["High Demand", "5% increase"]
        }
        instr = self.writer._build_operational_instructions(section)
        self.assertTrue(any("cautious grounding hints" in i for i in instr))
        self.assertTrue(any("NOT present these data points as definitive market statistics" in i for i in instr))

    def test_missing_metadata_defaults(self):
        # Empty dict should not crash and should return default taxonomy behavior
        section = {}
        try:
            instr = self.writer._build_operational_instructions(section)
            self.assertIsInstance(instr, list)
            # Default mode is taxonomy_breakdown, which adds 3-4 instructions
            self.assertGreater(len(instr), 0)
            self.assertTrue(any("non-overlapping logic" in i for i in instr))
        except Exception as e:
            self.fail(f"Adapter crashed on empty metadata: {e}")

    def test_writer_payload_inclusion(self):
        # Verify that write() actually builds and uses these instructions
        section = {
            "execution_mode": "market_practical",
            "heading_text": "Prices"
        }
        
        # We'll use a small helper to run the async write method
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Mock template to capture kwargs
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
            
            payload = mock_template.last_kwargs
            self.assertIn("operational_instructions", payload)
            self.assertIsInstance(payload["operational_instructions"], list)
            self.assertGreater(len(payload["operational_instructions"]), 0)
        finally:
            loop.close()

if __name__ == "__main__":
    unittest.main()
