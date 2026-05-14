import unittest
from typing import Dict, Any
from src.services.content_generator import OutlineGenerator

class MockAIClient:
    async def send(self, *args, **kwargs):
        return {"content": "{}", "metadata": {}}

class TestSemanticLayer(unittest.TestCase):
    def setUp(self):
        self.generator = OutlineGenerator(MockAIClient())

    def test_pricing_mapping(self):
        # Heading with price keyword
        section = {"heading_text": "أسعار الشقق في الرياض", "section_type": "core"}
        self.generator._normalize_section(section, 1, "brand_commercial", {}, "الرياض")
        self.assertEqual(section["execution_mode"], "market_practical")
        self.assertEqual(section["semantic_goal"], "realistic cost and value expectations")

    def test_location_mapping(self):
        # Taxonomy with location keyword
        section = {"heading_text": "أفضل الأحياء", "taxonomy_axis": "location"}
        self.generator._normalize_section(section, 1, "brand_commercial", {}, "الرياض")
        self.assertEqual(section["execution_mode"], "locality_analysis")
        self.assertIn("lifestyle", section["semantic_goal"])

    def test_type_options_mapping(self):
        # Type/Category section should default to taxonomy_breakdown
        section = {"heading_text": "أنواع الشقق", "section_type": "core"}
        self.generator._normalize_section(section, 1, "brand_commercial", {}, "الرياض")
        self.assertEqual(section["execution_mode"], "taxonomy_breakdown")

    def test_comparison_mapping(self):
        # Heading with comparison keyword
        section = {"heading_text": "مقارنة بين المناطق"}
        self.generator._normalize_section(section, 1, "brand_commercial", {}, "الرياض")
        self.assertEqual(section["execution_mode"], "comparison_decision")

    def test_faq_priority_mapping(self):
        # FAQ should be buyer_guidance even if it mentions price/location
        section = {"heading_text": "سعر الشقة وموقعها", "section_type": "faq"}
        self.generator._normalize_section(section, 1, "brand_commercial", {}, "الرياض")
        self.assertEqual(section["execution_mode"], "buyer_guidance")

    def test_legacy_default_safe_handling(self):
        # Section without any identifying info
        section = {"heading_text": "Generic Section"}
        self.generator._normalize_section(section, 1, "brand_commercial", {}, "الرياض")
        self.assertIn("execution_mode", section)
        self.assertEqual(section["execution_mode"], "taxonomy_breakdown")
        self.assertIn("semantic_goal", section)

if __name__ == "__main__":
    unittest.main()
