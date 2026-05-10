import unittest
from src.services.validation_service import ValidationService

class TestExperienceValidation(unittest.TestCase):
    def setUp(self):
        self.validator = ValidationService()

    def test_is_experience_based_topic(self):
        serp_brief = {"must_consider_sections": ["Location", "Tickets"]}
        strategy = {"subtype": "attraction"}
        
        self.assertTrue(self.validator._is_experience_based_topic("Boulevard City", serp_brief, strategy))
        
        # Test with PK signal
        self.assertTrue(self.validator._is_experience_based_topic("Museum of the Future", {}, {}))
        
        # Test with neutral topic
        self.assertFalse(self.validator._is_experience_based_topic("How to write SEO", {}, {}))

    def test_evaluate_outline_coverage_experience(self):
        # Experience topic should not require 'why_it_matters'
        outline = [
            {"heading_text": "Intro", "heading_level": "H2", "section_type": "introduction"},
            {"heading_text": "What is it", "heading_level": "H2", "section_type": "definition"},
            {"heading_text": "Our Activities", "heading_level": "H2", "section_type": "activities"}, # maps to examples_or_use_cases
            {"heading_text": "Tips", "heading_level": "H2", "section_type": "tips"}, # maps to common_mistakes
            {"heading_text": "FAQ", "heading_level": "H2", "section_type": "faq"},
            {"heading_text": "Conclusion", "heading_level": "H2", "section_type": "conclusion"}
        ]
        
        serp_brief = {"must_consider_sections": ["Activities"]}
        strategy = {"subtype": "place"}
        
        results = self.validator.evaluate_outline_coverage(
            outline, 
            "informational", 
            "Boulevard City", 
            serp_brief, 
            strategy
        )
        
        # 'why_it_matters' should be covered (auto-pass)
        self.assertIn("why_it_matters", results["covered"])
        # 'examples_or_tips' should be covered by 'activities'
        self.assertIn("examples_or_tips", results["covered"])
        # 'common_mistakes' should be covered by 'tips'
        self.assertIn("common_mistakes", results["covered"])

if __name__ == "__main__":
    unittest.main()
