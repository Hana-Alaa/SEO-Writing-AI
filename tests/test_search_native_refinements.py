import unittest
from src.services.serp_topic_miner import SERPTopicMiner
from src.services.outline_repair_service import OutlineRepairService

class TestHeadingRefinement(unittest.TestCase):
    def setUp(self):
        self.miner = SERPTopicMiner()
        self.repair = OutlineRepairService()

    def test_filler_reduction(self):
        # English
        self.assertEqual(self.miner.clean_heading_filler("Boulevard City - complete guide", "en"), "Boulevard City")
        self.assertEqual(self.miner.clean_heading_filler("Riyadh Season: integrated experience", "en"), "Riyadh Season")
        self.assertEqual(self.miner.clean_heading_filler("Top attractions | overview", "en"), "Top attractions")
        
        # Arabic
        self.assertEqual(self.miner.clean_heading_filler("بوليفارد سيتي - دليل شامل", "ar"), "بوليفارد سيتي")
        self.assertEqual(self.miner.clean_heading_filler("موسم الرياض | نظرة عامة", "ar"), "موسم الرياض")
        self.assertEqual(self.miner.clean_heading_filler("أهم المعالم - شامل", "ar"), "أهم المعالم")

    def test_brand_utility_guidance(self):
        topics = [{"topic": "Booking tickets", "type": "visitor_info"}]
        guidance = self.miner.generate_brand_utility_guidance(topics, "Tikevent", "en")
        self.assertIn("Tikevent", guidance)
        self.assertIn("How to complete your booking via Tikevent", guidance)

    def test_prevent_over_splitting_parking_suppressed(self):
        # Case: Location exists, Parking exists as H3, but NO SERP support for parking
        outline = [
            {
                "heading_text": "Location and Access",
                "heading_level": "H2",
                "section_type": "visitor_information",
                "subheadings": ["How to get there", "Parking at the venue"]
            }
        ]
        serp_brief = {
            "observed_topics": ["Location", "Directions"],
            "secondary_keyword_phrases": [],
            "heading_candidates": []
        }
        
        repaired = self.repair.promote_visitor_intents(outline, "Venue", "Venue", serp_brief)
        
        # Expect: "Parking at the venue" remains as H3 because no SERP support to promote to H2
        # And "Location and Access" might be promoted if detected (it is already H2 but generically handled)
        
        # Check if any parking H2 was created
        parking_h2s = [s for s in repaired if "Parking" in s["heading_text"] and s["heading_level"] == "H2"]
        self.assertEqual(len(parking_h2s), 0, "Parking should not have been promoted to H2")

    def test_prevent_over_splitting_parking_promoted(self):
        # Case: Location exists, Parking exists as H3, AND STRONG SERP support for parking
        outline = [
            {
                "heading_text": "Location and Access",
                "heading_level": "H2",
                "section_type": "visitor_information",
                "subheadings": ["How to get there", "Parking at the venue"]
            }
        ]
        serp_brief = {
            "observed_topics": [{"topic": "Parking", "type": "visitor_info"}],
            "secondary_keyword_phrases": ["parking in Venue"],
            "heading_candidates": ["Parking at Venue"]
        }
        
        repaired = self.repair.promote_visitor_intents(outline, "Venue", "Venue", serp_brief)
        
        # Expect: "Parking at the venue" is promoted to H2
        parking_h2s = [s for s in repaired if "Parking" in s["heading_text"] and s["heading_level"] == "H2"]
        self.assertEqual(len(parking_h2s), 1, "Parking should have been promoted due to SERP support")

    def test_search_native_phrasing_informational(self):
        # This is hard to test deterministically without a full AI pass, 
        # but we can verify the prompt contains the new guidance.
        with open("f:/SEO-Writing-AI/assets/prompts/templates/01_outline_generator_heading_only_informational_v2.txt", "r", encoding="utf-8") as f:
            prompt = f.read()
        
        self.assertIn("PREFER incorporating it directly into the relevant heading", prompt)
        self.assertIn("Do not add decorative tails", prompt)
        self.assertIn("CONTEXTUAL BRAND UTILITY", prompt)
        self.assertIn("one utility-oriented FAQ question can naturally reference the brand/platform", prompt)
        self.assertNotIn("If useful, include ONE FAQ question inspired by", prompt)
        self.assertIn("FAQ questions should preferably cover unresolved practical actions", prompt)
        self.assertIn("summarize a practical reader takeaway, planning consideration", prompt)

if __name__ == "__main__":
    unittest.main()
