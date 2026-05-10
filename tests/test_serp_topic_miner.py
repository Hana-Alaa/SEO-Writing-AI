import unittest
from src.services.serp_topic_miner import SERPTopicMiner

class TestSERPTopicMiner(unittest.TestCase):
    def setUp(self):
        self.miner = SERPTopicMiner()

    def test_mine_topics_return_type(self):
        serp_data = {"top_results": [{"title": "Test"}]}
        results = self.miner.mine_topics(serp_data, "Test")
        self.assertIsInstance(results, dict)
        self.assertIn("topics", results)
        self.assertIn("secondary_keyword_phrases", results)
        self.assertIn("heading_candidates", results)
        self.assertIn("guidance", results)

    def test_mine_visitor_intent_ar(self):
        serp_data = {
            "top_results": [
                {
                    "title": "بوليفارد سيتي الرياض: التذاكر والمواعيد",
                    "headings": {
                        "h2": ["أوقات العمل في بوليفارد سيتي", "أسعار التذاكر", "موقع البوليفارد"]
                    }
                }
            ]
        }
        results = self.miner.mine_topics(serp_data, "بوليفارد سيتي")
        topic_labels = [t["topic"] for t in results["topics"]]
        
        self.assertIn("أوقات العمل في بوليفارد سيتي", topic_labels)
        self.assertIn("أسعار التذاكر", topic_labels)
        self.assertIn("موقع البوليفارد", topic_labels)

    def test_mine_attribute_topics(self):
        serp_data = {
            "top_results": [
                {"title": "Best Cleaning Services in Dubai", "headings": {"h2": ["Professional Cleaning Services", "Affordable Cleaning Dubai"]}},
                {"title": "Top Cleaning Services Dubai", "headings": {"h2": ["Professional Cleaning Services", "Residential Cleaning Dubai"]}}
            ]
        }
        results = self.miner.mine_topics(serp_data, "Cleaning Services")
        attribute_topics = [t["topic"] for t in results["topics"] if t["type"] == "attribute"]
        self.assertIn("Professional Cleaning", attribute_topics)

    def test_clean_heading_filler(self):
        self.assertEqual(self.miner.clean_heading_filler("ما هي الحديقة؟ دليل شامل", "ar"), "ما هي الحديقة؟")
        self.assertEqual(self.miner.clean_heading_filler("Welcome to the park - complete guide", "en"), "Welcome to the park")
        # Should NOT trim if result is too short (e.g. 1 word left)
        self.assertEqual(self.miner.clean_heading_filler("Park: unique experience", "en"), "Park: unique experience")

    def test_mine_secondary_phrases(self):
        serp_data = {
            "top_results": [{"title": "حجز تذاكر الحديقة", "snippet": "أسعار تذاكر الحديقة لعام 2024"}],
            "lsi_keywords": ["مواعيد الحديقة"]
        }
        results = self.miner.mine_topics(serp_data, "الحديقة")
        phrases = results["secondary_keyword_phrases"]
        self.assertIn("مواعيد الحديقة", phrases)
        # Regex matches "حجز تذاكر"
        self.assertTrue(any("حجز" in p for p in phrases))

    def test_brand_utility_guidance(self):
        topics = [{"topic": "حجز تذاكر", "type": "visitor_info"}]
        guidance = self.miner.generate_brand_utility_guidance(topics, "Tikevent", "ar")
        self.assertIn("Tikevent", guidance)
        self.assertIn("الحجز", guidance)

    def test_no_overfitting_generic_topic(self):
        # Topic: "How to improve programming skills"
        serp_data = {
            "top_results": [
                {"title": "10 Ways to Improve Your Programming Skills", "headings": {"h2": ["Practice Coding Daily", "Read Clean Code - complete guide"]}}
            ]
        }
        results = self.miner.mine_topics(serp_data, "Programming Skills")
        
        # Should still trim filler
        self.assertTrue(any("Trim filler" in g for g in results["guidance"]))
        
        # Should NOT have visitor intent unless signals are present
        topics = [t["topic"] for t in results["topics"]]
        self.assertNotIn("Tickets", topics)
        self.assertNotIn("Location", topics)

if __name__ == "__main__":
    unittest.main()
