import unittest
from src.services.workflow_controller import AsyncWorkflowController

class TestTopicPackEnrichment(unittest.TestCase):
    def setUp(self):
        # We only need the controller for its logic, not for real execution
        self.controller = AsyncWorkflowController(
            work_dir="."
        )

    def test_rental_pack_activation_by_keyword(self):
        state = {
            "primary_keyword": "شقق للايجار في الرياض",
            "topic_packs_enabled": True
        }
        active = self.controller._detect_active_topic_packs(state)
        self.assertIn("rental_real_estate_pack", active)

    def test_rental_pack_activation_by_serp(self):
        state = {
            "primary_keyword": "سكن الرياض",
            "serp_data": {"paa_questions": ["How to rent an apartment?"]},
            "topic_packs_enabled": True
        }
        active = self.controller._detect_active_topic_packs(state)
        self.assertIn("rental_real_estate_pack", active)

    def test_no_activation_for_unrelated_topic(self):
        state = {
            "primary_keyword": "كيفية طبخ المكبوس",
            "topic_packs_enabled": True
        }
        active = self.controller._detect_active_topic_packs(state)
        self.assertEqual(len(active), 0)

    def test_enrichment_details_injected(self):
        state = {
            "primary_keyword": "شقق للايجار",
            "article_language": "ar",
            "topic_packs_enabled": True
        }
        section = {
            "heading_text": "أسعار الشقق",
            "section_type": "pricing"
        }
        # Mock _infer_taxonomy_axis if needed, but it should return 'pricing'
        
        enriched = self.controller._enrich_section_contract(
            section=section,
            outline=[section],
            index=0,
            state=state
        )
        
        details = enriched.get("must_include_details", [])
        # Check for rental-specific price drivers
        self.assertTrue(any("محركات الأسعار" in d for d in details))
        self.assertTrue(any("الشهري مقابل السنوي" in d for d in details))

if __name__ == "__main__":
    unittest.main()
