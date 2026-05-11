import unittest
from src.services.outline_repair_service import OutlineRepairService

class TestOutlineRepairService(unittest.TestCase):
    def setUp(self):
        self.repair_service = OutlineRepairService()

    def test_promote_visitor_intents(self):
        outline = [
            {
                "heading_text": "Introduction",
                "heading_level": "INTRO",
                "section_type": "introduction",
                "section_id": "sec_1"
            },
            {
                "heading_text": "Visitor Information",
                "heading_level": "H2",
                "section_type": "visitor_information",
                "section_id": "sec_2",
                "subheadings": ["Location and Map", "Opening Hours", "Ticket Prices"]
            },
            {
                "heading_text": "Conclusion",
                "heading_level": "H2",
                "section_type": "conclusion",
                "section_id": "sec_3"
            }
        ]
        
        repaired = self.repair_service.promote_visitor_intents(outline, "Boulevard City", "Boulevard City")
        
        # Check that H3s were promoted
        h2_texts = [s["heading_text"] for s in repaired if s["heading_level"] == "H2"]
        self.assertIn("Location of Boulevard City and How to Get There", h2_texts)
        self.assertIn("Opening Hours and Best Time to Visit Boulevard City", h2_texts)
        self.assertIn("Ticket Prices and Booking for Boulevard City", h2_texts)
        
        # Check that original Visitor Information H2 is gone (since all H3s were promoted)
        self.assertNotIn("Visitor Information", h2_texts)
        
        # Check IDs are resequenced
        ids = [s["section_id"] for s in repaired]
        self.assertEqual(ids, ["sec_1", "sec_2", "sec_3", "sec_4", "sec_5"])

    def test_enrich_brand_utility_faq_positive(self):
        outline = [
            {"section_type": "visitor_information", "heading_text": "Info"},
            {"section_type": "faq", "heading_text": "FAQ", "subheadings": ["Q1"]}
        ]
        serp_brief = {"brand_utility_candidates": ["How to book via BrandX"]}
        repaired = self.repair_service.enrich_brand_utility_faq(outline, serp_brief, "BrandX", "informational")
        # FAQ is now index 1
        self.assertEqual(len(repaired[1]["subheadings"]), 2)
        self.assertEqual(repaired[1]["subheadings"][1]["heading_text"], "How to book via BrandX")

    def test_enrich_brand_utility_faq_duplicate(self):
        outline = [
            {"section_type": "pricing", "heading_text": "Prices"},
            {"section_type": "faq", "heading_text": "FAQ", "subheadings": ["How to use BrandX?"]}
        ]
        serp_brief = {"brand_utility_candidates": ["How to book via BrandX"]}
        repaired = self.repair_service.enrich_brand_utility_faq(outline, serp_brief, "BrandX", "informational")
        self.assertEqual(len(repaired[1]["subheadings"]), 1)

    def test_enrich_brand_utility_faq_replaces_generic_arabic_platform(self):
        outline = [
            {"section_type": "visitor_information", "heading_text": "Info"},
            {
                "section_type": "faq",
                "heading_text": "FAQ",
                "subheadings": [
                    "\u0647\u0644 \u064a\u0645\u0643\u0646 \u062d\u062c\u0632 \u062a\u0630\u0627\u0643\u0631 \u0628\u0648\u0644\u064a\u0641\u0627\u0631\u062f \u0633\u064a\u062a\u064a \u0627\u0644\u0631\u064a\u0627\u0636 \u0639\u0628\u0631 \u0627\u0644\u0645\u0646\u0635\u0629 \u0627\u0644\u0631\u0633\u0645\u064a\u0629\u061f"
                ],
            }
        ]
        repaired = self.repair_service.enrich_brand_utility_faq(
            outline,
            {},
            "\u062a\u064a\u0643 \u0627\u064a\u0641\u064a\u0646\u062a",
            "informational",
            "\u0628\u0648\u0644\u064a\u0641\u0627\u0631\u062f \u0633\u064a\u062a\u064a",
        )
        self.assertIn(
            "\u062a\u064a\u0643 \u0627\u064a\u0641\u064a\u0646\u062a",
            str(repaired[1]["subheadings"][0]),
        )
        self.assertNotIn(
            "\u0627\u0644\u0645\u0646\u0635\u0629 \u0627\u0644\u0631\u0633\u0645\u064a\u0629",
            str(repaired[1]["subheadings"][0]),
        )

    def test_enrich_brand_utility_faq_safety_banned_phrase(self):
        outline = [
            {"section_type": "pricing", "heading_text": "Prices"},
            {"section_type": "faq", "heading_text": "FAQ", "subheadings": ["Q1"]}
        ]
        serp_brief = {"brand_utility_candidates": ["أفضل منصة لحجز التذاكر"]}
        repaired = self.repair_service.enrich_brand_utility_faq(outline, serp_brief, "BrandX", "informational")
        self.assertEqual(len(repaired[1]["subheadings"]), 1)

    def test_enrich_brand_utility_faq_commercial_rejected(self):
        outline = [{"section_type": "faq", "heading_text": "FAQ", "subheadings": ["Q1"]}]
        serp_brief = {"brand_utility_candidates": ["How to book via BrandX"]}
        repaired = self.repair_service.enrich_brand_utility_faq(outline, serp_brief, "BrandX", "commercial")
        self.assertEqual(len(repaired[0]["subheadings"]), 1)

    def test_enrich_brand_utility_faq_replaces_weak_when_full(self):
        outline = [
            {"section_type": "offer", "heading_text": "Offer"},
            {"section_type": "faq", "heading_text": "FAQ", "subheadings": ["price 1", "ticket 2", "book 3", "location 4", "general weak question"]}
        ]
        serp_brief = {"brand_utility_candidates": ["How to book via BrandX"]}
        repaired = self.repair_service.enrich_brand_utility_faq(outline, serp_brief, "BrandX", "informational")
        self.assertEqual(len(repaired[1]["subheadings"]), 5)
        # The last one should be replaced since it's weak
        replaced = repaired[1]["subheadings"][4]
        replaced_text = replaced["heading_text"] if isinstance(replaced, dict) else str(replaced)
        self.assertEqual(replaced_text, "How to book via BrandX")

    def test_enrich_brand_utility_faq_skips_when_full_and_strong(self):
        outline = [
            {"section_type": "pricing", "heading_text": "Prices"},
            {"section_type": "faq", "heading_text": "FAQ", "subheadings": ["price 1", "ticket 2", "book 3", "location 4", "hour 5"]}
        ]
        serp_brief = {"brand_utility_candidates": ["How to book via BrandX"]}
        repaired = self.repair_service.enrich_brand_utility_faq(outline, serp_brief, "BrandX", "informational")
        self.assertEqual(len(repaired[1]["subheadings"]), 5)
        # Should not insert since all are strong
        self.assertEqual(repaired[1]["subheadings"][4], "hour 5")

    def test_dedupe_faq_against_h2_removes_duplicate_hours(self):
        outline = [
            {"heading_level": "H2", "heading_text": "مواعيد عمل الحديقة"},
            {"section_type": "faq", "heading_text": "أسئلة شائعة", "subheadings": ["ما هي أوقات العمل؟", "متى يفتح؟"]}
        ]
        repaired = self.repair_service.dedupe_faq_against_h2(outline)
        self.assertEqual(len(repaired[1]["subheadings"]), 0)

    def test_dedupe_faq_against_h2_removes_duplicate_activities(self):
        outline = [
            {"heading_level": "H2", "heading_text": "أهم الأنشطة"},
            {"section_type": "faq", "heading_text": "أسئلة شائعة", "subheadings": ["ما هي أبرز الأنشطة؟"]}
        ]
        repaired = self.repair_service.dedupe_faq_against_h2(outline)
        self.assertEqual(len(repaired[1]["subheadings"]), 0)

    def test_dedupe_faq_against_h2_preserves_distinct_variation(self):
        outline = [
            {"heading_level": "H2", "heading_text": "أسعار التذاكر"},
            {"section_type": "faq", "heading_text": "أسئلة شائعة", "subheadings": ["كم سعر التذكرة؟", "هل توجد أسعار خاصة للأطفال؟", "هل يحتاج مسبق؟"]}
        ]
        repaired = self.repair_service.dedupe_faq_against_h2(outline)
        self.assertEqual(len(repaired[1]["subheadings"]), 2)
        self.assertIn("هل توجد أسعار خاصة للأطفال؟", repaired[1]["subheadings"])
        self.assertIn("هل يحتاج مسبق؟", repaired[1]["subheadings"])

    def test_clean_conclusion_heading(self):
        outline = [
            {"section_type": "visitor_information", "heading_text": "مواعيد العمل", "heading_level": "H2"},
            {"section_type": "conclusion", "heading_text": "تجربة متكاملة", "heading_level": "H2"}
        ]
        repaired = self.repair_service.clean_conclusion_heading(outline, "الحديقة")
        self.assertEqual(repaired[1]["heading_text"], "خلاصة ونصائح قبل زيارة الحديقة")

    def test_clean_conclusion_heading_last_h2_fallback(self):
        outline = [
            {"section_type": "visitor_information", "heading_text": "مواعيد العمل", "heading_level": "H2"},
            {"section_type": "other", "heading_text": "تجربة زيارة", "heading_level": "H2"}
        ]
        repaired = self.repair_service.clean_conclusion_heading(outline, "")
        self.assertEqual(repaired[1]["heading_text"], "خلاصة ونصائح قبل الزيارة")

if __name__ == "__main__":
    unittest.main()
