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
        self.assertEqual(repaired[1]["subheadings"][1], "How to book via BrandX")

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
        self.assertEqual(repaired[1]["subheadings"][4], "How to book via BrandX")

    def test_enrich_brand_utility_faq_uses_implementation_for_strategy_topic(self):
        outline = [
            {
                "section_type": "core_or_benefits",
                "heading_text": "\u0627\u0644\u0641\u0631\u0642 \u0628\u064a\u0646 SEO \u0648 SEM: \u0627\u0644\u062a\u0639\u0631\u064a\u0641 \u0648\u0627\u0644\u0645\u0641\u0627\u0647\u064a\u0645 \u0627\u0644\u0623\u0633\u0627\u0633\u064a\u0629",
            },
            {
                "section_type": "faq",
                "heading_text": "\u0623\u0633\u0626\u0644\u0629 \u0634\u0627\u0626\u0639\u0629",
                "subheadings": [
                    "\u0623\u064a \u0627\u0644\u0627\u0633\u062a\u0631\u0627\u062a\u064a\u062c\u064a\u062a\u064a\u0646 \u0623\u0633\u0631\u0639 \u0641\u064a \u062a\u062d\u0642\u064a\u0642 \u0627\u0644\u0646\u062a\u0627\u0626\u062c\u061f",
                    "\u0647\u0644 \u064a\u0645\u0643\u0646 \u0627\u0644\u062c\u0645\u0639 \u0628\u064a\u0646 SEO \u0648 SEM\u061f",
                    "\u0645\u0627 \u0647\u064a \u0627\u0644\u062a\u0643\u0627\u0644\u064a\u0641 \u0627\u0644\u0645\u062a\u0648\u0642\u0639\u0629\u061f",
                    "\u0643\u064a\u0641 \u0623\u062e\u062a\u0627\u0631 \u0627\u0644\u0623\u0646\u0633\u0628 \u0644\u0645\u0634\u0631\u0648\u0639\u064a\u061f",
                ],
            },
        ]
        repaired = self.repair_service.enrich_brand_utility_faq(
            outline,
            {},
            "Creative Minds",
            "informational",
            "\u0627\u0644\u0641\u0631\u0642 \u0628\u064a\u0646 seo \u0648 sem",
        )
        faq_text = " ".join(repaired[1]["subheadings"])
        self.assertIn("Creative Minds", faq_text)
        self.assertIn("\u062a\u0646\u0641\u064a\u0630 \u0627\u0633\u062a\u0631\u0627\u062a\u064a\u062c\u064a\u0629 SEO \u0648 SEM", faq_text)
        self.assertNotIn("\u062d\u062c\u0632 \u062a\u0630\u0627\u0643\u0631", faq_text)
        self.assertTrue(all(isinstance(item, str) for item in repaired[1]["subheadings"]))

    def test_enrich_brand_utility_faq_skips_pure_knowledge_topic(self):
        outline = [
            {
                "section_type": "core_or_benefits",
                "heading_text": "\u062a\u0627\u0631\u064a\u062e \u0627\u0644\u062f\u0648\u0644\u0629 \u0627\u0644\u0639\u0628\u0627\u0633\u064a\u0629",
            },
            {
                "section_type": "faq",
                "heading_text": "\u0623\u0633\u0626\u0644\u0629 \u0634\u0627\u0626\u0639\u0629",
                "subheadings": ["\u0645\u062a\u0649 \u0628\u062f\u0623\u062a \u0627\u0644\u062f\u0648\u0644\u0629 \u0627\u0644\u0639\u0628\u0627\u0633\u064a\u0629\u061f"],
            },
        ]
        repaired = self.repair_service.enrich_brand_utility_faq(
            outline,
            {},
            "BrandX",
            "informational",
            "\u062a\u0627\u0631\u064a\u062e \u0627\u0644\u062f\u0648\u0644\u0629 \u0627\u0644\u0639\u0628\u0627\u0633\u064a\u0629",
        )
        self.assertEqual(repaired[1]["subheadings"], ["\u0645\u062a\u0649 \u0628\u062f\u0623\u062a \u0627\u0644\u062f\u0648\u0644\u0629 \u0627\u0644\u0639\u0628\u0627\u0633\u064a\u0629\u061f"])

    def test_normalize_heading_only_section_types_definition_not_offer(self):
        outline = [
            {
                "section_type": "offer",
                "heading_level": "H2",
                "heading_text": "\u0627\u0644\u0641\u0631\u0642 \u0628\u064a\u0646 SEO \u0648 SEM: \u0627\u0644\u062a\u0639\u0631\u064a\u0641 \u0648\u0627\u0644\u0645\u0641\u0627\u0647\u064a\u0645 \u0627\u0644\u0623\u0633\u0627\u0633\u064a\u0629",
            }
        ]
        repaired = self.repair_service.normalize_heading_only_section_types(outline)
        self.assertEqual(repaired[0]["section_type"], "core_or_benefits")

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

    def test_finalize_brand_commercial_coverage_roles_injects_missing(self):
        """Verify that missing mandatory roles are injected/reassigned."""
        # Outline with only intro, offer, and conclusion (missing features, differentiators, proof, etc.)
        outline = [
            {"section_id": "sec_01", "section_type": "introduction", "heading_text": "Intro", "coverage_role": "introduction"},
            {"section_id": "sec_02", "section_type": "offer", "heading_text": "Offer 1", "coverage_role": "offer_clarity"},
            {"section_id": "sec_03", "section_type": "offer", "heading_text": "Offer 2", "coverage_role": "offer_clarity"},
            {"section_id": "sec_04", "section_type": "offer", "heading_text": "Offer 3", "coverage_role": "offer_clarity"},
            {"section_id": "sec_05", "section_type": "offer", "heading_text": "Offer 4", "coverage_role": "offer_clarity"},
            {"section_id": "sec_06", "section_type": "conclusion", "heading_text": "Conclusion", "coverage_role": "conclusion"}
        ]
        
        repaired = self.repair_service.finalize_brand_commercial_coverage_roles(
            outline, 
            primary_keyword="شقق للايجار", 
            brand_name="قولدن هوست"
        )
        
        roles = [s.get("coverage_role") for s in repaired]
        # It should have reassigned redundant offers
        self.assertIn("features_or_included", roles)
        self.assertIn("differentiators", roles)
        self.assertIn("proof", roles)
        
    def test_apply_strategic_map_and_roles_pk_anchoring(self):
        """Verify PK anchoring: exactly one H2 has contains_exact_primary_keyword=True."""
        outline = [
            {"heading_level": "H2", "heading_text": "Section 1", "section_type": "core"},
            {"heading_level": "H2", "heading_text": "Section 2", "section_type": "benefits"},
            {"heading_level": "H2", "heading_text": "Section 3", "section_type": "extra"},
            {"heading_level": "H2", "heading_text": "Section 4", "section_type": "more"},
            {"heading_level": "H3", "heading_text": "Sub 1", "section_type": "detail"}
        ]
        
        pk = "الكلمة الرئيسية"
        repaired = self.repair_service.apply_strategic_map_and_roles(
            outline, 
            primary_keyword=pk, 
            content_type="brand_commercial"
        )
        
        # Check PK flags
        pk_h2_count = sum(1 for s in repaired if s.get("contains_exact_primary_keyword") is True)
        self.assertEqual(pk_h2_count, 1, "Exactly one H2 must be the PK anchor")
        
        # Check H3 doesn't have it
        h3_pk = any(s.get("contains_exact_primary_keyword") for s in repaired if s.get("heading_level") == "H3")
        self.assertFalse(h3_pk, "H3 must not be the PK anchor")
        
        # Check body writing flags (requires_primary_keyword)
        writing_pk_count = sum(1 for s in repaired if s.get("requires_primary_keyword") is True)
        self.assertGreaterEqual(writing_pk_count, 4, "Should have at least 4 body writing slots")

    def test_h2_deduplication(self):
        """Verify that duplicate H2 headings are resolved."""
        outline = [
            {"heading_level": "H2", "heading_text": "المميزات", "section_id": "sec_1"},
            {"heading_level": "H2", "heading_text": "المميزات", "section_id": "sec_2"},
            {"heading_level": "H3", "heading_text": "فرعي", "section_id": "sec_3"}
        ]
        
        repaired = self.repair_service.apply_strategic_map_and_roles(
            outline, 
            primary_keyword="تيست", 
            content_type="brand_commercial"
        )
        
        h2_texts = [s["heading_text"] for s in repaired if s["heading_level"] == "H2"]
        self.assertEqual(len(set(h2_texts)), 2, "H2 headings must be unique after repair")
        self.assertNotEqual(h2_texts[0], h2_texts[1])

if __name__ == "__main__":
    unittest.main()
