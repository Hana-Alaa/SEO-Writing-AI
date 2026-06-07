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

    def test_finalize_brand_commercial_coverage_roles_does_not_inject_missing(self):
        """Missing roles must not be manufactured by reassigning existing sections."""
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
            primary_keyword="business service",
            brand_name="Example Brand",
            brand_evidence_inventory={
                "projects_available": False,
                "trust_available": False,
                "pricing_available": False,
            },
        )

        roles = [s.get("coverage_role") for s in repaired]
        self.assertEqual(roles.count("offer_clarity"), 4)
        self.assertNotIn("features_or_included", roles)
        self.assertNotIn("differentiators", roles)
        self.assertNotIn("proof", roles)
        self.assertEqual([s["heading_text"] for s in repaired], [s["heading_text"] for s in outline])

    def test_finalize_removes_unsupported_proof_section(self):
        outline = [
            {"section_id": "intro", "section_type": "introduction", "heading_text": "Intro"},
            {
                "section_id": "proof",
                "section_type": "proof",
                "heading_text": "Projects completed by Example Brand",
                "coverage_role": "proof",
            },
            {"section_id": "end", "section_type": "conclusion", "heading_text": "Next step"},
        ]

        repaired = self.repair_service.finalize_brand_commercial_coverage_roles(
            outline,
            primary_keyword="business service",
            brand_name="Example Brand",
            brand_evidence_inventory={
                "projects_available": False,
                "trust_available": False,
                "pricing_available": False,
            },
        )

        self.assertNotIn("proof", [section.get("coverage_role") for section in repaired])
        self.assertNotIn("proof", [section.get("section_id") for section in repaired])

    def test_finalize_preserves_supported_proof_section(self):
        outline = [
            {
                "section_id": "proof",
                "section_type": "proof",
                "heading_text": "Projects completed by Example Brand",
                "coverage_role": "proof",
            },
        ]

        repaired = self.repair_service.finalize_brand_commercial_coverage_roles(
            outline,
            primary_keyword="business service",
            brand_name="Example Brand",
            brand_evidence_inventory={
                "projects_available": True,
                "trust_available": False,
                "pricing_available": False,
            },
        )

        self.assertEqual(len(repaired), 1)
        self.assertEqual(repaired[0]["coverage_role"], "proof")
        self.assertEqual(repaired[0]["heading_text"], outline[0]["heading_text"])

    def test_brand_pricing_heading_is_downgraded_without_pricing_evidence(self):
        outline = [
            {
                "section_id": "pricing",
                "section_type": "pricing",
                "heading_text": "Example Brand packages and pricing",
            },
        ]

        repaired = self.repair_service.finalize_brand_commercial_coverage_roles(
            outline,
            primary_keyword="business service",
            brand_name="Example Brand",
            brand_evidence_inventory={
                "projects_available": True,
                "trust_available": False,
                "pricing_available": False,
            },
        )

        self.assertEqual(repaired[0]["coverage_role"], "cost_value")
        self.assertEqual(repaired[0]["section_type"], "pricing")
        self.assertEqual(repaired[0]["brand_policy"], "neutral_market")
        self.assertNotIn("Example Brand", repaired[0]["heading_text"])

    def test_pricing_is_cost_value_not_proof(self):
        section = {
            "section_type": "core",
            "heading_text": "Pricing and cost factors",
        }
        self.assertEqual(self.repair_service._infer_coverage_role(section), "cost_value")

    def test_removed_proof_section_does_not_consume_keyword_anchor(self):
        primary_keyword = "business service"
        outline = [
            {"heading_level": "INTRO", "heading_text": "Intro", "section_type": "introduction"},
            {
                "heading_level": "H2",
                "heading_text": f"{primary_keyword} project results",
                "section_type": "proof",
                "coverage_role": "proof",
            },
            {
                "heading_level": "H2",
                "heading_text": "Service scope",
                "section_type": "offer",
                "coverage_role": "offer_clarity",
            },
            {"heading_level": "H2", "heading_text": "Next step", "section_type": "conclusion"},
        ]

        repaired = self.repair_service.apply_strategic_map_and_roles(
            outline,
            primary_keyword=primary_keyword,
            content_type="brand_commercial",
            brand_name="Example Brand",
            brand_evidence_inventory={
                "projects_available": False,
                "trust_available": False,
                "pricing_available": False,
            },
        )

        self.assertNotIn("proof", [section.get("coverage_role") for section in repaired])
        service = next(section for section in repaired if section.get("coverage_role") == "offer_clarity")
        self.assertTrue(service["requires_primary_keyword"])
        self.assertFalse(service["contains_exact_primary_keyword"])
        
    def test_apply_strategic_map_and_roles_pk_anchoring(self):
        """Use the exact PK in intro, one suitable H2, and conclusion only."""
        pk = "الكلمة الرئيسية"
        outline = [
            {"heading_level": "INTRO", "heading_text": "Intro", "section_type": "introduction"},
            {"heading_level": "H2", "heading_text": f"خدمات {pk}", "section_type": "offer"},
            {"heading_level": "H2", "heading_text": "Section 2", "section_type": "benefits"},
            {"heading_level": "H2", "heading_text": "Section 3", "section_type": "extra"},
            {"heading_level": "H3", "heading_text": "Sub 1", "section_type": "detail"},
            {"heading_level": "H2", "heading_text": "Conclusion", "section_type": "conclusion"},
        ]

        repaired = self.repair_service.apply_strategic_map_and_roles(
            outline,
            primary_keyword=pk,
            content_type="brand_commercial",
            brand_evidence_inventory={
                "projects_available": False,
                "trust_available": False,
                "pricing_available": False,
            },
        )

        # Check PK flags
        pk_h2_count = sum(1 for s in repaired if s.get("contains_exact_primary_keyword") is True)
        self.assertEqual(pk_h2_count, 1, "Exactly one H2 must be the PK anchor")

        # Check H3 doesn't have it
        h3_pk = any(s.get("contains_exact_primary_keyword") for s in repaired if s.get("heading_level") == "H3")
        self.assertFalse(h3_pk, "H3 must not be the PK anchor")

        # The exact phrase is reserved for intro, one H2, and conclusion.
        writing_pk_count = sum(1 for s in repaired if s.get("requires_primary_keyword") is True)
        self.assertEqual(writing_pk_count, 3)

    def test_h2_deduplication(self):
        """Exact duplicate H2s are merged instead of renamed."""
        outline = [
            {
                "heading_level": "H2",
                "heading_text": "المميزات",
                "section_id": "sec_1",
                "subheadings": ["الميزة الأولى"],
            },
            {
                "heading_level": "H2",
                "heading_text": "المميزات",
                "section_id": "sec_2",
                "subheadings": ["الميزة الثانية"],
            },
            {"heading_level": "H3", "heading_text": "فرعي", "section_id": "sec_3"}
        ]

        repaired = self.repair_service.apply_strategic_map_and_roles(
            outline,
            primary_keyword="تيست",
            content_type="brand_commercial",
            brand_evidence_inventory={
                "projects_available": False,
                "trust_available": False,
                "pricing_available": False,
            },
        )

        h2_texts = [s["heading_text"] for s in repaired if s["heading_level"] == "H2"]
        self.assertEqual(h2_texts, ["المميزات"])
        self.assertEqual(
            repaired[0]["subheadings"],
            ["الميزة الأولى", "الميزة الثانية"],
        )
        self.assertNotIn("تفاصيل إضافية", " ".join(h2_texts))



class TestOutlineRepairDiagnosticMode(unittest.IsolatedAsyncioTestCase):
    async def test_diagnostic_mode_behavior(self):
        from src.services.workflow_controller import AsyncWorkflowController
        from unittest.mock import AsyncMock, MagicMock
        
        # Test Case 1: disable_outline_repair = False (default / normal behavior)
        controller = AsyncWorkflowController()
        
        mock_outline = [
            {"section_type": "introduction", "heading_level": "INTRO", "heading_text": "Intro"},
            {"section_type": "faq", "heading_level": "H2", "heading_text": "FAQ", "subheadings": ["Q1"]}
        ]
        controller.outline_gen.generate = AsyncMock(return_value={
            "outline": mock_outline,
            "metadata": {"prompt": "p", "response": "r", "tokens": 10, "model": "m"}
        })
        
        # Mock validator to bypass validation checks
        controller.validator.consolidate_faq = MagicMock(side_effect=lambda outline: outline)
        controller.validator.prune_unsupported_optional_subheadings = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller.validator.repair_outline_deterministic = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller.validator.enforce_intent_distribution = MagicMock(side_effect=lambda outline, *args, **kwargs: (outline, []))
        controller.validator.inject_local_seo = MagicMock(side_effect=lambda outline, *args, **kwargs: (outline, []))
        controller.validator.validate_outline_quality = MagicMock(return_value=[])
        controller.validator.validate_heading_outline_quality = MagicMock(return_value=[])
        
        # Mock outline_repair_service methods
        controller.outline_repair_service.promote_visitor_intents = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller.outline_repair_service.dedupe_faq_against_h2 = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller.outline_repair_service.refill_faq_after_dedupe = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller.outline_repair_service.enrich_brand_utility_faq = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller.outline_repair_service.normalize_heading_only_section_types = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller.outline_repair_service.clean_echo_and_repetition = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller.outline_repair_service.apply_strategic_map_and_roles = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller.outline_repair_service.clean_conclusion_heading = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        
        state_normal = {
            "input_data": {
                "title": "Test Title",
                "keywords": ["test"],
                "disable_outline_repair": False
            },
            "brand_context": "",
            "intent": "informational",
            "content_type": "informational"
        }
        
        state_normal_res = await controller._step_1_outline(state_normal)
        self.assertIsNotNone(state_normal_res)
        
        controller.outline_repair_service.promote_visitor_intents.assert_called()
        controller.outline_repair_service.dedupe_faq_against_h2.assert_called()
        controller.outline_repair_service.refill_faq_after_dedupe.assert_called()
        controller.outline_repair_service.enrich_brand_utility_faq.assert_called()
        controller.outline_repair_service.normalize_heading_only_section_types.assert_called()
        controller.outline_repair_service.clean_echo_and_repetition.assert_called()
        controller.outline_repair_service.apply_strategic_map_and_roles.assert_called()
        controller.outline_repair_service.clean_conclusion_heading.assert_called()

        # Test Case 2: disable_outline_repair = True, heading_only_mode = False
        controller_diag = AsyncWorkflowController()
        controller_diag.outline_gen.generate = AsyncMock(return_value={
            "outline": mock_outline,
            "metadata": {"prompt": "p", "response": "r", "tokens": 10, "model": "m"}
        })
        
        controller_diag.validator.consolidate_faq = MagicMock(side_effect=lambda outline: outline)
        controller_diag.validator.prune_unsupported_optional_subheadings = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag.validator.repair_outline_deterministic = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag.validator.enforce_intent_distribution = MagicMock(side_effect=lambda outline, *args, **kwargs: (outline, []))
        controller_diag.validator.inject_local_seo = MagicMock(side_effect=lambda outline, *args, **kwargs: (outline, []))
        controller_diag.validator.validate_outline_quality = MagicMock(return_value=[])
        controller_diag.validator.validate_heading_outline_quality = MagicMock(return_value=[])
        
        controller_diag.outline_repair_service.promote_visitor_intents = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag.outline_repair_service.dedupe_faq_against_h2 = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag.outline_repair_service.refill_faq_after_dedupe = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag.outline_repair_service.enrich_brand_utility_faq = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag.outline_repair_service.normalize_heading_only_section_types = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag.outline_repair_service.clean_echo_and_repetition = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag.outline_repair_service.apply_strategic_map_and_roles = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag.outline_repair_service.clean_conclusion_heading = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        
        state_diag = {
            "input_data": {
                "title": "Test Title",
                "keywords": ["test"],
                "disable_outline_repair": True
            },
            "brand_context": "",
            "intent": "informational",
            "content_type": "informational"
        }
        
        state_diag_res = await controller_diag._step_1_outline(state_diag)
        self.assertIsNotNone(state_diag_res)
        
        # Mutating operations skipped
        controller_diag.outline_repair_service.promote_visitor_intents.assert_not_called()
        controller_diag.outline_repair_service.refill_faq_after_dedupe.assert_not_called()
        controller_diag.outline_repair_service.enrich_brand_utility_faq.assert_not_called()
        controller_diag.outline_repair_service.clean_echo_and_repetition.assert_not_called()
        controller_diag.outline_repair_service.apply_strategic_map_and_roles.assert_not_called()
        controller_diag.outline_repair_service.clean_conclusion_heading.assert_not_called()
        
        # Stability / normalizations executed
        controller_diag.outline_repair_service.dedupe_faq_against_h2.assert_called()
        controller_diag.outline_repair_service.normalize_heading_only_section_types.assert_called()
        
        self.assertEqual(state_diag_res.get("outline"), mock_outline)

        # Test Case 3: disable_outline_repair = True, heading_only_mode = True
        controller_diag_head = AsyncWorkflowController()
        controller_diag_head.outline_gen.generate = AsyncMock(return_value={
            "outline": mock_outline,
            "metadata": {"prompt": "p", "response": "r", "tokens": 10, "model": "m"}
        })
        
        controller_diag_head.validator.consolidate_faq = MagicMock(side_effect=lambda outline: outline)
        controller_diag_head.validator.prune_unsupported_optional_subheadings = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_head.validator.repair_outline_deterministic = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_head.validator.enforce_intent_distribution = MagicMock(side_effect=lambda outline, *args, **kwargs: (outline, []))
        controller_diag_head.validator.inject_local_seo = MagicMock(side_effect=lambda outline, *args, **kwargs: (outline, []))
        controller_diag_head.validator.validate_outline_quality = MagicMock(return_value=[])
        controller_diag_head.validator.validate_heading_outline_quality = MagicMock(return_value=[])
        
        controller_diag_head.outline_repair_service.promote_visitor_intents = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_head.outline_repair_service.dedupe_faq_against_h2 = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_head.outline_repair_service.refill_faq_after_dedupe = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_head.outline_repair_service.enrich_brand_utility_faq = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_head.outline_repair_service.normalize_heading_only_section_types = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_head.outline_repair_service.clean_echo_and_repetition = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_head.outline_repair_service.apply_strategic_map_and_roles = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_head.outline_repair_service.clean_conclusion_heading = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        
        state_diag_head = {
            "input_data": {
                "title": "Test Title",
                "keywords": ["test"],
                "disable_outline_repair": True,
                "heading_only_mode": True
            },
            "brand_context": "",
            "intent": "informational",
            "content_type": "informational"
        }
        
        state_diag_head_res = await controller_diag_head._step_1_outline(state_diag_head)
        self.assertIsNotNone(state_diag_head_res)
        
        # Mutating operations skipped
        controller_diag_head.outline_repair_service.enrich_brand_utility_faq.assert_not_called()
        controller_diag_head.outline_repair_service.clean_conclusion_heading.assert_not_called()
        
        # Stability / normalizations executed
        controller_diag_head.outline_repair_service.dedupe_faq_against_h2.assert_called()
        controller_diag_head.outline_repair_service.normalize_heading_only_section_types.assert_called()
        
        self.assertEqual(state_diag_head_res.get("outline"), mock_outline)

        # Test Case 4: disable_outline_repair = True, content_stage_only_mode = True
        controller_diag_content = AsyncWorkflowController()
        controller_diag_content.outline_gen.generate = AsyncMock(return_value={
            "outline": mock_outline,
            "metadata": {"prompt": "p", "response": "r", "tokens": 10, "model": "m"}
        })
        
        controller_diag_content.validator.consolidate_faq = MagicMock(side_effect=lambda outline: outline)
        controller_diag_content.validator.prune_unsupported_optional_subheadings = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_content.validator.repair_outline_deterministic = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_content.validator.enforce_intent_distribution = MagicMock(side_effect=lambda outline, *args, **kwargs: (outline, []))
        controller_diag_content.validator.inject_local_seo = MagicMock(side_effect=lambda outline, *args, **kwargs: (outline, []))
        controller_diag_content.validator.validate_outline_quality = MagicMock(return_value=[])
        controller_diag_content.validator.validate_heading_outline_quality = MagicMock(return_value=[])
        
        controller_diag_content.outline_repair_service.promote_visitor_intents = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_content.outline_repair_service.dedupe_faq_against_h2 = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_content.outline_repair_service.refill_faq_after_dedupe = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_content.outline_repair_service.enrich_brand_utility_faq = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_content.outline_repair_service.normalize_heading_only_section_types = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_content.outline_repair_service.clean_echo_and_repetition = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_content.outline_repair_service.apply_strategic_map_and_roles = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        controller_diag_content.outline_repair_service.clean_conclusion_heading = MagicMock(side_effect=lambda outline, *args, **kwargs: outline)
        
        state_diag_content = {
            "input_data": {
                "title": "Test Title",
                "keywords": ["test"],
                "disable_outline_repair": True,
                "content_stage_only_mode": True
            },
            "brand_context": "",
            "intent": "informational",
            "content_type": "informational"
        }
        
        state_diag_content_res = await controller_diag_content._step_1_outline(state_diag_content)
        self.assertIsNotNone(state_diag_content_res)
        
        # Mutating operations skipped
        controller_diag_content.outline_repair_service.enrich_brand_utility_faq.assert_not_called()
        controller_diag_content.outline_repair_service.clean_conclusion_heading.assert_not_called()
        
        # Stability / normalizations executed
        controller_diag_content.outline_repair_service.dedupe_faq_against_h2.assert_called()
        controller_diag_content.outline_repair_service.normalize_heading_only_section_types.assert_called()
        
        self.assertEqual(state_diag_content_res.get("outline"), mock_outline)


if __name__ == "__main__":
    unittest.main()
