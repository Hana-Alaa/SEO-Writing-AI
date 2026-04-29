import unittest
import re
import os
import sys
from typing import List, Dict, Any, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.services.validation_service import ValidationService

class TestValidationCalibration(unittest.TestCase):
    def setUp(self):
        self.validator = ValidationService()

    def test_long_brand_substitution(self):
        # Brand > 30 chars
        long_brand = "مؤسسة الابتكار العقاري العالمية للتطوير والاستثمار"
        self.assertTrue(len(long_brand) > 30)
        
        # Heading contains a substitute like "المنصة"
        heading = "كيف تساعدك المنصة في العثور على سكن؟"
        self.assertTrue(self.validator._heading_contains_exact_brand_name(heading, long_brand))

    def test_faq_question_validation(self):
        # Valid question starter
        self.assertTrue(self.validator._is_valid_faq_question("هل يتوفر تقسيط؟"))
        self.assertTrue(self.validator._is_valid_faq_question("كم سعر المتر؟"))
        # Invalid
        self.assertFalse(self.validator._is_valid_faq_question("تفاصيل الحجز"))

    def test_arabic_entity_normalization(self):
        # Verify "شقق" and "شقه" match
        tokens = self.validator._expanded_token_set("شقق للايجار")
        self.assertIn("شقه", tokens)
        self.assertIn("شقق", tokens)

    def test_h3_granularity_pruning(self):
        # Granular spec in features should be pruned
        outline = [
            {
                "heading_level": "H2",
                "section_type": "features",
                "heading_text": "مميزات الوحدات",
                "subheadings": [
                    "مواصفات التشطيب", # Granular -> Prune
                    "شقق دوبلكس" # Standalone -> Keep
                ]
            }
        ]
        repaired = self.validator.repair_outline_deterministic(outline)
        self.assertEqual(len(repaired[0]["subheadings"]), 1)
        self.assertEqual(repaired[0]["subheadings"][0], "شقق دوبلكس")

    def test_h3_atomization_repair(self):
        # Multiple ideas in H3 should be pruned
        outline = [
            {
                "heading_level": "H2",
                "section_type": "offer",
                "heading_text": "المناطق المتاحة",
                "subheadings": [
                    "دبي مارينا", # Single idea -> Keep
                    "المترو والمناطق الحيوية", # Multiple ideas (atomization violation) -> Prune
                    "الشيخ زايد والبرشاء" # Multiple ideas -> Prune
                ]
            }
        ]
        repaired = self.validator.repair_outline_deterministic(outline)
        self.assertEqual(len(repaired[0]["subheadings"]), 1)
        self.assertEqual(repaired[0]["subheadings"][0], "دبي مارينا")

    def test_cross_entity_subheading_is_pruned(self):
        outline = [
            {
                "heading_level": "H2",
                "section_type": "offer",
                "heading_text": "أفضل شقق للايجار في الرياض حسب المنطقة",
                "subheadings": [
                    "شاليهات مفروشة للإيجار اليومي",
                    "شقق استوديو قريبة من العمل"
                ]
            }
        ]
        repaired = self.validator.repair_outline_deterministic(
            outline,
            primary_keyword="شقق للايجار في الرياض",
            area="الرياض"
        )
        self.assertEqual(repaired[0]["subheadings"], ["شقق استوديو قريبة من العمل"])

    def test_service_keyword_profile_keeps_compound_entity_phrase(self):
        profile = self.validator._derive_keyword_profile(
            "افضل شركة تصميم مواقع في السعودية",
            "السعودية",
        )
        self.assertEqual(self.validator._normalize_heading_label(profile["head_entity"]), "شركه")
        self.assertEqual(self.validator._normalize_heading_label(profile["entity_phrase"]), "شركه تصميم مواقع")
        self.assertIn("تصميم", profile["entity_descriptor_tokens"])
        self.assertIn("مواقع", profile["entity_descriptor_tokens"])
        self.assertFalse(
            self.validator._heading_preserves_entity_focus(
                "هل تختار شركة محلية أم دولية في السعودية؟",
                profile,
            )
        )
        self.assertTrue(
            self.validator._heading_preserves_entity_focus(
                "هل تختار شركة تصميم مواقع محلية أم دولية في السعودية؟",
                profile,
            )
        )

    def test_brand_differentiation_repair(self):
        # Generic differentiation should be rewritten
        outline = [
            {
                "heading_level": "H2",
                "section_type": "differentiation",
                "heading_text": "لماذا تختارنا؟"
            }
        ]
        repaired = self.validator.repair_outline_deterministic(
            outline, 
            primary_keyword="شقق للبيع", 
            brand_name="المنصة"
        )
        self.assertIn("لماذا تختار المنصة", repaired[0]["heading_text"])
        self.assertIn("شقق للبيع", repaired[0]["heading_text"])

    def test_keyword_stuffing_repair_protected(self):
        pk = "شقق للايجار في الرياض"
        outline = [
            {"heading_level": "H2", "section_type": "offer", "heading_text": "أفضل شقق للايجار في الرياض"}, # Anchor 1 (Keep)
            {"heading_level": "H2", "section_type": "proof", "heading_text": "أسعار شقق للايجار في الرياض 2026"}, # Protected (Keep)
            {"heading_level": "H2", "section_type": "features", "heading_text": "مميزات شقق للايجار في الرياض"}, # Repeat (Rewrite)
            {"heading_level": "H2", "section_type": "differentiation", "heading_text": "لماذا تختار عقار يا مصر للبحث عن شقق للايجار في الرياض؟"} # Protected (Keep)
        ]
        repaired = self.validator.repair_outline_deterministic(outline, primary_keyword=pk)
        
        # Verify first is kept
        self.assertEqual(repaired[0]["heading_text"], "أفضل شقق للايجار في الرياض")
        # Verify proof is protected
        self.assertEqual(repaired[1]["heading_text"], "أسعار شقق للايجار في الرياض 2026")
        # Verify features is rewritten
        self.assertNotIn(pk, repaired[2]["heading_text"])
        # Verify differentiation is protected
        self.assertIn(pk, repaired[3]["heading_text"])

    def test_proof_heading_repair_rental(self):
        pk = "شقق للايجار في الرياض"
        outline = [
            {"heading_level": "H2", "section_type": "proof", "heading_text": "أسعار العقارات"} # Weak, missing intent and location
        ]
        repaired = self.validator.repair_outline_deterministic(outline, primary_keyword=pk, area="الرياض")
        
        # Should be rebuilt: "متوسط أسعار شقق للايجار في الرياض حسب المنطقة وأهم العوامل المؤثرة"
        new_text = repaired[0]["heading_text"]
        self.assertIn("شقق", new_text)
        self.assertIn("للايجار", new_text)
        self.assertIn("الرياض", new_text)
        self.assertIn("متوسط أسعار", new_text)

    def test_proof_heading_repair_sale(self):
        pk = "فلل للبيع في القاهرة"
        outline = [
            {"heading_level": "H2", "section_type": "proof", "heading_text": "تكلفة الشراء"} # Weak
        ]
        repaired = self.validator.repair_outline_deterministic(outline, primary_keyword=pk, area="القاهرة")
        
        new_text = repaired[0]["heading_text"]
        self.assertIn("فلل", new_text)
        self.assertIn("للبيع", new_text)
        # character ة vs ه normalization
        self.assertTrue("القاهرة" in new_text or "القاهره" in new_text)

    def test_monthly_yearly_comparison_valid(self):
        pk = "شقق للايجار في الرياض"
        outline = [
            {"heading_level": "NONE", "section_type": "introduction", "heading_text": "مقدمة"},
            {"heading_level": "H2", "section_type": "offer", "heading_text": "شقق للايجار في الرياض"},
            {"heading_level": "H2", "section_type": "features", "heading_text": "مميزات السكن"},
            {"heading_level": "H2", "section_type": "comparison", "heading_text": "هل الأفضل اختيار شقق بنظام الدفع الشهري أم السنوي في الرياض؟", "subheadings": ["المزايا"]},
            {"heading_level": "H2", "section_type": "conclusion", "heading_text": "خاتمة"}
        ]
        errors = self.validator.validate_heading_outline_quality(outline, primary_keyword=pk)
        self.assertFalse(any("COMPARISON_SECTION_WEAK" in e for e in errors))

if __name__ == "__main__":
    unittest.main()
