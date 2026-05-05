import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.services.validation_service import ValidationService


class TestValidationServiceHeadingAudit(unittest.TestCase):
    def setUp(self):
        self.validator = ValidationService()
        self.base_outline = [
            {
                "section_id": "intro",
                "heading_level": "INTRO",
                "section_type": "introduction",
                "heading_text": "intro",
                "subheadings": [],
            },
            {
                "section_id": "sec_1",
                "heading_level": "H2",
                "section_type": "offer",
                "heading_text": "أفضل خدمات التنظيف في الرياض حسب احتياجك",
                "subheadings": [],
            },
            {
                "section_id": "sec_2",
                "heading_level": "H2",
                "section_type": "features",
                "heading_text": "خدمات تنظيف المنازل والشقق المناسبة لكل مساحة",
                "subheadings": [],
            },
            {
                "section_id": "sec_3",
                "heading_level": "H2",
                "section_type": "pricing",
                "heading_text": "أسعار خدمات التنظيف في الرياض حسب نوع الوحدة",
                "subheadings": [],
            },
            {
                "section_id": "sec_4",
                "heading_level": "H2",
                "section_type": "process",
                "heading_text": "خطوات حجز خدمة تنظيف مناسبة في الرياض",
                "subheadings": [],
            },
            {
                "section_id": "sec_5",
                "heading_level": "H2",
                "section_type": "faq",
                "heading_text": "أسئلة شائعة حول خدمات التنظيف في الرياض",
                "subheadings": [],
            },
            {
                "section_id": "sec_6",
                "heading_level": "H2",
                "section_type": "conclusion",
                "heading_text": "ابدأ حجز خدمة تنظيف مناسبة بثقة",
                "subheadings": [],
            },
        ]
        self.base_args = {
            "content_type": "brand_commercial",
            "area": "الرياض",
            "primary_keyword": "شركة تنظيف بالرياض",
            "brand_name": "كلين لاين",
            "display_brand_name": "شركة كلين لاين",
            "content_strategy": {},
            "seo_intelligence": {},
            "entity_phrase": "شركة تنظيف",
            "service_phrase": "خدمات التنظيف",
        }

    def _warnings(self, report, code):
        return [warning for warning in report["warnings"] if warning["code"] == code]

    def test_clean_outline(self):
        outline = copy.deepcopy(self.base_outline)
        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)

        self.assertTrue(report["passed"])
        self.assertEqual(report["mode"], "audit_only")
        self.assertEqual(report["summary"]["total_warnings"], 0)

    def test_pk_repetition_logic_core_h2_only(self):
        outline = copy.deepcopy(self.base_outline)
        outline[1]["heading_text"] = "شركة تنظيف بالرياض رخيصة"
        outline[2]["heading_text"] = "مميزات أفضل شركة تنظيف بالرياض"
        outline[3]["heading_text"] = "أسعار شركة تنظيف بالرياض"

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)
        warnings = self._warnings(report, "PK_REPETITION")

        self.assertTrue(report["passed"])
        self.assertEqual(len(warnings), 3)
        self.assertEqual({warning["severity"] for warning in warnings}, {"medium"})

        outline[3]["heading_text"] = "أسعار خدمات التنظيف في الرياض"
        report_low = self.validator.audit_heading_outline_quality(outline, **self.base_args)
        warnings_low = self._warnings(report_low, "PK_REPETITION")

        self.assertEqual(len(warnings_low), 2)
        self.assertEqual({warning["severity"] for warning in warnings_low}, {"low"})

    def test_pk_repetition_ignores_faq_conclusion_and_h3(self):
        outline = copy.deepcopy(self.base_outline)
        outline[5]["heading_text"] = "أسئلة شائعة عن شركة تنظيف بالرياض"
        outline[6]["heading_text"] = "ابدأ مع شركة تنظيف بالرياض"
        outline[2]["subheadings"] = ["شركة تنظيف بالرياض للمنازل"]

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)

        self.assertEqual(self._warnings(report, "PK_REPETITION"), [])

    def test_pk_repetition_normalizes_arabic_variants(self):
        outline = copy.deepcopy(self.base_outline)
        outline[1]["heading_text"] = "افضل شركه تصميم مواقع في السعوديه"
        outline[2]["heading_text"] = "مميزات أفضل شركة تصميم المواقع في السعودية"
        args = copy.deepcopy(self.base_args)
        args.update({
            "primary_keyword": "أفضل شركة تصميم المواقع في السعودية",
            "entity_phrase": "شركة تصميم مواقع",
            "service_phrase": "تصميم مواقع",
        })

        report = self.validator.audit_heading_outline_quality(outline, **args)
        warnings = self._warnings(report, "PK_REPETITION")

        self.assertEqual(len(warnings), 2)
        self.assertEqual({warning["severity"] for warning in warnings}, {"low"})

    def test_generic_h2(self):
        outline = copy.deepcopy(self.base_outline)
        outline[2]["heading_text"] = "pricing"

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)
        warnings = self._warnings(report, "GENERIC_H2")

        self.assertTrue(warnings)
        self.assertEqual(warnings[0]["severity"], "medium")

    def test_generic_h2_flags_features_without_intent_or_location_context(self):
        outline = copy.deepcopy(self.base_outline)
        outline[2]["heading_text"] = "المزايا التي تساعدك على اختيار شقة سكنية مريحة"

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)
        warnings = self._warnings(report, "GENERIC_H2")

        self.assertTrue(warnings)
        self.assertEqual(warnings[0]["severity"], "medium")

    def test_generic_h2_flags_report_style_location_heading(self):
        outline = copy.deepcopy(self.base_outline)
        outline[2]["section_type"] = "location"
        outline[2]["heading_text"] = "توزيع شقق للايجار في الرياض حسب الأحياء الأكثر طلباً"

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)
        warnings = self._warnings(report, "GENERIC_H2")

        self.assertTrue(warnings)
        self.assertEqual(warnings[0]["severity"], "medium")

    def test_provider_h3_with_parent_exception(self):
        outline = copy.deepcopy(self.base_outline)
        outline[2]["subheadings"] = ["شركات أجنبية", "مكاتب محلية"]

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)
        warnings = self._warnings(report, "PROVIDER_H3")

        self.assertTrue(warnings)

        outline[2]["section_type"] = "comparison"
        outline[2]["heading_text"] = "مقارنة بين أنواع شركات التنظيف في الرياض"
        report_no_warn = self.validator.audit_heading_outline_quality(outline, **self.base_args)

        self.assertEqual(self._warnings(report_no_warn, "PROVIDER_H3"), [])

    def test_provider_h3_does_not_trigger_for_non_provider_keyword(self):
        outline = copy.deepcopy(self.base_outline)
        outline[1]["heading_text"] = "أفضل أنواع الشقق في الرياض حسب احتياجك"
        outline[1]["subheadings"] = ["شركات إدارة العقارات"]
        args = copy.deepcopy(self.base_args)
        args.update({
            "primary_keyword": "أفضل أنواع الشقق في الرياض",
            "entity_phrase": "شقق",
            "service_phrase": "شقق",
        })

        report = self.validator.audit_heading_outline_quality(outline, **args)

        self.assertEqual(self._warnings(report, "PROVIDER_H3"), [])

    def test_broken_arabic_process(self):
        outline = copy.deepcopy(self.base_outline)
        outline[4]["heading_text"] = "خطوات اختيار وتعاقد مع شركة تنظيف"

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)
        warnings = self._warnings(report, "BROKEN_ARABIC_PROCESS")

        self.assertTrue(warnings)
        self.assertEqual(warnings[0]["severity"], "medium")

    def test_mechanical_numbering_does_not_trigger_broken_arabic_process(self):
        outline = copy.deepcopy(self.base_outline)
        outline[4]["heading_text"] = "1. خطوة اختيار خدمة التنظيف المناسبة"

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)

        self.assertEqual(self._warnings(report, "BROKEN_ARABIC_PROCESS"), [])

    def test_broken_arabic_process_only_process_stage(self):
        outline = copy.deepcopy(self.base_outline)
        outline[2]["heading_text"] = "خطوات اختيار وتعاقد مع شركة تنظيف"

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)

        self.assertEqual(self._warnings(report, "BROKEN_ARABIC_PROCESS"), [])

    def test_pricing_provider_focus(self):
        outline = copy.deepcopy(self.base_outline)
        outline[3]["heading_text"] = "متوسط أسعار أفضل شركة تصميم مواقع في السعودية"
        args = copy.deepcopy(self.base_args)
        args.update({
            "primary_keyword": "أفضل شركة تصميم مواقع في السعودية",
            "entity_phrase": "شركة تصميم مواقع",
            "service_phrase": "تصميم مواقع",
        })

        report = self.validator.audit_heading_outline_quality(outline, **args)
        warnings = self._warnings(report, "PRICING_PROVIDER_FOCUS")

        self.assertTrue(warnings)
        self.assertEqual(warnings[0]["severity"], "medium")

        outline[3]["heading_text"] = "تكلفة تصميم موقع إلكتروني في السعودية حسب نطاق المشروع"
        report_clean = self.validator.audit_heading_outline_quality(outline, **args)

        self.assertEqual(self._warnings(report_clean, "PRICING_PROVIDER_FOCUS"), [])

    def test_brand_mismatch(self):
        outline = copy.deepcopy(self.base_outline)
        outline[1]["heading_text"] = "لماذا تختار كلين لاين"
        outline[1]["section_type"] = "differentiation"

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)
        warnings = self._warnings(report, "BRAND_MISMATCH")

        self.assertTrue(warnings)
        self.assertEqual(warnings[0]["severity"], "high")

        args = copy.deepcopy(self.base_args)
        args["display_brand_name"] = ""
        report_skip = self.validator.audit_heading_outline_quality(outline, **args)

        self.assertEqual(self._warnings(report_skip, "BRAND_MISMATCH"), [])

    def test_brand_mismatch_flags_domain_alias_and_generic_leakage(self):
        outline = copy.deepcopy(self.base_outline)
        outline[1]["heading_text"] = "لماذا تختار Cems It لتصميم موقعك؟"
        outline[1]["section_type"] = "differentiation"
        args = copy.deepcopy(self.base_args)
        args.update({
            "brand_name": "Cems It",
            "display_brand_name": "Creative Minds",
            "content_strategy": {"brand_aliases": ["Cems It"], "domain_brand_name": "cems-it"},
        })

        report = self.validator.audit_heading_outline_quality(outline, **args)
        warnings = self._warnings(report, "BRAND_MISMATCH")

        self.assertTrue(warnings)
        self.assertEqual(warnings[0]["severity"], "high")

        outline[1]["heading_text"] = "لماذا تختار شركة تصميم مواقع لمشروعك؟"
        report_generic = self.validator.audit_heading_outline_quality(outline, **args)

        self.assertTrue(self._warnings(report_generic, "BRAND_MISMATCH"))

    def test_weak_h3_empty_duplicate_granular_and_atomized(self):
        outline = copy.deepcopy(self.base_outline)
        parent = "خدمات تنظيف المنازل والشقق المناسبة لكل مساحة"
        outline[2]["heading_text"] = parent
        outline[2]["subheadings"] = [
            "",
            parent,
            "تقسيمات الغرف",
            "تنظيف المنازل والمكاتب مع التعقيم الكامل",
        ]

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)
        warnings = self._warnings(report, "WEAK_H3")

        self.assertGreaterEqual(len(warnings), 4)
        self.assertIn("medium", {warning["severity"] for warning in warnings})
        self.assertIn("low", {warning["severity"] for warning in warnings})

    def test_entity_drift_semantic_variant_not_flagged(self):
        outline = copy.deepcopy(self.base_outline)
        outline[2]["heading_text"] = "معايير النظافة العميقة داخل المنزل"

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)

        self.assertEqual(self._warnings(report, "ENTITY_DRIFT"), [])

    def test_entity_drift_high_still_passes_audit(self):
        outline = copy.deepcopy(self.base_outline)
        outline[2]["heading_text"] = "أهمية الوقت في اختيار القرار"
        outline[3]["heading_text"] = "راحة العملاء قبل اتخاذ الخطوة التالية"

        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)
        warnings = self._warnings(report, "ENTITY_DRIFT")

        self.assertTrue(warnings)
        self.assertEqual({warning["severity"] for warning in warnings}, {"high"})
        self.assertTrue(report["passed"])

    def test_real_estate_weak_h3_exemption(self):
        outline = copy.deepcopy(self.base_outline)
        outline[2]["subheadings"] = [
            "شقق استوديو وغرفة واحدة للأفراد",
            "شقق عائلية واسعة في مجمعات سكنية",
            "شقق مفروشة بالكامل للإيجار الشهري"
        ]
        args = copy.deepcopy(self.base_args)
        args["primary_keyword"] = "شقق للايجار في الرياض"
        args["content_type"] = "listing"
        
        report = self.validator.audit_heading_outline_quality(outline, **args)
        warnings = [w for w in report["warnings"] if w["code"] == "WEAK_H3" and w["section_id"] == "sec_2"]
        self.assertEqual(len(warnings), 0)

    def test_real_estate_pk_repetition(self):
        outline = copy.deepcopy(self.base_outline)
        outline[1]["heading_text"] = "شقق للايجار في الرياض حسب احتياجك"
        outline[2]["heading_text"] = "متوسط أسعار إيجار الشقق في أحياء الرياض الرئيسية"
        outline[3]["heading_text"] = "خطوات العثور على شقق مناسبة في الرياض"
        
        args = copy.deepcopy(self.base_args)
        args["primary_keyword"] = "شقق للايجار في الرياض"
        args["content_type"] = "listing"
        
        report = self.validator.audit_heading_outline_quality(outline, **args)
        warnings = [w for w in report["warnings"] if w["code"] == "PK_REPETITION"]
        # Only outline[1] is considered repetition, which is 1 count, so no warnings should fire.
        self.assertEqual(len(warnings), 0)
        
        # Test actual repetition
        outline[2]["heading_text"] = "أفضل خيارات شقق للايجار في الرياض"
        report_rep = self.validator.audit_heading_outline_quality(outline, **args)
        warnings_rep = [w for w in report_rep["warnings"] if w["code"] == "PK_REPETITION"]
        self.assertTrue(len(warnings_rep) > 0)

    def test_faq_weak_h3_exemption(self):
        outline = copy.deepcopy(self.base_outline)
        outline[3]["section_type"] = "faq"
        outline[3]["subheadings"] = ["كم السعر؟", "هل الخدمة جيدة؟"] # short questions
        report = self.validator.audit_heading_outline_quality(outline, **self.base_args)
        warnings = [w for w in report["warnings"] if w["code"] == "WEAK_H3" and w["section_id"] == "sec_3"]
        self.assertEqual(len(warnings), 0)

    def test_outline_remains_unchanged(self):
        outline = copy.deepcopy(self.base_outline)
        outline_copy = copy.deepcopy(self.base_outline)

        self.validator.audit_heading_outline_quality(outline, **self.base_args)

        self.assertEqual(outline, outline_copy)


if __name__ == "__main__":
    unittest.main()
