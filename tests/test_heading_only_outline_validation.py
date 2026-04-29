import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.services.validation_service import ValidationService


def test_heading_only_outline_validation_accepts_specific_headings():
    validator = ValidationService()
    outline = [
        {
            "section_id": "sec_01",
            "heading_text": "Opening promise",
            "heading_level": "H2",
            "section_type": "introduction",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_02",
            "heading_text": "Apartment prices in New Cairo by district",
            "heading_level": "H2",
            "section_type": "core",
            "section_intent": "informational",
            "subheadings": [
                "Districts with lower entry prices",
                "How payment plans change total cost",
            ],
        },
        {
            "section_id": "sec_03",
            "heading_text": "What amenities matter before you choose",
            "heading_level": "H2",
            "section_type": "core",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_04",
            "heading_text": "Common buying mistakes in New Cairo compounds",
            "heading_level": "H2",
            "section_type": "core",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_05",
            "heading_text": "Questions buyers ask before choosing a unit",
            "heading_level": "H2",
            "section_type": "faq",
            "section_intent": "informational",
            "subheadings": [
                "What down payment is common in New Cairo",
                "How long does handover usually take",
            ],
        },
        {
            "section_id": "sec_06",
            "heading_text": "Next steps before you compare projects",
            "heading_level": "H2",
            "section_type": "conclusion",
            "section_intent": "informational",
            "subheadings": [],
        },
    ]

    errors = validator.validate_heading_outline_quality(
        outline,
        content_type="informational",
        area="New Cairo",
    )

    assert errors == []


def test_heading_only_outline_validation_rejects_generic_headings():
    validator = ValidationService()
    outline = [
        {
            "section_id": "sec_01",
            "heading_text": "Opening promise",
            "heading_level": "H2",
            "section_type": "introduction",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_02",
            "heading_text": "Overview",
            "heading_level": "H2",
            "section_type": "core",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_03",
            "heading_text": "Pricing",
            "heading_level": "H2",
            "section_type": "core",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_04",
            "heading_text": "FAQ",
            "heading_level": "H2",
            "section_type": "faq",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_05",
            "heading_text": "Conclusion",
            "heading_level": "H2",
            "section_type": "conclusion",
            "section_intent": "informational",
            "subheadings": [],
        },
    ]

    errors = validator.validate_heading_outline_quality(
        outline,
        content_type="informational",
        area="New Cairo",
    )

    assert any("GENERIC_HEADING_LABEL" in error for error in errors)


def test_heading_only_outline_validation_rejects_proof_heading_without_sale_intent():
    validator = ValidationService()
    outline = [
        {
            "section_id": "sec_01",
            "heading_text": "مقدمة عن سوق شقق القاهرة الجديدة",
            "heading_level": "INTRO",
            "section_type": "introduction",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_02",
            "heading_text": "كيف تختار أفضل شقق للبيع فى القاهرة الجديدة حسب ميزانيتك؟",
            "heading_level": "H2",
            "section_type": "offer",
            "section_intent": "commercial",
            "subheadings": [],
        },
        {
            "section_id": "sec_03",
            "heading_text": "ما هي أنواع ومساحات الشقق المتاحة في القاهرة الجديدة؟",
            "heading_level": "H2",
            "section_type": "features",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_04",
            "heading_text": "متوسط أسعار شقق القاهرة الجديدة حسب المنطقة وأهم العوامل المؤثرة",
            "heading_level": "H2",
            "section_type": "proof",
            "section_intent": "commercial",
            "subheadings": [],
        },
        {
            "section_id": "sec_05",
            "heading_text": "لماذا تختار عقار يا مصر للبحث عن شقق للبيع فى القاهرة الجديدة؟",
            "heading_level": "H2",
            "section_type": "differentiation",
            "section_intent": "commercial",
            "subheadings": [],
        },
        {
            "section_id": "sec_06",
            "heading_text": "هل تختار شقق جاهزة أم تحت الإنشاء في القاهرة الجديدة؟",
            "heading_level": "H2",
            "section_type": "comparison",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_07",
            "heading_text": "خطوات شراء شقق القاهرة الجديدة من المعاينة حتى القرار",
            "heading_level": "H2",
            "section_type": "process",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_08",
            "heading_text": "أهم الأسئلة الشائعة حول شراء شقق في القاهرة الجديدة",
            "heading_level": "H2",
            "section_type": "faq",
            "section_intent": "informational",
            "subheadings": [
                "ما هي أفضل الأحياء للسكن العائلي في القاهرة الجديدة؟",
                "كم يبلغ متوسط سعر المتر للشقق في التجمع الخامس حالياً؟",
            ],
        },
        {
            "section_id": "sec_09",
            "heading_text": "ابدأ الآن في اختيار شقتك المثالية في القاهرة الجديدة",
            "heading_level": "H2",
            "section_type": "conclusion",
            "section_intent": "commercial",
            "subheadings": [],
        },
    ]

    errors = validator.validate_heading_outline_quality(
        outline,
        content_type="brand_commercial",
        area="القاهرة الجديدة",
        primary_keyword="شقق للبيع فى القاهرة الجديدة",
        brand_name="عقار يا مصر",
        content_strategy={},
        seo_intelligence={},
    )

    assert any("PRICE_KEYWORD_INTENT_MISSING" in error for error in errors)


def test_prune_unsupported_optional_subheadings_removes_unjustified_financing_faq():
    validator = ValidationService()
    outline = [
        {
            "section_id": "sec_01",
            "heading_text": "أهم الأسئلة الشائعة حول شراء شقق في القاهرة الجديدة",
            "heading_level": "H2",
            "section_type": "faq",
            "section_intent": "informational",
            "subheadings": [
                "ما هي أفضل الأحياء للسكن العائلي في القاهرة الجديدة؟",
                "هل تتوفر شقق للبيع بتسهيلات في السداد خارج الكمبوندات؟",
            ],
        }
    ]

    cleaned = validator.prune_unsupported_optional_subheadings(
        outline,
        primary_keyword="شقق للبيع فى القاهرة الجديدة",
        content_strategy={},
        seo_intelligence={
            "market_analysis": {
                "semantic_assets": {"paa_questions": [], "related_searches": []},
                "market_insights": {"keyword_clusters": [], "content_gaps": []},
            }
        },
    )

    assert cleaned[0]["subheadings"] == [
        "ما هي أفضل الأحياء للسكن العائلي في القاهرة الجديدة؟"
    ]


def test_price_factor_h3s_support_price_proof_parent():
    validator = ValidationService()
    keyword_profile = validator._derive_keyword_profile(
        "شقق للبيع فى القاهرة الجديدة",
        "القاهرة الجديدة",
    )

    assert validator._h3_supports_parent(
        "متوسط أسعار شقق للبيع في القاهرة الجديدة وأهم العوامل المؤثرة",
        "تأثير القرب من التسعين الشمالي والجنوبي على السعر",
        keyword_profile,
    )

    assert validator._h3_supports_parent(
        "متوسط أسعار شقق للبيع في القاهرة الجديدة وأهم العوامل المؤثرة",
        "تأثير حالة التشطيب على إجمالي سعر الشقة",
        keyword_profile,
    )


def test_comparison_section_accepts_compound_vs_standalone_decision_angle():
    validator = ValidationService()

    assert validator._comparison_section_has_decision_angle(
        "هل تختار شقق داخل كمبوند أم في عمارات مستقلة؟",
        [],
    )


def test_heading_outline_validation_rejects_cross_entity_property_types():
    validator = ValidationService()
    outline = [
        {
            "section_id": "sec_01",
            "heading_text": "مقدمة عن البحث عن شقق للايجار في الرياض",
            "heading_level": "INTRO",
            "section_type": "introduction",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_02",
            "heading_text": "أفضل شقق للايجار في الرياض حسب احتياجك",
            "heading_level": "H2",
            "section_type": "offer",
            "section_intent": "commercial",
            "subheadings": [
                "شاليهات مفروشة قريبة من المطار",
                "فلل سكنية للعائلات الكبيرة",
            ],
        },
        {
            "section_id": "sec_03",
            "heading_text": "أنواع الشقق المتاحة في الرياض",
            "heading_level": "H2",
            "section_type": "features",
            "section_intent": "informational",
            "subheadings": ["شقق استوديو"],
        },
        {
            "section_id": "sec_04",
            "heading_text": "متوسط أسعار شقق للايجار في الرياض حسب المنطقة",
            "heading_level": "H2",
            "section_type": "proof",
            "section_intent": "commercial",
            "subheadings": [],
        },
        {
            "section_id": "sec_05",
            "heading_text": "ابدأ في اختيار الشقة المناسبة في الرياض",
            "heading_level": "H2",
            "section_type": "conclusion",
            "section_intent": "commercial",
            "subheadings": [],
        },
    ]

    errors = validator.validate_heading_outline_quality(
        outline,
        content_type="brand_commercial",
        area="الرياض",
        primary_keyword="شقق للايجار في الرياض",
        brand_name="Golden Host",
        content_strategy={},
        seo_intelligence={},
    )

    assert any("H3_ENTITY_FAMILY_DRIFT" in error for error in errors)
