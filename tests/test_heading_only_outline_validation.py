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
