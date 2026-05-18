import logging
import sys
import io
from src.services.outline_repair_service import OutlineRepairService

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(message)s')

repair_service = OutlineRepairService()

outline = [
    {
        "heading_level": "H2",
        "heading_text": "مواعيد عمل حديقة الحيوان",
        "section_type": "visitor_information"
    },
    {
        "heading_level": "H2",
        "heading_text": "أسعار تذاكر الدخول وطريقة الحجز",
        "section_type": "visitor_information"
    },
    {
        "section_type": "faq",
        "heading_text": "أسئلة شائعة",
        "subheadings": [
            "ما هي أوقات العمل؟",          # Duplicates hours
            "كيف يمكن حجز التذاكر؟",        # Duplicates tickets
            "ما هي أبرز الأنشطة؟",         # Distinct (activities doesn't exist as H2)
            "هل توجد أسعار خاصة للأطفال؟", # Distinct variation (tickets + children)
            "هل يسمح بدخول الطعام؟"       # Distinct (no H2 overlap)
        ]
    },
    {
        "section_type": "conclusion",
        "heading_text": "تجربة متكاملة",
        "heading_level": "H2"
    }
]

serp_brief = {
    "brand_utility_candidates": ["كيف أحجز عبر تكت مكس؟"]
}

brand_context = "Official brand: TicketMix."
content_type = "informational"
entity_phrase = "حديقة الحيوان"

print("=== STARTING REPAIR PIPELINE ===")

print("\n1. DEDUPE FAQ AGAINST H2:")
outline = repair_service.dedupe_faq_against_h2(outline)

print("\n2. ENRICH BRAND UTILITY FAQ:")
outline = repair_service.enrich_brand_utility_faq(outline, serp_brief, brand_context, content_type)

print("\n3. CLEAN CONCLUSION HEADING:")
outline = repair_service.clean_conclusion_heading(outline, entity_phrase)

import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

print("\n=== FINAL OUTLINE ===")
import json
print(json.dumps(outline, indent=2, ensure_ascii=False))
