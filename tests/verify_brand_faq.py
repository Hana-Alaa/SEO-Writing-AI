import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from src.services.outline_repair_service import OutlineRepairService
svc = OutlineRepairService()

BRAND = "تيك ايفينت"
ENTITY = "بوليفارد سيتي الرياض"

def get_faq(outline):
    for s in outline:
        if s.get("section_type") == "faq" or "أسئلة شائعة" in s.get("heading_text", ""):
            return s.get("subheadings", [])
    return []

def show(faqs, label):
    print(f"\n=== {label} ===")
    print(f"Final count: {len(faqs)}")
    for q in faqs:
        t = q.get("heading_text", q) if isinstance(q, dict) else q
        print(f"  - {t}")
    brand_present = any(BRAND in (q.get("heading_text", q) if isinstance(q, dict) else str(q)) for q in faqs)
    print(f"Brand ('{BRAND}') present: {brand_present}")
    print(f"Count >= 4: {len(faqs) >= 4}")

# ── Scenario A: 4 FAQs after refill → brand must append as #5 ────────────────
outline_a = [
    {"heading_level": "H2", "heading_text": "مواعيد العمل", "section_type": "visitor_information"},
    {"heading_level": "H2", "heading_text": "أسعار التذاكر وطريقة الحجز", "section_type": "visitor_information"},
    {"section_type": "faq", "heading_text": "أسئلة شائعة", "subheadings": [
        "هل المكان مناسب للعائلات؟",
        "ما هي أبرز الفعاليات الموسمية؟",
        "هل تتوفر مواقف سيارات بالقرب من بوليفارد سيتي؟",
        "كيف يمكن الوصول بالمواصلات العامة؟",
    ]},
    {"section_type": "conclusion", "heading_text": "خلاصة", "heading_level": "H2"},
]
result_a = svc.enrich_brand_utility_faq(outline_a, {}, BRAND, "informational", entity_phrase=ENTITY)
show(get_faq(result_a), "Scenario A: 4 FAQs → brand appended as #5")

# ── Scenario B: 5 FAQs → weakest replaced, count stays at 5 ──────────────────
outline_b = [
    {"heading_level": "H2", "heading_text": "مواعيد العمل", "section_type": "visitor_information"},
    {"heading_level": "H2", "heading_text": "أسعار التذاكر", "section_type": "visitor_information"},
    {"section_type": "faq", "heading_text": "أسئلة شائعة", "subheadings": [
        "هل المكان مناسب للعائلات؟",
        "ما هي أبرز الفعاليات الموسمية؟",
        "هل تتوفر مواقف سيارات بالقرب من بوليفارد سيتي؟",
        "كيف يمكن الوصول بالمواصلات العامة؟",
        "هل المكان مناسب لذوي الاحتياجات الخاصة؟",
    ]},
    {"section_type": "conclusion", "heading_text": "خلاصة", "heading_level": "H2"},
]
result_b = svc.enrich_brand_utility_faq(outline_b, {}, BRAND, "informational", entity_phrase=ENTITY)
show(get_faq(result_b), "Scenario B: 5 FAQs → weakest replaced, no count drop")

# ── Scenario C: brand FAQ already present → no-op ─────────────────────────────
outline_c = [
    {"heading_level": "H2", "heading_text": "أسعار التذاكر", "section_type": "visitor_information"},
    {"section_type": "faq", "heading_text": "أسئلة شائعة", "subheadings": [
        "هل يمكن حجز تذاكر بوليفارد سيتي الرياض عبر تيك ايفينت؟",
        "ما هي أبرز الفعاليات الموسمية؟",
    ]},
]
result_c = svc.enrich_brand_utility_faq(outline_c, {}, BRAND, "informational", entity_phrase=ENTITY)
show(get_faq(result_c), "Scenario C: brand already present → skipped (no duplicate)")
