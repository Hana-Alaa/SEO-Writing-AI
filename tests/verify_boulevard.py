import requests
import json
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

payload = {
    "title": "بوليفارد سيتي الرياض",
    "keywords": '["بوليفارد سيتي الرياض"]',
    "article_language": "ar",
    "area": "الرياض",
    "urls": "[]",
    "external_urls": "[]",
    "include_meta_keywords": "true",
    "generate_images": "false",
    "workflow_mode": "core",
    "article_size": "1000",
    "include_conclusion": "true",
    "include_faq": "true",
    "include_tables": "true",
    "include_bullet_lists": "true",
    "include_comparison_blocks": "false",
    "bold_key_terms": "true",
    "num_images": "0",
    "image_style": "illustration",
    "image_size": "1024x1024",
    "include_featured_image": "false",
    "custom_branding_frame": "false",
    "secondary_keywords": "[]",
    "competitor_count": "3",
    "heading_only_mode": "true",
    "brand_voice_description": "تيك ايفينت",
}

print("Sending request to /generate ...")
try:
    resp = requests.post("http://localhost:8000/generate", data=payload, timeout=180)
    resp.raise_for_status()
    result = resp.json()
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)

outline = result.get("outline_structure", [])
preview = result.get("heading_preview_markdown", "")

print("\n=== HEADING PREVIEW MARKDOWN ===")
print(preview or "(empty)")

print("\n=== STRUCTURED OUTLINE (heading_text only) ===")
for section in outline:
    h_level = section.get("heading_level", "")
    h_text = section.get("heading_text", "")
    s_type = section.get("section_type", "")
    subs = section.get("subheadings", [])
    print(f"  [{h_level}] ({s_type}) {h_text}")
    for sub in subs:
        if isinstance(sub, dict):
            print(f"      [H3] {sub.get('heading_text', sub)}")
        else:
            print(f"      [H3] {sub}")

# --- Verification Checks ---
print("\n=== VERIFICATION CHECKLIST ===")

h2_texts = [s.get("heading_text", "").lower() for s in outline if s.get("heading_level") == "H2"]

# Check 1: no duplicate hours vs best-time H2
hours_h2 = any(any(kw in h for kw in ["مواعيد", "ساعات", "أوقات عمل"]) for h in h2_texts)
best_time_h2 = any(any(kw in h for kw in ["أفضل وقت", "وقت الزيارة", "زيارة متى", "تخطيط"]) for h in h2_texts)
both_present = hours_h2 and best_time_h2
print(f"[{'FAIL' if both_present else 'PASS'}] Hours H2 present: {hours_h2} | Best-time H2 present: {best_time_h2} | Both: {both_present}")

# Check 2: Brand FAQ present
faq_sections = [s for s in outline if s.get("section_type") == "faq" or "أسئلة" in s.get("heading_text", "")]
brand_faq_found = False
brand_faq_text = ""
brand_promotional = False
standalone_brand_h2 = False

for section in faq_sections:
    for sub in section.get("subheadings", []):
        sub_text = sub.get("heading_text", sub) if isinstance(sub, dict) else str(sub)
        if any(kw in sub_text.lower() for kw in ["تيك ايفينت", "ticketevent", "تيك", "المنصة", "الحجز عبر"]):
            brand_faq_found = True
            brand_faq_text = sub_text
            if any(promo in sub_text.lower() for promo in ["أفضل منصة", "لماذا", "احجز الآن", "book now"]):
                brand_promotional = True

for section in outline:
    h_text = section.get("heading_text", "").lower()
    if section.get("heading_level") == "H2" and any(kw in h_text for kw in ["تيك ايفينت", "المنصة"]):
        standalone_brand_h2 = True

print(f"[{'PASS' if brand_faq_found else 'FAIL'}] Brand FAQ found: {brand_faq_found} | Text: '{brand_faq_text}'")
print(f"[{'FAIL' if brand_promotional else 'PASS'}] Brand FAQ promotional: {brand_promotional}")
print(f"[{'FAIL' if standalone_brand_h2 else 'PASS'}] Standalone brand H2: {standalone_brand_h2}")

# Check 3: Conclusion is practical
conclusion_sections = [s for s in outline if s.get("section_type") == "conclusion"]
if not conclusion_sections:
    # Try last H2
    h2_sections = [s for s in outline if s.get("heading_level") == "H2"]
    if h2_sections:
        conclusion_sections = [h2_sections[-1]]

editorial_phrases = ["تجربة زيارة", "استكشاف المزيد", "خطواتك القادمة", "خلاصة وتجربة", "تجربة متكاملة"]
conclusion_editorial = False
conclusion_text = ""
for s in conclusion_sections:
    conclusion_text = s.get("heading_text", "")
    if any(ep in conclusion_text for ep in editorial_phrases):
        conclusion_editorial = True

print(f"[{'FAIL' if conclusion_editorial else 'PASS'}] Conclusion editorial: {conclusion_editorial} | Text: '{conclusion_text}'")

print("\n=== DONE ===")
