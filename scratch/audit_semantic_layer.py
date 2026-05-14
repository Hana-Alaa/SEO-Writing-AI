import asyncio
from typing import Dict, Any
from src.services.content_generator import OutlineGenerator, SectionWriter
from src.services.strategy_service import SEMANTIC_EXECUTION_LAYER

class MockAIClient:
    async def send(self, *args, **kwargs):
        return {"content": "{}", "metadata": {}}

def run_test():
    generator = OutlineGenerator(MockAIClient())
    results = []

    # 1. Pricing Mapping
    sec = {"heading_text": "أسعار الشقق", "section_type": "core"}
    generator._normalize_section(sec, 1, "brand_commercial", {}, "الرياض")
    results.append(("Pricing", sec["execution_mode"] == "market_practical"))

    # 2. Location Mapping
    sec = {"heading_text": "أحياء الرياض", "section_type": "core"}
    generator._normalize_section(sec, 1, "brand_commercial", {}, "الرياض")
    results.append(("Location", sec["execution_mode"] == "locality_analysis"))

    # 3. Type/Category Mapping
    sec = {"heading_text": "أنواع الشقق", "section_type": "core"}
    generator._normalize_section(sec, 1, "brand_commercial", {}, "الرياض")
    results.append(("Type/Category", sec["execution_mode"] == "taxonomy_breakdown"))

    # 4. Comparison Mapping
    sec = {"heading_text": "مقارنة الخيارات"}
    generator._normalize_section(sec, 1, "brand_commercial", {}, "الرياض")
    results.append(("Comparison", sec["execution_mode"] == "comparison_decision"))

    # 5. FAQ Mapping (Should NOT inherit pricing/location)
    sec = {"heading_text": "سعر الشقة وموقعها", "section_type": "faq"}
    generator._normalize_section(sec, 1, "brand_commercial", {}, "الرياض")
    results.append(("FAQ Priority", sec["execution_mode"] == "buyer_guidance"))

    # 6. Legacy/Default
    sec = {"heading_text": "Generic Section"}
    generator._normalize_section(sec, 1, "brand_commercial", {}, "الرياض")
    results.append(("Default", sec["execution_mode"] == "taxonomy_breakdown"))
    results.append(("Safe Fields Exist", "semantic_goal" in sec))

    # 7. SectionWriter Payload Verification
    # We can't easily run write() without more mocks, but we can inspect the safe_section logic
    # if we look at the code or simulate it.
    
    print("--- Mapping Audit Results ---")
    all_passed = True
    for name, passed in results:
        status = "PASSED" if passed else "FAILED"
        print(f"{name}: {status}")
        if not passed: all_passed = False
    
    if all_passed:
        print("\nAll semantic mapping tests PASSED.")
    else:
        print("\nSome tests FAILED.")

if __name__ == "__main__":
    run_test()
