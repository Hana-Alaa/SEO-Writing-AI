import asyncio
from typing import Dict, Any, List
from src.services.content_generator import SectionWriter

class MockAIClient:
    async def send(self, *args, **kwargs):
        return {"content": "{}", "metadata": {}}

def run_audit():
    print("--- Operational Contract Adapter Audit ---")
    writer = SectionWriter(MockAIClient())
    
    # 1. Pricing / Market Practical
    sec_pricing = {
        "execution_mode": "market_practical",
        "taxonomy_axis": "area",
        "forbidden_taxonomy_axis": "category_or_type",
        "observed_data_mentions": ["35000 SAR"],
        "must_include_details": ["Annual payment options"]
    }
    instr_pricing = writer._build_operational_instructions(sec_pricing)
    print("\n[Pricing Section Instructions]")
    for i in instr_pricing: print(f"- {i}")
    
    # Assertions for pricing
    has_price_logic = any("prices vary" in i for i in instr_pricing)
    has_axis = any("Compare affordability primarily by the area" in i for i in instr_pricing)
    has_observed = any("cautious grounding hints" in i for i in instr_pricing)
    has_must_include = any("Integrate detail about: Annual payment options" in i for i in instr_pricing)
    
    print(f"Pricing Logic: {'PASSED' if has_price_logic else 'FAILED'}")
    print(f"Axis Enforcement: {'PASSED' if has_axis else 'FAILED'}")
    print(f"Data Grounding: {'PASSED' if has_observed else 'FAILED'}")
    print(f"Detail Integration: {'PASSED' if has_must_include else 'FAILED'}")

    # 2. Locality Analysis
    sec_locality = {
        "execution_mode": "locality_analysis",
        "must_include_details": ["Proximity to Metro"]
    }
    instr_locality = writer._build_operational_instructions(sec_locality)
    print("\n[Locality Section Instructions]")
    for i in instr_locality: print(f"- {i}")
    
    has_resident_needs = any("resident lifestyle needs" in i for i in instr_locality)
    print(f"Locality Logic: {'PASSED' if has_resident_needs else 'FAILED'}")

    # 3. Taxonomy Breakdown
    sec_taxonomy = {
        "execution_mode": "taxonomy_breakdown",
        "heading_text": "Apartment Types"
    }
    instr_taxonomy = writer._build_operational_instructions(sec_taxonomy)
    print("\n[Taxonomy Section Instructions]")
    for i in instr_taxonomy: print(f"- {i}")
    
    has_user_match = any("match each category to a specific user situation" in i for i in instr_taxonomy)
    print(f"Taxonomy Logic: {'PASSED' if has_user_match else 'FAILED'}")

    # 4. Legacy / Empty Field Compatibility
    sec_empty = {}
    try:
        instr_empty = writer._build_operational_instructions(sec_empty)
        print(f"\nEmpty Field Compatibility: PASSED (Found {len(instr_empty)} default instructions)")
    except Exception as e:
        print(f"\nEmpty Field Compatibility: FAILED ({e})")

if __name__ == "__main__":
    run_audit()
