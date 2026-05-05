import asyncio
import sys
from src.services.strategy_service import StrategyService
from unittest.mock import MagicMock

# Force UTF-8 output if possible
if sys.stdout.encoding != 'utf-8':
    try:
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    except:
        pass

async def verify_location_calibration():
    # Setup
    strategy_service = StrategyService(
        ai_client=MagicMock(),
        title_generator=MagicMock(),
        jinja_env=MagicMock()
    )
    
    # Test Case 1: Property/Listing
    state_listing = {
        "primary_keyword": "شقق للايجار في الرياض",
        "raw_title": "شقق للايجار في الرياض",
        "content_type": "listing",
        "intent": "commercial_local",
        "area": "الرياض",
        "display_brand_name": "عقاراتي"
    }
    
    strategy_listing = {
        "section_role_map": {}
    }
    
    result_listing = strategy_service._apply_dynamic_section_role_overrides(strategy_listing, state_listing)
    
    print("--- Test Case 1: Real Estate Listing (Riyadh Apartments) ---")
    print(f"Location Rule: {result_listing['section_role_map'].get('location_or_distribution')}")
    print(f"Enforced Rules: {result_listing['enforced_structural_rules']}")
    print("\n")

    # Test Case 2: Local Service
    state_service = {
        "primary_keyword": "شركة تنظيف بالرياض",
        "raw_title": "شركة تنظيف بالرياض",
        "content_type": "brand_commercial",
        "intent": "commercial_local",
        "area": "الرياض",
        "display_brand_name": "كلين"
    }
    
    strategy_service_data = {
        "section_role_map": {}
    }
    
    result_service = strategy_service._apply_dynamic_section_role_overrides(strategy_service_data, state_service)
    
    print("--- Test Case 2: Local Service (Cleaning Company) ---")
    print(f"Location Rule: {result_service['section_role_map'].get('location_or_distribution')}")
    print(f"Enforced Rules: {result_service['enforced_structural_rules']}")
    print("\n")

    # Test Case 3: Informational
    state_info = {
        "primary_keyword": "طرق تبييض الأسنان",
        "raw_title": "طرق تبييض الأسنان",
        "content_type": "informational",
        "intent": "informational",
        "area": "",
        "display_brand_name": ""
    }
    
    strategy_info = {
        "section_role_map": {}
    }
    
    result_info = strategy_service._apply_dynamic_section_role_overrides(strategy_info, state_info)
    
    print("--- Test Case 3: Informational (Teeth Whitening) ---")
    print(f"Location Rule: {result_info['section_role_map'].get('location_or_distribution')}")
    print(f"Enforced Rules: {result_info['enforced_structural_rules']}")

if __name__ == "__main__":
    asyncio.run(verify_location_calibration())
