import asyncio
import logging
from src.services.research_service import ResearchService

logging.basicConfig(level=logging.INFO)

async def test():
    # Mock AI Client
    class MockAIClient:
        pass
        
    client = MockAIClient()
    service = ResearchService(client, "test_workdir")
    
    state = {
        "brand_url": "https://goldenhost.co/",
        "primary_keyword": "شقق للايجار في الرياض",
        "output_dir": "test_workdir"
    }
    
    print("Running brand discovery...")
    assets = await service._discover_logo_and_colors("https://goldenhost.co/", state)
    print(f"\nDiscovered Assets: {assets}")
    
    result = await service.run_brand_discovery(state)
    print("\n--- Brand Discovery Results ---")
    print(f"Brand Name: {result.get('brand_name')}")
    print(f"Display Brand Name: {result.get('display_brand_name')}")
    print(f"Official Brand Name: {result.get('official_brand_name')}")
    print(f"Brand Aliases: {result.get('brand_aliases')}")
    print(f"Domain Brand Name: {result.get('domain_brand_name')}")
    print(f"Brand Context: {result.get('brand_context')}")
    print(f"Brand Source: {result.get('brand_source')}")

if __name__ == "__main__":
    asyncio.run(test())
