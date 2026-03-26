import asyncio
import logging
from typing import Dict, Any
from src.services.content_generator import SectionWriter, OutlineGenerator

# Mock AI Client that doesn't make real calls
class MockAIClient:
    async def send(self, prompt, step=None):
        return {"content": '{"content": "mock"}', "metadata": {}}

async def test_resilience():
    logging.basicConfig(level=logging.ERROR)
    mock_ai = MockAIClient()
    
    # WORST CASE DATA
    broken_style_blueprint = {
        "tonal_dna": {"persona": "Test Persona"},
        "cta_strategy": {"density": "high"}
    }
    
    # 3. Test SectionWriter (Explicit Keyword Args)
    print("\n--- Testing SectionWriter Rendering with Broken Data ---")
    try:
        writer = SectionWriter(ai_client=mock_ai)
        await writer.write(
            title="Test Title",
            global_keywords={},
            section={"heading_text": "Test H2", "assigned_keywords": []},
            article_intent="Informational",
            seo_intelligence={},
            content_type="informational",
            link_strategy="manual",
            brand_url="https://test.com",
            brand_link_used=False,
            brand_link_allowed=True,
            allow_external_links=True,
            execution_plan={},
            area="Cairo",
            style_blueprint=broken_style_blueprint,
            serp_data={},
            full_outline=[] # Explicitly empty
        )
        print("✅ SUCCESS: SectionWriter rendered without crashing.")
    except Exception as e:
        import traceback
        print(f"❌ FAILURE: SectionWriter crashed: {e}")
        traceback.print_exc()

    # 4. Test OutlineGenerator (Explicit Keyword Args)
    print("\n--- Testing OutlineGenerator Rendering with Empty Data ---")
    try:
        outliner = OutlineGenerator(ai_client=mock_ai)
        await outliner.generate(
            title="Test Title",
            keywords=[],
            urls=[],
            article_language="ar",
            intent="Informational",
            seo_intelligence={},
            style_blueprint={},
            content_type="informational",
            content_strategy={},
            brand_context="",
            area="Cairo"
        )
        print("✅ SUCCESS: OutlineGenerator rendered without crashing.")
    except Exception as e:
        import traceback
        print(f"❌ FAILURE: OutlineGenerator crashed: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_resilience())
