import asyncio
import os
from typing import Dict, Any
from src.services.content_generator import OutlineGenerator, SectionWriter
from src.services.strategy_service import SEMANTIC_EXECUTION_LAYER, WRITER_MODE_PROFILES
from jinja2 import Environment, FileSystemLoader

class MockAIClient:
    async def send(self, *args, **kwargs):
        return {"content": "{}", "metadata": {}}

def run_audit():
    print("--- Runtime Writing Modes Audit ---")
    
    # 1. Verify Mode Profiles Registry
    required_modes = [
        "market_practical", "locality_analysis", "taxonomy_breakdown",
        "comparison_decision", "buyer_guidance", "trust_proof", "onboarding_context"
    ]
    profiles_exist = all(m in WRITER_MODE_PROFILES for m in required_modes)
    print(f"Mode Profiles Registry Complete: {'PASSED' if profiles_exist else 'FAILED'}")

    # 2. Verify SectionWriter Prompt Injection Logic
    # We'll mock the template environment to capture the rendered prompt
    writer = SectionWriter(MockAIClient())
    
    # Test Section: Market Practical
    section = {
        "heading_text": "أسعار الشقق",
        "execution_mode": "market_practical",
        "semantic_goal": "realistic cost and value expectations",
        "decision_frame": "budget vs quality vs location",
        "content_behavior": "Focus on data-driven tiers."
    }
    
    # Mock template.render to see what it receives
    class MockTemplate:
        def render(self, **kwargs):
            self.last_kwargs = kwargs
            return "Rendered Prompt"
    
    mock_template = MockTemplate()
    writer.env.get_template = lambda x: mock_template
    
    # Run write() - we need to wrap it because it's async
    async def test_write():
        await writer.write(
            title="Test",
            global_keywords={},
            section=section,
            article_intent="Commercial",
            seo_intelligence={},
            content_type="brand_commercial",
            link_strategy="internal",
            brand_url="",
            brand_link_used=False,
            brand_link_allowed=True,
            allow_external_links=True,
            execution_plan={},
            area="الرياض"
        )
    
    asyncio.run(test_write())
    
    injected_instructions = mock_template.last_kwargs.get("mode_instructions")
    expected_instructions = WRITER_MODE_PROFILES["market_practical"]
    
    injection_success = injected_instructions == expected_instructions
    print(f"Correct Mode Instructions Injected (market_practical): {'PASSED' if injection_success else 'FAILED'}")
    
    # 3. Verify Fallback
    section_no_mode = {"heading_text": "Untitled"}
    async def test_fallback():
        await writer.write(
            title="Test", global_keywords={}, section=section_no_mode, 
            article_intent="Commercial", seo_intelligence={}, content_type="brand_commercial",
            link_strategy="internal", brand_url="", brand_link_used=False, brand_link_allowed=True,
            allow_external_links=True, execution_plan={}, area="الرياض"
        )
    asyncio.run(test_fallback())
    fallback_instructions = mock_template.last_kwargs.get("mode_instructions")
    fallback_success = fallback_instructions == WRITER_MODE_PROFILES["taxonomy_breakdown"]
    print(f"Safe Fallback to taxonomy_breakdown: {'PASSED' if fallback_success else 'FAILED'}")

    # 4. Snippet Generations
    print("\n--- Example Rendered Prompt Snippets ---")
    
    # Locality Analysis Snippet
    section_loc = {"heading_text": "الأحياء", "execution_mode": "locality_analysis"}
    async def get_loc_snippet():
        await writer.write(
            title="Test", global_keywords={}, section=section_loc, 
            article_intent="Commercial", seo_intelligence={}, content_type="brand_commercial",
            link_strategy="internal", brand_url="", brand_link_used=False, brand_link_allowed=True,
            allow_external_links=True, execution_plan={}, area="الرياض"
        )
    asyncio.run(get_loc_snippet())
    loc_instr = mock_template.last_kwargs.get("mode_instructions")
    print(f"LOCALITY_ANALYSIS instructions:\n{loc_instr}")

    # Comparison Decision Snippet
    section_comp = {"heading_text": "مقارنة", "execution_mode": "comparison_decision"}
    async def get_comp_snippet():
        await writer.write(
            title="Test", global_keywords={}, section=section_comp, 
            article_intent="Commercial", seo_intelligence={}, content_type="brand_commercial",
            link_strategy="internal", brand_url="", brand_link_used=False, brand_link_allowed=True,
            allow_external_links=True, execution_plan={}, area="الرياض"
        )
    asyncio.run(get_comp_snippet())
    comp_instr = mock_template.last_kwargs.get("mode_instructions")
    print(f"\nCOMPARISON_DECISION instructions:\n{comp_instr}")

if __name__ == "__main__":
    run_audit()
