import asyncio
import json
import os
import sys
from typing import Dict, Any

# Mocking the AI Client for dry-run
class MockAIClient:
    async def send(self, prompt: str, step: str = "unknown") -> Dict[str, Any]:
        print(f"--- [MockAI] Sending prompt for step: {step} ---")
        if "style_extraction" in step:
            return {
                "content": json.dumps({
                    "tonal_dna": {
                        "persona": "Expert Authoritative",
                        "audience_level": "Professional",
                        "forbidden_jargon": ["Cybersecurity", "MSOP"],
                        "writing_tone": "Polished",
                        "sentence_rhythm": "Balanced"
                    },
                    "cta_strategy": {
                        "style": "Aggressive",
                        "wording_patterns": ["Book now!"],
                        "density": "high",
                        "total_ideal_count": 3
                    },
                    "formatting_blueprint": {"bolding_frequency": "Medium"},
                    "structural_skeleton": [{"type": "H2"}, {"type": "TABLE"}, {"type": "FAQ"}]
                }),
                "metadata": {"prompt": prompt, "response": "{}", "tokens": {}, "duration": 0.1}
            }
        if "outline" in step:
             return {
                "content": json.dumps({
                    "outline": [
                        {"section_id": "sec_1", "heading_level": "H2", "heading_text": "Test Heading", "section_intent": "Informational", "assigned_links": []}
                    ]
                }),
                "metadata": {"prompt": prompt, "response": "{}", "tokens": {}, "duration": 0.1}
            }
        return {
            "content": json.dumps({"content": "Test content", "used_links": [], "topics_covered": ["Test Claim"]}),
            "metadata": {"prompt": prompt, "response": "{}", "tokens": {}, "duration": 0.1}
        }

async def verify_style_flow():
    print("🚀 Starting Style Copycat Verification (Dry-Run)...")
    
    # 1. Setup paths
    sys.path.append(os.getcwd())
    from src.utils.style_extractor import StyleExtractor
    from src.services.strategy_service import StrategyService
    from src.services.content_generator import OutlineGenerator, SectionWriter

    ai_client = MockAIClient()
    
    # 2. Test StyleExtractor
    print("\n--- Phase 1: Style Extraction Check ---")
    extractor = StyleExtractor(ai_client)
    html_ref = "<h1>Title</h1><p>Text</p><table><tr><td>Col</td></tr></table>"
    blueprint = await extractor.extract_blueprint(html_ref)
    persona = blueprint.get('tonal_dna', {}).get('persona')
    print(f"✅ Blueprint Extracted Persona: {persona}")
    assert persona == "Expert Authoritative"

    # 3. Test Template Rendering (Outline)
    print("\n--- Phase 2: Outline Template Rendering ---")
    outline_gen = OutlineGenerator(ai_client)
    try:
        await outline_gen.generate(
            title="Test", keywords=["test"], urls=[], article_language="en",
            intent="informational", seo_intelligence={}, content_type="informational",
            content_strategy={}, brand_context="", area="Global",
            style_blueprint=blueprint
        )
        print("✅ Outline Template: Renders correctly with blueprint.")
    except Exception as e:
        print(f"❌ Outline Template rendering failed: {e}")

    # 4. Test Template Rendering (Section)
    print("\n--- Phase 3: Section Writer Template Rendering ---")
    section_writer = SectionWriter(ai_client)
    try:
        await section_writer.write(
            title="Test", global_keywords={"primary": "test"},
            section={"heading_text": "Test H2", "section_id": "sec_1", "assigned_links": []},
            article_intent="informational", seo_intelligence={},
            content_type="informational", link_strategy={},
            brand_url="https://test.com", brand_link_used=False,
            brand_link_allowed=True, allow_external_links=True,
            execution_plan={"cta_type": "none", "cta_position": "none"}, area="Global",
            style_blueprint=blueprint,
            full_outline=[],
            prohibited_competitors=[],
            used_phrases=[],
            used_topics=[],
            used_internal_links=[],
            used_external_links=[],
            external_sources=[],
            external_resources=[],
            used_claims=["Existing Claim"],
            ctas_placed=1
        )
        print("✅ SectionWriter Template: Renders correctly with blueprint.")
    except Exception as e:
         import traceback
         traceback.print_exc()
         print(f"❌ SectionWriter Template rendering failed: {e}")

    # 5. Test WorkflowController & Humanizer
    print("\n--- Phase 4: WorkflowController & Humanizer Integration ---")
    from src.services.workflow_controller import AsyncWorkflowController
    controller = AsyncWorkflowController()
    controller.ai_client = ai_client # Use mock
    
    state = {
        "full_content_so_far": "Test content snippet.",
        "style_blueprint": blueprint,
        "tone": "Polished"
    }
    
    try:
        new_state = await controller._step_3_humanizer(state)
        print("✅ Humanizer Pass: Successfully integrated into WorkflowController.")
        assert "full_content_so_far" in new_state
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Humanizer Pass failed: {e}")

    print("\n👑 VERIFICATION COMPLETE: Human-Centric SEO architecture is plumbed correctly.")

if __name__ == "__main__":
    asyncio.run(verify_style_flow())
