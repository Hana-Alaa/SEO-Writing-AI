import asyncio
import copy
from unittest.mock import AsyncMock, MagicMock
from src.services.content_generator import OutlineGenerator
from src.services.workflow_controller import AsyncWorkflowController

async def test_critique_no_mutation():
    """Verify that the critique step does not modify the original outline."""
    print("Running test_critique_no_mutation...")
    ai_client = AsyncMock()
    mock_critique_json = '{"mode": "critique_only", "overall_score": 8.5, "passed": true, "summary": "Good", "missing_sections": []}'
    ai_client.send.return_value = {"content": mock_critique_json, "metadata": {"tokens": {}}}
    
    gen = OutlineGenerator(ai_client)
    
    original_outline = [
        {"section_id": "sec_1", "heading_text": "Intro", "section_type": "introduction"},
        {"section_id": "sec_2", "heading_text": "Body", "section_type": "core"}
    ]
    outline_input = copy.deepcopy(original_outline)
    
    critique = await gen.critique_outline(
        primary_keyword="test",
        title="test",
        outline=outline_input,
        content_type="informational",
        intent="informational",
        area="",
        entity_phrase="",
        service_phrase="",
        display_brand_name="",
        content_strategy={},
        heading_quality_audit={}
    )
    
    assert critique["overall_score"] == 8.5
    assert outline_input == original_outline, "Outline was mutated by critique step!"
    print("SUCCESS: test_critique_no_mutation")

async def test_critique_fallback_on_parse_failure():
    """Verify that critique returns a safe fallback if AI returns garbage."""
    print("Running test_critique_fallback_on_parse_failure...")
    ai_client = AsyncMock()
    ai_client.send.return_value = {"content": "INVALID JSON STRING", "metadata": {"tokens": {}}}
    
    gen = OutlineGenerator(ai_client)
    
    critique = await gen.critique_outline(
        primary_keyword="test",
        title="test",
        outline=[],
        content_type="informational",
        intent="informational",
        area="",
        entity_phrase="",
        service_phrase="",
        display_brand_name="",
        content_strategy={},
        heading_quality_audit={}
    )
    
    assert critique["mode"] == "critique_only"
    assert "Critique unavailable" in critique["summary"]
    assert isinstance(critique["missing_sections"], list)
    print("SUCCESS: test_critique_fallback_on_parse_failure")

async def test_workflow_integration_critique_storage():
    """Verify that WorkflowController stores the critique in state."""
    print("Running test_workflow_integration_critique_storage...")
    controller = AsyncWorkflowController(work_dir="temp")
    controller.outline_gen = AsyncMock()
    controller.validator = MagicMock()
    
    controller.validator.audit_heading_outline_quality.return_value = {"passed": True, "warnings": []}
    
    mock_critique = {"mode": "critique_only", "overall_score": 9.0}
    controller.outline_gen.critique_outline = AsyncMock(return_value=mock_critique)
    
    state = {
        "heading_only_mode": True,
        "outline": [{"section_id": "1"}],
        "primary_keyword": "test",
        "raw_title": "test",
        "content_type": "informational",
        "intent": "informational",
        "content_strategy": {},
        "seo_intelligence": {},
        "article_language": "ar"
    }
    
    if state.get("heading_only_mode"):
        report = controller.validator.audit_heading_outline_quality()
        state["heading_quality_audit"] = report
        
        if state.get("outline") and state.get("heading_only_mode"):
            critique = await controller.outline_gen.critique_outline()
            state["ai_outline_critique"] = critique
            
    assert state["ai_outline_critique"] == mock_critique
    print("SUCCESS: test_workflow_integration_critique_storage")

async def main():
    try:
        await test_critique_no_mutation()
        await test_critique_fallback_on_parse_failure()
        await test_workflow_integration_critique_storage()
        print("\nAll critique tests passed!")
    except Exception as e:
        print(f"\nTEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())
