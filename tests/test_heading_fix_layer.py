import asyncio
import copy
from unittest.mock import AsyncMock, MagicMock
from src.services.workflow_controller import AsyncWorkflowController
from src.services.content_generator import OutlineGenerator

async def test_heading_fix_acceptance_logic():
    """Verify the acceptance and rejection logic of the heading fix layer."""
    print("Running test_heading_fix_acceptance_logic...")
    
    controller = AsyncWorkflowController(work_dir="temp")
    controller.outline_gen = AsyncMock()
    controller.validator = MagicMock()
    
    # Original state
    original_outline = [
        {"section_id": "sec_1", "heading_text": "Intro", "section_type": "introduction", "section_intent": "info", "heading_level": "INTRO"},
        {"section_id": "sec_2", "heading_text": "Generic Heading", "section_type": "offer", "section_intent": "commercial", "heading_level": "H2"}
    ]
    
    state = {
        "heading_only_mode": True,
        "outline": copy.deepcopy(original_outline),
        "primary_keyword": "test",
        "heading_quality_audit": {"warnings": [{"id": "GENERIC_H2", "severity": "medium"}]},
        "ai_outline_critique": {"overall_score": 7.0}
    }
    
    # CASE 1: Valid Fix
    fixed_outline = copy.deepcopy(original_outline)
    fixed_outline[1]["heading_text"] = "Fixed Specific Heading"
    
    controller.outline_gen.fix_outline_headings.return_value = {
        "outline": fixed_outline,
        "changes": [{"section_id": "sec_2", "field": "heading_text", "before": "Generic Heading", "after": "Fixed Specific Heading", "reason": "Too generic"}]
    }
    
    # Mock audit for fixed outline (no warnings)
    controller.validator.audit_heading_outline_quality.return_value = {"warnings": []}
    
    fix_result = await controller._run_controlled_heading_fix(state)
    assert fix_result["accepted"] is True, f"Expected acceptance, got rejection: {fix_result.get('reason')}"
    assert state["outline"][1]["heading_text"] == "Fixed Specific Heading"
    print("SUCCESS: Valid fix accepted")

    # CASE 2: Structural Change (Section ID) - Should be rejected
    state["outline"] = copy.deepcopy(original_outline) # Reset
    state["heading_quality_audit"] = {"warnings": [{"id": "GENERIC_H2", "severity": "medium"}]} # Reset audit
    broken_outline = copy.deepcopy(original_outline)
    broken_outline[1]["section_id"] = "changed_id"
    
    controller.outline_gen.fix_outline_headings.return_value = {"outline": broken_outline, "changes": [{"section_id": "changed_id"}]}
    
    fix_result = await controller._run_controlled_heading_fix(state)
    assert fix_result["accepted"] is False
    assert "Structural failure" in fix_result["reason"], f"Expected structural failure, got: {fix_result['reason']}"
    print("SUCCESS: Structural change (ID) rejected")

    # CASE 3: Increased Warnings - Should be rejected
    state["outline"] = copy.deepcopy(original_outline) # Reset
    state["heading_quality_audit"] = {"warnings": [{"id": "GENERIC_H2", "severity": "medium"}]} # Reset audit
    bad_fix_outline = copy.deepcopy(original_outline)
    bad_fix_outline[1]["heading_text"] = "Even Worse Heading"
    
    controller.outline_gen.fix_outline_headings.return_value = {"outline": bad_fix_outline, "changes": [{"section_id": "sec_2"}]}
    controller.validator.audit_heading_outline_quality.return_value = {"warnings": [{}, {}]} # 2 warnings instead of 1
    
    fix_result = await controller._run_controlled_heading_fix(state)
    assert fix_result["accepted"] is False
    assert "Quality failure: Warnings increased" in fix_result["reason"], f"Expected warnings increased failure, got: {fix_result['reason']}"
    print("SUCCESS: Increased warnings rejected")

    # CASE 4: New HIGH severity warning - Should be rejected
    state["outline"] = copy.deepcopy(original_outline) # Reset
    state["heading_quality_audit"] = {"warnings": [{"id": "GENERIC_H2", "severity": "medium"}]} # Reset audit
    
    high_warning_outline = copy.deepcopy(original_outline)
    high_warning_outline[1]["heading_text"] = "Heading with high warning"
    controller.outline_gen.fix_outline_headings.return_value = {"outline": high_warning_outline, "changes": [{"section_id": "sec_2"}]}
    
    # 1 high warning, same count as 1 medium warning
    controller.validator.audit_heading_outline_quality.return_value = {"warnings": [{"severity": "high"}]} 
    
    fix_result = await controller._run_controlled_heading_fix(state)
    assert fix_result["accepted"] is False
    assert "New high-severity warnings introduced" in fix_result["reason"], f"Expected high-severity failure, got: {fix_result['reason']}"
    print("SUCCESS: New high-severity warning rejected")

async def main():
    try:
        await test_heading_fix_acceptance_logic()
        print("\nAll heading fix layer tests passed!")
    except Exception as e:
        print(f"\nTEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())
