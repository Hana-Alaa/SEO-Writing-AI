import asyncio
import copy
from unittest.mock import AsyncMock, MagicMock
from src.services.workflow_controller import AsyncWorkflowController

async def test_heading_fix_tightening_logic():
    """Verify the tightening and protection logic of the heading fix layer."""
    print("Running test_heading_fix_tightening_logic...")
    
    controller = AsyncWorkflowController(work_dir="temp")
    controller.outline_gen = AsyncMock()
    controller.validator = MagicMock()
    
    # Original outline
    original_outline = [
        {"section_id": "sec_01", "heading_text": "Intro", "section_type": "introduction", "heading_level": "INTRO"},
        {"section_id": "sec_02", "heading_text": "Offer", "section_type": "offer", "heading_level": "H2"},
        {"section_id": "sec_03", "heading_text": "Good Section", "section_type": "core", "heading_level": "H2"},
        {"section_id": "sec_04", "heading_text": "Conclusion", "section_type": "conclusion", "heading_level": "H2"}
    ]
    
    # Audit: Only sec_02 has a warning. sec_01 has a LOW warning.
    audit = {
        "warnings": [
            {"section_id": "sec_01", "severity": "low", "id": "MINOR"},
            {"section_id": "sec_02", "severity": "medium", "id": "GENERIC"}
        ]
    }
    # Critique now uses the real dictionary schema
    critique = {
        "weak_sections": [{"section_id": "sec_02"}], 
        "h3_issues": [], 
        "overall_score": 8.0
    }
    
    state = {
        "heading_only_mode": True,
        "outline": copy.deepcopy(original_outline),
        "primary_keyword": "test",
        "heading_quality_audit": audit,
        "ai_outline_critique": critique,
        "content_strategy": {}
    }
    
    # AI proposes changes to EVERYTHING
    fixed_candidate = [
        {"section_id": "sec_01", "heading_text": "Fixed Intro", "section_type": "introduction", "heading_level": "INTRO"},
        {"section_id": "sec_02", "heading_text": "Fixed Offer", "section_type": "offer", "heading_level": "H2"},
        {"section_id": "sec_03", "heading_text": "Fixed Good Section", "section_type": "core", "heading_level": "H2"},
        {"section_id": "sec_04", "heading_text": "Fixed Conclusion", "section_type": "conclusion", "heading_level": "H2"}
    ]
    
    controller.outline_gen.fix_outline_headings.return_value = {
        "outline": fixed_candidate,
        "changes": [
            {"section_id": "sec_01", "field": "heading_text", "before": "Intro", "after": "Fixed Intro"},
            {"section_id": "sec_02", "field": "heading_text", "before": "Offer", "after": "Fixed Offer"},
            {"section_id": "sec_03", "field": "heading_text", "before": "Good Section", "after": "Fixed Good Section"},
            {"section_id": "sec_04", "field": "heading_text", "before": "Conclusion", "after": "Fixed Conclusion"}
        ]
    }
    
    # Mock audit for the result (passes quality check)
    controller.validator.audit_heading_outline_quality.return_value = {"warnings": []}
    
    fix_result = await controller._run_controlled_heading_fix(state)
    
    assert fix_result["accepted"] is True, f"Fix rejected: {fix_result.get('reason')}"
    final_outline = state["outline"]
    
    # Check Protections:
    # 1. Intro should be REVERTED (original "Intro") because warning was "low"
    assert final_outline[0]["heading_text"] == "Intro"
    print("SUCCESS: Intro protection (low severity) verified")
    
    # 2. Offer should be FIXED ("Fixed Offer") because it had a medium warning
    assert final_outline[1]["heading_text"] == "Fixed Offer"
    print("SUCCESS: Target section fix verified")
    
    # 3. Good Section should be REVERTED because it had no warnings/issues
    assert final_outline[2]["heading_text"] == "Good Section"
    print("SUCCESS: Unflagged section protection verified")
    
    # 4. Conclusion should be REVERTED always
    assert final_outline[3]["heading_text"] == "Conclusion"
    print("SUCCESS: Conclusion protection verified")

if __name__ == "__main__":
    asyncio.run(test_heading_fix_tightening_logic())
    print("\nAll tightening tests passed!")
