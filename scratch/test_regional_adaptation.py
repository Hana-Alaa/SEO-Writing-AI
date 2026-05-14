import asyncio
import os
from jinja2 import Environment, FileSystemLoader
from src.services.content_generator import SectionWriter
from unittest.mock import MagicMock

async def test_regional_adaptation():
    template_dir = os.path.abspath("assets/prompts/templates")
    env = Environment(loader=FileSystemLoader(template_dir))
    
    # Mock deps
    ai_client = MagicMock()
    
    # Test Data
    global_keywords = {"primary": "عقارات", "lsi": [], "semantic": []}
    section = {
        "heading_text": "Section Test",
        "primary_keyword": "عقارات",
        "execution_mode": "taxonomy_breakdown",
        "section_contract": {"location_policy": "local_required"}
    }
    
    # Cases to test
    cases = [
        {"area": "مصر، القاهرة", "expected_key": "مواصلات", "region": "Egypt"},
        {"area": "السعودية، الرياض", "expected_key": "عوائل", "region": "Saudi"},
        {"area": "دبي، الامارات", "expected_key": "مجمعات سكنية", "region": "UAE"},
    ]
    
    for case in cases:
        # We need to simulate the injection logic
        # Instead of calling write() which calls the LLM, we'll test the payload construction
        # Or we can just test the template rendering with the logic we expect
        
        from src.services.strategy_service import REGIONAL_ARABIC_PROFILES
        
        # Manually reproduce the detection logic for verification
        area_norm = case["area"].lower()
        regional_profile = ""
        if any(kw in area_norm for kw in ["مصر", "egypt", "cairo", "القاهرة"]):
            regional_profile = REGIONAL_ARABIC_PROFILES["egypt"]
        elif any(kw in area_norm for kw in ["السعودية", "saudi", "riyadh", "الرياض"]):
            regional_profile = REGIONAL_ARABIC_PROFILES["saudi"]
        elif any(kw in area_norm for kw in ["الامارات", "uae", "dubai", "دبي"]):
            regional_profile = REGIONAL_ARABIC_PROFILES["uae"]
            
        # Render
        template = env.get_template("02_section_writer_base.txt")
        rendered = template.render(
            section=section,
            regional_profile=regional_profile,
            cognitive_blueprint={"section_thesis": "Test", "decision_logic": [], "evidence_plan": [], "reader_value": "V", "avoid_patterns": []},
            global_keywords=global_keywords,
            primary_keyword="عقارات",
            article_language="ar"
        )
        
        # Assertions
        assert case["expected_key"] in rendered, f"Failed for {case['region']}: expected {case['expected_key']} in prompt"
        assert "REGIONAL ADAPTATION" in rendered
        
        # Ensure no dialect prose rules were violated (e.g. they should be in the AVOID section)
        if case["region"] == "Egypt":
            assert "هتلاقي" in rendered # In AVOID
        if case["region"] == "Saudi":
            assert "بتلقى" in rendered # In AVOID

    print("All Regional Adaptation Rendering Tests PASSED!")

if __name__ == "__main__":
    asyncio.run(test_regional_adaptation())
