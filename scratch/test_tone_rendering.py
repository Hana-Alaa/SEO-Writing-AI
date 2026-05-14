import asyncio
import os
from jinja2 import Environment, FileSystemLoader

async def test_tone_rendering():
    template_dir = os.path.abspath("assets/prompts/templates")
    env = Environment(loader=FileSystemLoader(template_dir))
    
    # Render base template
    template = env.get_template("02_section_writer_base.txt")
    rendered = template.render(
        section={},
        cognitive_blueprint={"section_thesis": "Test", "decision_logic": [], "evidence_plan": [], "reader_value": "V", "avoid_patterns": []}
    )
    
    # 1. New tone framings
    assert "CLEAR AND APPROACHABLE EXPERT" in rendered
    assert "Practical Guide for Beginners" in rendered
    assert "Light Professional Modern Standard Arabic (MSA)" in rendered
    
    # 2. Banned colloquial examples
    assert "AVOID: تقدر, لو حابب, عشان" in rendered
    assert "إزاي" in rendered
    
    # 3. Preferred replacements
    assert "PREFER: يمكنك, إذا كنت ترغب" in rendered
    assert "كيف" in rendered
    
    # 4. Banned corporate jargon
    assert "AVOID: استثمار استثنائي, حلول مبتكرة" in rendered
    
    # 5. Preferred tone examples
    assert "يساعدك على المقارنة بين الخيارات" in rendered
    assert "يناسب العائلات التي تبحث عن الهدوء" in rendered

    # Render commercial extension
    comm_template = env.get_template("02_section_writer_brand_commercial_v2.txt")
    comm_rendered = comm_template.render(
        section={},
        cognitive_blueprint={"section_thesis": "Test", "decision_logic": [], "evidence_plan": [], "reader_value": "V", "avoid_patterns": []},
        brand_name="TestBrand",
        primary_keyword="TestPK",
        global_keyword_count=0,
        brand_mentions_count=0,
        article_language="ar"
    )
    
    assert "Light Professional Modern Standard Arabic (MSA)" in comm_rendered
    
    # 6. Commercial Safety
    assert "missing out by not acting now" not in comm_rendered
    assert "take action NOW" not in comm_rendered
    assert "ابدأ استثمارك" not in comm_rendered
    assert "قارن الخيارات" in comm_rendered
    assert "TONE-SAFETY PRIORITY RULE" in comm_rendered
    assert "regionally familiar vocabulary" in comm_rendered
    assert "no dialect grammar" in comm_rendered

    # 7. Generic Safety Cleanup
    assert "home seeker" not in comm_rendered
    assert "practical decision-maker" in comm_rendered
    assert "<button>" not in comm_rendered.split("FORBIDDEN")[1].split("FAQ")[0] # Check it's not allowed in CTAs
    assert "العقارات المتاحة" not in comm_rendered
    assert "استلام فوري" not in comm_rendered
    assert "الخيارات المتاحة" in comm_rendered
    assert "العروض الحالية" in comm_rendered

    print("All Tone Rendering Tests PASSED!")

if __name__ == "__main__":
    asyncio.run(test_tone_rendering())
