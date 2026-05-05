from jinja2 import Environment, FileSystemLoader
import json
import os

def check_template():
    env = Environment(loader=FileSystemLoader('assets/prompts/templates'))
    template = env.get_template('01d_heading_fix.txt')
    
    dummy_data = {
        "primary_keyword": "test",
        "entity_phrase": "test",
        "service_phrase": "test",
        "area": "test",
        "display_brand_name": "test",
        "content_type": "test",
        "content_strategy": {"key": "val"},
        "outline": [{"id": "1"}],
        "heading_quality_audit": {"warnings": []},
        "ai_outline_critique": {"score": 10}
    }
    
    try:
        rendered = template.render(**dummy_data)
        print("Template rendered successfully!")
        # print(rendered)
    except Exception as e:
        print(f"Template rendering failed: {e}")
        exit(1)

if __name__ == "__main__":
    check_template()
