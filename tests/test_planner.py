import asyncio
import os
import sys
import logging

# Set up simple logging
logging.basicConfig(level=logging.DEBUG)

# Add the project scope
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.services.image_generator import ImagePromptPlanner
from src.services.openrouter_client import OpenRouterClient

async def main():
    client = OpenRouterClient()
    planner = ImagePromptPlanner(
        ai_client=client,
        template_path="assets/prompts/templates/06_image_planner.txt"
    )
    
    print("Testing image planner generation...")
    outline = [
        {"section_id": "sec_01", "title": "Introduction"}, 
        {"section_id": "sec_02", "title": "Core Features"}
    ]
    
    try:
        image_prompts = await planner.generate(
            title="Web Design Agency in Riyadh",
            primary_keyword="web design riyadh",
            keywords=["seo", "ui/ux"],
            outline=outline,
            brand_visual_style="Minimalist and modern"
        )
        print(f"Result count: {len(image_prompts)}")
        print(f"Prompts: {image_prompts}")
    except Exception as e:
        print(f"Exception caught: {e}")

if __name__ == "__main__":
    asyncio.run(main())
