import asyncio
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.services.image_generator import ImageGenerator

async def test_images():
    generator = ImageGenerator(save_dir="output/images")

    image_prompts = [
        {
            "section_id": "test_section",
            "prompt": "A modern workspace with a laptop showing Python code, clean professional style",
            "alt_text": "تعلم البرمجة باستخدام Python",
            "image_type": "Featured Image"
        }
    ]

    results = await generator.generate_images(
        image_prompts=image_prompts,
        primary_keyword="تعلم البرمجة"
    )

    print(results)

if __name__ == "__main__":
    asyncio.run(test_images())
