
import os
import sys
import logging
from PIL import Image

# Add root directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.image_generator import ImageGenerator

logging.basicConfig(level=logging.INFO)

async def test_frame_only():
    # Setup paths
    frame_path = "e:/SEO-Writing-AI/output/images/reference.png"
    base_image_path = "e:/SEO-Writing-AI/output/images/1772545866705.webp"
    output_path = "e:/SEO-Writing-AI/output/test_frame_final.png"

    print(f"Testing Simplified Frame Branding:\nFrame: {frame_path}")

    # Initialize Generator
    gen = ImageGenerator(ai_client=None)
    
    # Run the simplified frame processing
    processed_path = gen._process_image_versions(
        base_image_path, 
        image_frame_path=frame_path,
        apply_brand=True
    )
    
    import shutil
    shutil.copy(processed_path, output_path)
    print(f"SUCCESS: Result saved to {output_path}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_frame_only())
