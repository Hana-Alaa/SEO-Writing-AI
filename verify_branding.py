import asyncio
import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.append(os.getcwd())

from services.workflow_controller import AsyncWorkflowController
import logging

# Setup logging to be clean for the user
logging.basicConfig(level=logging.ERROR) # Only show errors for dependencies
logger = logging.getLogger("BrandingTest")
logger.setLevel(logging.INFO)

async def test_branding():
    print("="*60)
    print("SEO-Writing-AI: Branding & Image Pipeline Tester")
    print("="*60)
    
    brand_url = input("\n1. Enter Brand URL (e.g., https://webook.com): ").strip()
    if not brand_url:
        print("Error: Brand URL is required.")
        return

    brand_theme = input("2. Enter Brand Theme (Short keyword for the wave, e.g., 'Real Estate'): ").strip() or "Professional Business"
    topic = input("3. Enter Full Topic for Image (Long prompt is fine): ").strip() or "High-end corporate office meeting"
    
    print(f"\n[STATUS] Initializing AI Engines...")
    controller = AsyncWorkflowController(work_dir=".")
    
    # Setup a clean test state
    state = {
        "brand_url": brand_url,
        "primary_keyword": brand_theme, # Mandatory for Master Frame (Now short and clean)
        "input_data": {
            "title": f"Branding Test for {brand_url}",
            "primary_keyword": brand_theme,
            "keywords": [brand_theme],
            "article_language": "ar"
        },
        "output_dir": "output/test_branding"
    }

    # Ensure output directory exists
    os.makedirs(state["output_dir"], exist_ok=True)
    images_dir = os.path.join(state["output_dir"], "images")
    os.makedirs(images_dir, exist_ok=True)
    
    # Configure image client to save in our test dir
    controller.image_client.save_dir = images_dir

    print(f"\n[1/3] Searching for Brand Assets (Logo & Colors) on {brand_url}...")
    try:
        # Use the internal discovery logic
        brand_assets = await controller._discover_logo_and_colors(brand_url, state)
        
        if brand_assets:
            state.update(brand_assets)
            print(f"      ✅ Found Logo: {state.get('logo_path')}")
            print(f"      ✅ Extracted Colors: {state.get('brand_colors')}")
        else:
            print("      ❌ Failed to find a valid logo on the site.")
            return
    except Exception as e:
        print(f"      ❌ Brand Discovery Error: {e}")
        return

    print(f"\n[2/3] Asking AI to design your Premium 'Master Frame'...")
    try:
        # This calls the AI to generate a frame based on the colors
        updated_state = await controller._step_4_1_generate_master_frame(state)
        master_frame = updated_state.get("master_frame_path")
        if master_frame and os.path.exists(master_frame):
            print(f"      ✅ Master Frame Designed & Saved: {master_frame}")
        else:
            print("      ❌ Master Frame generation failed.")
            return
    except Exception as e:
        print(f"      ❌ Frame Generation Error: {e}")
        return

    print(f"\n[3/3] Generating a Branded Concept Image for '{topic}'...")
    # Clean, professional test prompt
    test_prompt = {
        "prompt": f"A professional, crisp high-resolution photo of {topic}, cinematic lighting, studio quality, centered with clear negative space, executive modern aesthetic",
        "type": "Featured",
        "alt_text": "Test Branded Image",
        "section_id": "test_section"
    }
    
    try:
        # Generate the image and apply the frame/logo
        images = await controller.image_client.generate_images(
            [test_prompt],
            primary_keyword=topic,
            image_frame_path=master_frame,
            logo_path=state.get("logo_path"),
            brand_visual_style=state.get("brand_visual_style"),
        )

        if images and "local_path" in images[0]:
            print(f"\n" + "!"*60)
            print(f"🏆 SUCCESS! YOUR BRANDED IMAGE IS READY")
            print(f"Location: {os.path.join(os.getcwd(), images[0]['local_path'])}")
            print("!"*60)
            print("\nOpening directory for you...")
            # Try to open the folder on Windows
            try:
                os.startfile(os.path.abspath(images_dir))
            except: pass
        else:
            print("      ❌ Image generation or branding failed.")
    except Exception as e:
        print(f"      ❌ Image Pipeline Error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(test_branding())
    except KeyboardInterrupt:
        print("\nTest cancelled.")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
