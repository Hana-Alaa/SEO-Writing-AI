import os
import sys
import asyncio
from PIL import Image
import logging

# Add current directory to path
sys.path.append(os.getcwd())

from src.services.image_generator import ImageGenerator

# Setup clean logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("ImageTester")

async def run_test():
    print("="*60)
    print("🚀 SEO-Writing-AI: Image Branding Pipeline Tester")
    print("="*60)
    print("This script tests the ADAPTIVE BRANDING and SOFT-MASK quality.")
    
    # Default paths from recent successful run for convenience
    default_img = r"output\حجز-تذاكر-المباريات_20260310_144632\images\1773147100818.webp"
    default_logo = r"output\حجز-تذاكر-المباريات_20260311_164757\images\brand_logo_7bbd7b48.png"
    default_template = r"output\حجز-تذاكر-المباريات_20260311_164757\images\master_brand_template.png"

    img_path = input(f"\n1. Image to process (default: {default_img}): ").strip() or default_img
    logo_path = input(f"2. Logo path (default: {default_logo}): ").strip() or default_logo
    template_path = input(f"3. Template/Wave background (default: {default_template}): ").strip() or default_template
    
    output_dir = "output/debug_test"
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize Generator (dummy AI client as we don't need it for local processing)
    gen = ImageGenerator(ai_client=None, save_dir=output_dir)
    
    try:
        print(f"\n[STEP 1] Testing 'create_branded_template' (Quality Polish)...")
        # This tests the soft-mask alpha keying
        final_template_path = os.path.join(output_dir, "functional_template.png")
        result_template = gen.create_branded_template(template_path, logo_path, final_template_path)
        
        if result_template:
            print(f"      ✅ Success! Branded template created at: {result_template}")
        else:
            print("      ❌ Failed to create branded template.")
            return

        print(f"\n[STEP 2] Testing 'Adaptive Branding' (Content Detection)...")
        # Load the base image
        with Image.open(img_path) as base_img:
            # We'll test both Overlay and Fallback modes
            print(f"      - Processing Image: {img_path} ({base_img.size})")
            
            # Use the newly created template
            final_img = gen._composite_with_template(base_img, result_template, logo_path=logo_path)
            
            final_save_path = os.path.join(output_dir, "final_branded_image.webp")
            final_img.convert("RGB").save(final_save_path, "WEBP", quality=90)
            
            print(f"\n" + "!"*60)
            print(f"🏆 TEST COMPLETE! Check the results in: {output_dir}")
            print(f"Final Image: {os.path.abspath(final_save_path)}")
            print("!"*60)
            
            # Try to open output folder on Windows
            try:
                os.startfile(os.path.abspath(output_dir))
            except: pass

    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_test())
