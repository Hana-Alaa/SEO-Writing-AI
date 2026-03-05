
import os
import sys
from PIL import Image

# Add root directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def analyze_template():
    template_path = "e:/SEO-Writing-AI/output/images/reference.webp"
    if not os.path.exists(template_path):
        print(f"Template not found: {template_path}")
        return

    with Image.open(template_path) as img:
        img = img.convert("RGBA")
        w, h = img.size
        print(f"Template size: {w}x{h}")
        
        # Check center pixel
        center_pixel = img.getpixel((w//2, h//2))
        print(f"Center pixel (RGBA): {center_pixel}")
        
        # Check a few random spots
        spots = [(10, 10), (w-10, 10), (10, h-10), (w-10, h-10)]
        for s in spots:
            print(f"Spot {s} (RGBA): {img.getpixel(s)}")

        # Print some grayscale values for the thresholding logic
        gray = img.convert("L")
        print(f"Center pixel (Grayscale): {gray.getpixel((w//2, h//2))}")

if __name__ == "__main__":
    analyze_template()
