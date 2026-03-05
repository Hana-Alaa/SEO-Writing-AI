
import os
import sys
from PIL import Image

# Add root directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def validate():
    res_path = "e:/SEO-Writing-AI/output/test_composited_result.png"
    if not os.path.exists(res_path):
        print(f"Result missing: {res_path}")
        return

    with Image.open(res_path) as img:
        img = img.convert("RGB")
        w, h = img.size
        print(f"Image size: {w}x{h}")
        
        # Check center pixel
        center_color = img.getpixel((w//2, h//2))
        print(f"Center color: {center_color}")
        
        # If center is white (255,255,255), it failed
        if center_color == (255, 255, 255):
            print("VALIDATION: FAIL - Center is still pure white. AI image not showing through.")
        else:
            print("VALIDATION: SUCCESS - Center is not white. AI image is likely visible.")

if __name__ == "__main__":
    validate()
