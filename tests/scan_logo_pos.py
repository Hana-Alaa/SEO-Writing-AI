
import os
import sys
import logging
from PIL import Image

# Add root directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def analyze_logo_placement():
    template_path = "e:/SEO-Writing-AI/output/images/reference.png"
    if not os.path.exists(template_path):
        print(f"Template not found: {template_path}")
        return

    with Image.open(template_path) as img:
        img = img.convert("RGBA")
        w, h = img.size
        print(f"Template size: {w}x{h}")
        
        # Scan 4 corners and center top
        regions = {
            "Top Left": (0, 0, int(w*0.3), int(h*0.3)),
            "Top Right": (int(w*0.7), 0, w, int(h*0.3)),
            "Bottom Left": (0, int(h*0.7), int(w*0.3), h),
            "Bottom Right": (int(w*0.7), int(h*0.7), w, h),
            "Top Center": (int(w*0.3), 0, int(w*0.7), int(h*0.2))
        }

        for name, box in regions.items():
            print(f"\nScanning {name}:")
            x1, y1, x2, y2 = box
            for y in range(y1, y2, (y2-y1)//10):
                row_str = ""
                for x in range(x1, x2, (x2-x1)//10):
                    p = img.getpixel((x, y))
                    # Not white (255) and not transparent (0)
                    if p[0] < 240 and p[3] > 10:
                        row_str += "X"
                    else:
                        row_str += "."
                print(row_str)

if __name__ == "__main__":
    analyze_logo_placement()
