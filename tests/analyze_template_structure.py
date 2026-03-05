
import os
import sys
from PIL import Image

# Add root directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def analyze_split_template():
    template_path = "e:/SEO-Writing-AI/output/images/reference.png"
    if not os.path.exists(template_path):
        template_path = "e:/SEO-Writing-AI/output/images/reference.webp"
        
    if not os.path.exists(template_path):
        print(f"Template not found.")
        return

    print(f"Analyzing structure of: {template_path}")
    with Image.open(template_path) as img:
        img = img.convert("RGBA")
        w, h = img.size
        print(f"Dimensions: {w}x{h}")
        
        # Scan row by row for color variance and transparency
        row_stats = []
        for y in range(h):
            row_colors = [img.getpixel((x, y)) for x in range(0, w, 10)]
            # Average variance of R, G, B in this row
            mean_r = sum(p[0] for p in row_colors) / len(row_colors)
            var_r = sum((p[0] - mean_r)**2 for p in row_colors) / len(row_colors)
            
            # Check for alpha
            has_alpha = any(p[3] < 255 for p in row_colors)
            
            row_stats.append((y, var_r, has_alpha))
            
        print("\nRow Analysis (y, variance, has_alpha) - showing samples:")
        for i in range(0, h, h//20):
            print(row_stats[i])
            
        # Look for the transition from high variance (image) to low variance (solid frame)
        # or vice versa
        print("\nPossible split points (sharp variance drop):")
        for i in range(1, h):
            v1 = row_stats[i-1][1]
            v2 = row_stats[i][1]
            if v1 > 500 and v2 < 100:
                print(f"Potential split at y={i} (Image -> Frame)")
            elif v1 < 100 and v2 > 500:
                print(f"Potential split at y={i} (Frame -> Image)")

if __name__ == "__main__":
    analyze_split_template()
