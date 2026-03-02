import sys
import os
import json

# Add current directory to path
sys.path.append(os.getcwd())

from utils.html_renderer import render_html_page

def main():
    md_path = r"e:\SEO-Writing-AI\output\web-design-agency_20260302_165551\final_article.md"
    output_dir = r"e:\SEO-Writing-AI\output\web-design-agency_20260302_165551"
    
    with open(md_path, "r", encoding="utf-8") as f:
        md_content = f.read()
    
    # Mock final_result
    final_result = {
        "output_dir": output_dir,
        "final_markdown": md_content,
        "title": "Top Web Design Agency in Riyadh: Expert Solutions for 2026",
        "article_language": "ar"
    }
    
    html_path = render_html_page(final_result)
    print(f"HTML re-rendered at: {html_path}")

if __name__ == "__main__":
    main()
