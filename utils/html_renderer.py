import os
import markdown
from jinja2 import Environment, FileSystemLoader

import logging

logger = logging.getLogger(__name__)

def render_html_page(final_result: dict):
    output_dir = final_result["output_dir"]
    os.makedirs(output_dir, exist_ok=True)
    
    md_content = final_result.get("final_markdown", "")
    logger.info(f"HTML Renderer received markdown length: {len(md_content)}")
    
    if not md_content:
        logger.warning("HTML Renderer received EMPTY markdown content!")
        
    # Remove the first H1 (# Title) line if present, as it is already rendered in the HTML template header
    lines = md_content.lstrip().splitlines()
    if lines and lines[0].startswith("# "):
        logger.info(f"Stripping H1 title from markdown: {lines[0]}")
        md_content = "\n".join(lines[1:])


    # 1. Convert Markdown → HTML
    try:
        html_content = markdown.markdown(
            md_content,
            extensions=[
                "fenced_code",     
                "tables",          
                "toc",              
                "attr_list",       
                "smarty",          
                "md_in_html",       
                "footnotes",       
            ],
            output_format="html5"
        )
        logger.info(f"Converted HTML content length: {len(html_content)}")
    except Exception as e:
        logger.error(f"Markdown conversion failed: {e}")
        html_content = f"<p>Error converting markdown: {e}</p>"

    # 2. Load HTML template
    try:
        env = Environment(
            loader=FileSystemLoader("templates"),
            autoescape=True
        )
        template = env.get_template("article.html")
    except Exception as e:
         logger.error(f"Template loading failed: {e}")
         return None

    # Use meta_title preferentially, fallback to title
    page_title = final_result.get("meta_title") or final_result.get("title", "Untitled")

    # 3. Render final HTML
    # Robust Detection: Check if title contains Arabic characters
    import re
    has_arabic = bool(re.search(r'[\u0600-\u06FF]', page_title))
    
    direction = "rtl" if has_arabic else "ltr"
    
    # Dynamic Copyright based on direction/language
    if direction == "rtl":
        copyright_text = "© 2026 جميع الحقوق محفوظة"
    else:
        copyright_text = "© 2026 All Rights Reserved"

    try:
        html = template.render(
            meta_title=page_title,
            meta_description=final_result.get("meta_description", ""),
            content=html_content,
            lang=final_result.get("article_language", "ar"),
            dir=direction,
            copyright_text=copyright_text
        )
        logger.info(f"Final rendered HTML length: {len(html)}")
    except Exception as e:
        logger.error(f"Template rendering failed: {e}")
        return None

    # 4. Save file
    html_path = os.path.join(output_dir, "page.html")
    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info(f"HTML saved to: {html_path}")
    except Exception as e:
        logger.error(f"File saving failed: {e}")

    return html_path