import asyncio
import json
import os
import sys

import logging
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "prompts.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.workflow_controller import AsyncWorkflowController

async def main():
    controller = AsyncWorkflowController(work_dir="output")
    
        # state = {
    #     "input_data": {
    #         "title": "أفضل الطرق لتعلم البرمجة بسرعة",
    #         "keywords": ["تعلم البرمجة", "بايثون", "تطوير الذات"],
    #         "urls": [
    #             {"text": "مقال عن بايثون", "link": "https://www.python.org/about/gettingstarted/"},
    #             {"text": "دورة تعلم البرمجة", "link": "https://www.codecademy.com/learn/learn-python-3"},
    #             {"text": "منصة تعليمية مجانية", "link": "https://www.freecodecamp.org/"},
    #             {"text": "مقالات تطوير الذات", "link": "https://www.coursera.org/browse/personal-development"},
    #         ]
    #     }
    # }

        # state = {
    #     "input_data": {
    #         "title": "مراجعة استضافة هوستنجر ",
            
    #         "keywords": [
    #             "استضافة هوستنجر", 
    #             "Hostinger Review", 
    #             "أرخص استضافة مواقع", 
    #             "سعر هوستنجر", 
    #             "عيوب هوستنجر",
    #             "إنشاء موقع ووردبريس"
    #         ],
            
    #         # "urls": [
    #         #     {"text": "عرض هوستنجر الخاص (خصم 85%)", "link": "https://www.hostinger.com/"},
    #         #     {"text": "أداة بناء المواقع بالذكاء الاصطناعي", "link": "https://www.hostinger.com/website-builder"},
    #         #     {"text": "مقارنة الخطط", "link": "https://www.hostinger.com/vps-hosting"}
    #         # ],
    #         "urls": [
    #             {"text": "الموقع الرسمي لـ Hostinger", "link": "https://www.hostinger.com/"}
    #         ]
    #     }
    # }


    state = {
        "input_data": {
            "title": "Web Design Agency in Riyadh - Custom Solutions",
            "keywords": [
                "Web Design Agency",
                "Web Design Riyadh",
                "Digital Marketing Agency Saudi Arabia",
                "UI/UX Design",
                "CEMS IT"
            ],
            "urls": [
                {"text": "CEMS IT Official Website", "link": "https://cems-it.com/"}
            ],
            "area": "Riyadh",
            "logo_path": "images/logo.png"
        },
    }

    try:
        final_result = await controller.run_workflow(state)
        
        final_markdown = final_result.get("final_markdown", "")

        output_dir = final_result.get("output_dir", "output")
        os.makedirs(output_dir, exist_ok=True)
        # md_file_path = os.path.join(output_dir, "final_article.md")

        md_file_path = os.path.join(output_dir, "final_article.md")
        with open(md_file_path, "w", encoding="utf-8") as f:
            f.write(final_markdown)
        print(f"✅ Markdown file saved at: {md_file_path}")

        # Prepare structured output
        output = {
            "title": final_result.get("title"),
            "slug": final_result.get("slug"),
            "images_count": len(final_result.get("images", [])),
            "meta_title": final_result.get("meta_title"),
            "meta_description": final_result.get("meta_description"),
            "seo_report_keys": list(final_result.get("seo_report", {}).keys()),
            "output_dir": final_result.get("output_dir")
        }

        # Print raw content sections
        # sections = final_result["workflow_state"]["sections"]
        # for sec_id, sec in sections.items():
            # print(f"\n--- Section {sec_id}: {sec.get('heading', sec_id)} ---")
            # print(sec.get("generated_content", "No content generated"))
        
        # print("\n=== FINAL MARKDOWN ===\n")
        # print(final_markdown)

        # Output JSON
        print(json.dumps(output, ensure_ascii=False, indent=2))
        
        from utils.html_renderer import render_html_page

        if final_result.get("meta_title"):
            final_result["title"] = final_result["meta_title"]
        

        html_path = render_html_page(final_result)
        print("HTML page generated at:", html_path)

        import markdown

        md_text = final_result.get("final_markdown", "")
        html_body = markdown.markdown(md_text, extensions=['tables', 'fenced_code'])
        
    except Exception as e:
        error_output = {
            "error": str(e),
            "status": "failed"
        }
        print(json.dumps(error_output, indent=2))
        

if __name__ == "__main__":
    asyncio.run(main())
