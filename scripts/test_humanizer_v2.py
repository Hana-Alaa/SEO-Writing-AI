import asyncio
import os
import sys
import json
from dotenv import load_dotenv

# Add the project root to sys.path
sys.path.append(os.getcwd())

# Load environment variables
load_dotenv()

from src.services.content_generator import FinalHumanizer
from src.services.openrouter_client import OpenRouterClient

async def test_humanizer():
    # Initialize real client
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        print("Error: OPENROUTER_API_KEY environment variable not set in .env.")
        return

    client = OpenRouterClient(api_key=api_key)
    humanizer = FinalHumanizer(
        ai_client=client,
        template_path="assets/prompts/templates/05_final_humanizer.txt"
    )

    # Sample input: The introduction from the recent "Apartments for sale in Egypt" article
    # This was the section that likely caused the error or was too long.
    section_heading = "شقق للبيع في مصر 2026 | ابحث عن أرخص الأسعار عبر عقار يا مصر"
    section_content = """يواجه الباحثون عن **شقق للبيع في مصر** تحديات متزايدة في ظل الطفرة العمرانية الهائلة؛ حيث أصبح العثور على وحدة سكنية تجمع بين السعر العادل والموقع الاستراتيجي دون تدخل الوسطاء أمراً يتطلب بحثاً دقيقاً ووعياً عقارياً.

تتنوع الخيارات السكنية المتاحة حالياً لتشمل **شقق تشطيب سوبر لوكس** في قلب الأحياء التاريخية بالقاهرة، وصولاً إلى الوحدات العصرية داخل **كمبوندات العاصمة الإدارية** ودمياط الجديدة، مما يمنحك فرصاً استثمارية وسكنية تناسب كافة الميزانيات والاحتياجات العائلية.

يسلط هذا الدليل الضوء على كيفية اقتناص **عقارات من المالك مباشرة** لتجنب العمولات الإضافية، مع استعراض شامل لأحدث اتجاهات السوق المصري وأفضل المناطق التي تضمن لك أعلى قيمة مقابل سعر في عام 2026.

**للحصول على رؤية شاملة حول أفضل العروض المتاحة حالياً في السوق المصري، يمكنك [اكتشف المزيد](https://aqaryamasr.com/) عبر منصة عقار يا مصر.**"""

    print("--- [INPUT] Original Section ---")
    print(section_content)
    print("-" * 50)

    try:
        print("Executing FinalHumanizer.humanize_section...")
        # Note: We are simulating the "is_introduction=True" flag
        result = await humanizer.humanize_section(
            target_section_heading=section_heading,
            target_section_content=section_content,
            article_language="Arabic",
            brand_name="عقار يا مصر",
            brand_source_text="عقار يا مصر هو محرك بحث عقاري يربط المشتري بالبائع مباشرة دون وسيط. نوفر عقارات في كافة محافظات مصر بأسعار حقيقية.",
            weaponized_usps="من المالك مباشرة، بدون عمولات، خريطة تفاعلية، دليل أسعار برؤية 2026",
            is_introduction=True,
            full_article_context=section_content 
        )
        
        print("\n--- [OUTPUT] Humanized Result ---")
        print(result)
        print("-" * 50)
        
        # Check if it respected the 2-paragraph rule
        paragraphs = [p for p in result.split('\n\n') if p.strip()]
        # The last paragraph might be the CTA if separated by \n\n
        print(f"Number of paragraphs detected: {len(paragraphs)}")
        
        if len(paragraphs) <= 3: # 2 paragraphs + 1 CTA line
             print("SUCCESS: Result looks concise and adheres to constraints.")
        else:
             print("WARNING: Result might still be too long.")

    except Exception as e:
        print(f"\nCRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_humanizer())
