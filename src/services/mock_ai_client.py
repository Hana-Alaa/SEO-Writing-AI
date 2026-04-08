import os
import time
import json
import logging
import asyncio
from typing import List, Dict, Optional, Any
from src.services.ai_client_base import BaseAIClient
from src.utils.observability import ObservabilityTracker

logger = logging.getLogger(__name__)

class MockAIClient(BaseAIClient):
    """
    A zero-cost AI client that returns hardcoded responses to test 
    the system's plumbing and logic without calling any API.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = "MOCK_KEY"
        self.model_writing = "mock-writing-model"
        self.model_research = "mock-research-model"
        self.observer = ObservabilityTracker()

    async def send(self, prompt: str, step: str = "default", max_tokens: Optional[int] = None) -> Dict[str, Any]:
        """Simulates AI sending with hardcoded logic based on prompt type."""
        start_time = time.time()
        
        # Determine brand_name from prompt if possible
        brand_name = None
        if "Brand Name: \"" in prompt:
            brand_name = prompt.split("Brand Name: \"")[1].split("\"")[0]
        
        # 1. Outline Generation Logic
        if "JSON" in prompt and ("outline" in prompt or "Structural Skeleton" in prompt):
            content = json.dumps({
                "outline": [
                    {
                        "section_id": "sec_01",
                        "heading_level": "H2",
                        "heading_text": "Mocked Introduction to SEO",
                        "section_type": "introduction",
                        "section_intent": "Informational",
                        "semantic_unit": "Intro",
                        "brand_mention_eligible": True,
                        "decision_layer": "Market Reality",
                        "sales_intensity": "low",
                        "content_goal": "Introduce the topic",
                        "content_angle": "Educational",
                        "localized_angle": "General",
                        "assigned_keywords": ["SEO basics", "content marketing"],
                        "cta_eligible": False,
                        "requires_primary_keyword": True
                    },
                    {
                        "section_id": "sec_02",
                        "heading_level": "H2",
                        "heading_text": "The Benefits of Mock Testing",
                        "section_type": "standard",
                        "section_intent": "Commercial",
                        "semantic_unit": "Testing",
                        "brand_mention_eligible": True,
                        "decision_layer": "Business Risk",
                        "sales_intensity": "medium",
                        "content_goal": "Showcase testing value",
                        "content_angle": "Strategic",
                        "localized_angle": "General",
                        "assigned_keywords": ["mocking", "cost-saving"],
                        "cta_eligible": True,
                        "requires_primary_keyword": False
                    },
                    {
                        "section_id": "sec_03",
                        "heading_level": "H2",
                        "heading_text": "Final Conclusion",
                        "section_type": "conclusion",
                        "section_intent": "Commercial",
                        "semantic_unit": "Summary",
                        "brand_mention_eligible": True,
                        "decision_layer": "Proof Layer",
                        "sales_intensity": "high",
                        "content_goal": "Encourage action",
                        "content_angle": "Conversion",
                        "localized_angle": "General",
                        "assigned_keywords": ["starting now"],
                        "cta_eligible": True,
                        "requires_primary_keyword": True
                    }
                ],
                "semantic_entities": ["Testing", "AI", "SEO"],
                "semantic_concepts": ["Automation", "Cost reduction"],
                "intent_clusters": ["Commercial", "Informational"],
                "keyword_expansion": { "primary": "SEO Testing", "core": [], "lsi": ["automated tests"], "semantic": [], "paa": [] }
            })
        
        # 2. Section Writing Logic
        elif "JSON" in prompt and "content" in prompt and "used_links" in prompt:
            heading = "this section"
            if "Heading \"" in prompt:
                heading = prompt.split("Heading \"")[1].split("\"")[0]
            
            is_arabic = "ar" in prompt.lower() or "Arabic" in prompt or any(ord(c) > 128 for c in heading)
            
            final_brand = brand_name or "عقار يا مصر" if is_arabic else "Aqar Ya Masr"

            if is_arabic:
                if "introduction" in prompt.lower():
                    content = (
                        f"الواقع يفرض نفسه بوضوح؛ الاعتماد على تقنياتنا المتطورة في {heading} ليس مجرد خيار، بل هو قرار استراتيجي يحسم كفاءة العمل. "
                        f"مع **{final_brand}**، نضمن لك تقليل التكاليف بنسبة تتجاوز 70% عبر المحاكاة الذكية، مما يضعك في صدارة المنافسين."
                    )
                elif "conclusion" in prompt.lower():
                    content = (
                        f"الحقيقة النهائية هي أن الاستثمار مع **{final_brand}** يمثل حجر الزاوية لنموك المستقبلي. "
                        f"احسم قرارك الآن مع **{final_brand}** لتأمين استدامة الجودة وحماية مواردك للأبد بنظام لا يقبل المنافسة."
                    )
                else:
                    # Dynamic Hybrid Content (Paragraph + Bullets)
                    content = (
                        f"التفوق التقني في {heading} يتطلب رؤية واضحة تقدمها **{final_brand}** لعملائها. "
                        "نحن نصيغ الواقع الاستثماري عبر بروتوكولات تمنع الأخطاء المكلفة، مما يوفر لك المزايا التالية:\n"
                        "- استقرار كامل في الأنظمة.\n"
                        "- تقليل الهدر البرمجي.\n"
                        "- سرعة فائقة في التنفيذ.\n"
                        f"هذا ما يجعل **{final_brand}** الخيار الأول للخبراء الحقيقيين."
                    )
            else:
                if "introduction" in prompt.lower():
                    content = (
                        f"The reality is clear: implementing our advanced methods within {heading} is a strategic decision that dictates efficiency. "
                        f"With **{final_brand}**, we ensure a cost reduction of over 70% through smart simulation, positioning you ahead."
                    )
                elif "conclusion" in prompt.lower():
                    content = (
                        f"The ultimate verdict is that investing with **{final_brand}** is the cornerstone of your future growth. "
                        f"Decide now with **{final_brand}** to secure your resource protection forever with an unbeatable system."
                    )
                else:
                    content = (
                        f"Technical superiority in {heading} requires the clear vision provided by **{final_brand}**. "
                        "We shape the investment reality via protocols that prevent costly errors, offering you these key benefits:\n"
                        "- Full system stability.\n"
                        "- Reduced engineering waste.\n"
                        "- High execution speed.\n"
                        f"This makes **{final_brand}** the top choice for market experts."
                    )

            content_json = json.dumps({
                "content": content,
                "used_links": ["https://example.com/mock-link"],
                "topics_covered": ["Simulation", "Efficiency", "AI testing", heading],
                "brand_link_used": True
            })
            content = content_json
            
        # 3. Metadata Logic (MetaSchemaGenerator)
        elif "MetaSchemaGenerator" in prompt or "meta" in prompt or "title" in prompt:
            content = json.dumps({
                "h1": "How to Save Money with AI Simulations: The Ultimate ROI Guide for 2026",
                "meta_title": "AI Testing Guide: Save Money with Advanced AI Simulations in 2026",
                "meta_description": "Discover how to leverage AI testing and simulations to drastically reduce API costs and improve SEO performance without wasting expensive tokens or time.",
                "meta_keywords": "AI testing, SEO, simulation, cost saving, automation",
                "article_schema": {
                    "@context": "https://schema.org",
                    "@type": "Article",
                    "headline": "How to Save Money with AI Simulations: The Ultimate ROI Guide for 2026"
                },
                "faq_schema": None
            })
            
        # 4. Default / Fallback
        else:
            content = "This is a default mocked response from MockAIClient."

        metadata = {
            "duration": time.time() - start_time,
            "model": self.model_writing,
            "prompt": prompt,
            "response": content,
            "tokens": {
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "total_tokens": 150
            }
        }
        
        return {"content": content, "metadata": metadata}

    async def send_with_web(self, prompt: str, max_results: int = 5) -> Dict[str, Any]:
        """Simulates Web Research with dummy insights."""
        content = json.dumps({
            "top_results": [{"title": "Mock Research result", "url": "http://mock.com", "snippet": "Useful fact about mocking."}],
            "intent": "informational",
            "strategic_intelligence": {
                "content_gaps": ["lack of testing"],
                "weaponized_usps": ["Zero cost simulations"],
                "battle_plan_brief": "Use mock clients effectively."
            }
        })
        return {"content": content, "metadata": {"tokens": {"total_tokens": 10}}}

    async def send_image(self, prompt: str, width=1024, height=1014, save_dir: str = None, seed: int = None, reference_path: str = None):
        """Returns a dummy image path."""
        target_dir = save_dir or "output/images"
        os.makedirs(target_dir, exist_ok=True)
        filename = os.path.join(target_dir, "mock_image.png")
        if not os.path.exists(filename):
            with open(filename, "wb") as f:
                f.write(b"MOCK_IMAGE_DATA")
        return filename

    async def describe_image_style(self, image_path: str) -> Dict[str, Any]:
        return {"content": "Minimalist mock style.", "metadata": {}}

    async def close(self):
        pass
