import logging
import re
from typing import Any, Dict, List, Optional
from datetime import datetime
from jinja2 import Template, StrictUndefined
from src.utils.safe_json import recover_json

logger = logging.getLogger(__name__)

class TitleGenerator:
    def __init__(self, ai_client: Any, template_path: str = "assets/prompts/templates/00_seo_intent_title.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    async def generate(
        self, 
        raw_title: str, 
        primary_keyword: str, 
        article_language: str,
        serp_titles: Optional[List[str]] = None,
        serp_cta_styles: Optional[List[str]] = None,
        area: Optional[str] = None
    ) -> Dict[str, Any]:

        current_year = str(datetime.now().year)
        
        # Pre-process raw title to inject current year if it has a placeholder year
        processed_raw_title = re.sub(r"\b(20\d{2})\b|\[year\]", current_year, raw_title, flags=re.IGNORECASE)

        prompt = self.template.render(
            raw_title=processed_raw_title,
            primary_keyword=primary_keyword,
            article_language=article_language,
            serp_titles=serp_titles or [],
            serp_cta_styles=serp_cta_styles or [],
            area=area,
            current_year=current_year
        )

        logger.info("\n==== FINAL PROMPT (TitleGenerator) ====\n")
        logger.info(prompt)
        logger.info("\n======================================\n")

        res = await self.ai_client.send(prompt, step="title")
        raw_response = res["content"]
        data = recover_json(raw_response) or {}
        
        title = data.get("optimized_title", processed_raw_title)
        intent = data.get("intent", "Informational")

        # Post-process to ensure the year is absolutely current
        if title:
            title = re.sub(r"\b(20\d{2})\b|\[year\]", current_year, title, flags=re.IGNORECASE)

        return {
            "optimized_title": title.strip(),
            "intent": intent.strip(),
            "metadata": res["metadata"],
            "prompt": prompt
        }
