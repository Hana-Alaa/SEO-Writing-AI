import logging
from typing import Any, Dict, List
from jinja2 import Template, StrictUndefined

logger = logging.getLogger(__name__)

class TitleGenerator:
    def __init__(self, ai_client: Any, template_path: str = "prompts/templates/00_title_generator.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    async def generate(self, raw_title: str, primary_keyword: str, intent: str, article_language: str) -> str:

        prompt = self.template.render(
            raw_title=raw_title,
            primary_keyword=primary_keyword,
            intent=intent,
            article_language=article_language
        )

        logger.info("\n==== FINAL PROMPT (TitleGenerator) ====\n")
        logger.info(prompt)
        logger.info("\n======================================\n")

        title = await self.ai_client.send(prompt, step="title")
        return (title or raw_title).strip()
    