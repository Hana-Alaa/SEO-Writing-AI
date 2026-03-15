from jinja2 import Template, StrictUndefined
import logging
import json 

logger = logging.getLogger(__name__)

class ArticleValidator:
    def __init__(self, ai_client, template_path="assets/prompts/templates/08_article_validator.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    async def validate( self, final_markdown, meta, images, title, article_language, primary_keyword, word_count, keyword_count, keyword_density, content_strategy=None):
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}

        prompt = self.template.render(
            title=title,
            article_language=article_language,
            primary_keyword=primary_keyword,
            final_markdown=final_markdown,
            meta_title=meta.get("meta_title", ""),
            meta_description=meta.get("meta_description", ""),
            article_schema=meta.get("article_schema", {}),
            faq_schema=meta.get("faq_schema", {}),
            image_plan=images,
            word_count=word_count,
            keyword_count=keyword_count,
            keyword_density=keyword_density,
            content_strategy=content_strategy
        )   


        logger.info("\n================ FINAL PROMPT (ArticleValidator) ================\n")
        logger.info(prompt)
        logger.info("\n=============================================================\n")
        
        res = await self.ai_client.send(prompt, step="article_validation")
        return res["content"]