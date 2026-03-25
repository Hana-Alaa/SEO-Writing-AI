from jinja2 import Template, StrictUndefined
import logging
logger = logging.getLogger(__name__)

class MetaSchemaGenerator:
    def __init__(self, ai_client, template_path="assets/prompts/templates/05_meta_schema_generator.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    async def generate(self, final_markdown, primary_keyword, intent, article_language, state, 
                       secondary_keywords=None, include_meta_keywords=False, article_url=None,
                       publish_date=None, modified_date=None, images=None, word_count=0):

        prompt = self.template.render(
            final_markdown=final_markdown,
            primary_keyword=primary_keyword,
            intent=intent,
            article_language=article_language,
            secondary_keywords=secondary_keywords,
            include_meta_keywords=include_meta_keywords,
            article_url=article_url,
            publish_date=publish_date,
            modified_date=modified_date,
            images=images,
            word_count=word_count,
            brand_logo_url=state.get("logo_path") if isinstance(state, dict) else None,
            area=state.get("area") if isinstance(state, dict) else None,
            brand_name=state.get("brand_name") if isinstance(state, dict) else ""
        )

        logger.info("\n================ FINAL PROMPT (MetaSchemaGenerator) ================\n")
        logger.info(prompt)
        logger.info("\n=============================================================\n")

        res = await self.ai_client.send(prompt, step="meta_schema")
        return res["content"]