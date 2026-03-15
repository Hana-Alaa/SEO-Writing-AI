from jinja2 import Template, StrictUndefined
import logging
logger = logging.getLogger(__name__)
from src.utils.safe_json import recover_json
from src.services.content_generator import ContentGeneratorError

class SectionValidator:
    def __init__(self, ai_client, template_path="assets/prompts/templates/03_section_validator.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    async def validate(self, title, article_language, section, content):
        prompt = self.template.render(
            title=title,
            article_language=article_language,
            section=section,
            generated_section_content=content
        )

        logger.info("\n================ FINAL PROMPT (SectionValidator) ================\n")
        logger.info(prompt)
        logger.info("\n=============================================================\n")

        response = await self.ai_client.send(prompt, step="section_validation")

        data = recover_json(response["content"]) if isinstance(response, dict) else recover_json(str(response))

        if not data or "status" not in data:
            raise ContentGeneratorError("Invalid section validation response.")

        if data["status"] not in ["PASS", "FAIL"]:
            raise ContentGeneratorError("Section validation returned invalid status.")

        return data
