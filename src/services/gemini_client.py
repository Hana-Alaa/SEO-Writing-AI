import os
from google import genai
import logging
from typing import Optional
import asyncio

logger = logging.getLogger(__name__)

class GeminiClient:
    """Stable Gemini client with system persona support"""

    STEP_DEFAULT_TOKENS = {
        "outline": 800,
        "section": 1200,
        "image": 300,
        "assembly": 700,
        "default": 700
    }

    def __init__(self, api_key: Optional[str] = None, system_persona_file: str = "assets/prompts/system_persona.txt"):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            logger.warning("GEMINI_API_KEY not found in environment variables.")

        self.client = genai.Client(api_key=self.api_key)

        if not os.path.exists(system_persona_file):
            raise FileNotFoundError(f"System persona file not found: {system_persona_file}")

        with open(system_persona_file, "r", encoding="utf-8") as f:
            self.system_persona = f.read()

    async def send(self, prompt: str, step: str = "default") -> str:
        max_tokens = self.STEP_DEFAULT_TOKENS.get(step, 700)

        try:
            return await asyncio.to_thread(
                self._generate_content_sync,
                prompt,
                max_tokens
            )
        except Exception as e:
            logger.error(f"GeminiClient error at step '{step}': {e}")
            return ""

    def _generate_content_sync(self, prompt: str, max_tokens: int) -> str:
        full_prompt = f"""{self.system_persona}

=====================
USER TASK:
{prompt}
"""

        result = self.client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=full_prompt
        )

        return result.text
