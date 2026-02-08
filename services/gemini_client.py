import os
from google import genai
import logging
from typing import Optional
import asyncio

logger = logging.getLogger(__name__)

class GeminiClient:
    """Wrapper for Google Gemini AI Studio (chat/completion)"""

    STEP_DEFAULT_TOKENS = {
        "outline": 800,
        "section": 1200,
        "image": 300,
        "assembly": 700,
        "default": 700
    }

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            logger.warning("GEMINI_API_KEY not found in environment variables.")
        self.client = genai.Client(api_key=self.api_key)

    async def send(self, prompt: str, step: str = "default") -> str:
        """
        Send a prompt and get the output text.
        """
        max_tokens = self.STEP_DEFAULT_TOKENS.get(step, 700)

        try:
            response = await asyncio.to_thread(
                self._generate_content_sync,
                prompt,
                max_tokens
            )
            return response
        except Exception as e:
            logger.error(f"GeminiClient error at step '{step}': {e}")
            return ""

    def _generate_content_sync(self, prompt: str, max_tokens: int) -> str:
        """
        Synchronous call to Gemini generate_content.
        """
        result = self.client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=prompt,
        )
        return result.text
