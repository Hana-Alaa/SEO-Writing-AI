from groq import Groq
import logging
from typing import List, Dict, Optional
from config.ai_config import GROQ

logger = logging.getLogger(__name__)

class GroqClient:
    def __init__(self):
        self.client = Groq(api_key=GROQ["api_key"])
        self.model = GROQ["default_model"]

    async def send(self, prompt: str, step: str = "default") -> str:
        max_tokens = GROQ["max_tokens"].get(step, 700)

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=max_tokens
            )
            return completion.choices[0].message.content
        except Exception as e:
            logger.error(f"Groq failed: {e}")
            return ""
