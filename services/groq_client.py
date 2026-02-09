from groq import Groq
import logging
from config.ai_config import GROQ

logger = logging.getLogger(__name__)

class GroqClient:
    def __init__(self):
        self.client = Groq(api_key=GROQ["api_key"])
        self.model = GROQ["default_model"]

        with open("prompts/system_persona.txt", "r", encoding="utf-8") as f:
            self.system_persona = f.read()

    async def send(self, prompt: str, step: str = "default") -> str:
        max_tokens = GROQ["max_tokens"].get(step, 700)

        messages = [
            {"role": "system", "content": self.system_persona},
            {"role": "user", "content": prompt}
        ]

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.7,
                max_tokens=max_tokens
            )
            return completion.choices[0].message.content.strip()

        except Exception as e:
            logger.error(f"Groq failed: {e}")
            return ""