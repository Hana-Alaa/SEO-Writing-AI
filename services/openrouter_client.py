import os
import asyncio
import httpx
import logging
from typing import List, Dict, Optional
from config.ai_config import OPENROUTER
from services.ai_client_base import BaseAIClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OpenRouterClient(BaseAIClient):
    """
    Client for interacting with the OpenRouter API with built-in retry logic.
    """
    STEP_TOKEN_LIMITS = {
        "outline": 1000,
        "section": 1500,
        "image": 300,
        "assembly": 800,
        "default": 800
    }
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the client.
        :param api_key: OpenRouter API key.
        """
        self.api_key = api_key or OPENROUTER["api_key"]
        self.model = OPENROUTER["default_model"]
        self.base_url = OPENROUTER["base_url"]
        if not self.api_key:
            logger.warning("OPENROUTER_API_KEY is missing")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "HTTP-Referer": OPENROUTER["site_url"],
            "X-Title": OPENROUTER["site_name"],
            "Content-Type": "application/json"
        }

    async def send(self, prompt: str, step: str = "default") -> str:
        """
        Simple shim to send a single prompt as a user message.
        """
        messages = [{"role": "user", "content": prompt}]
        response = await self.generate_completion(messages, step=step)
        return response if response else ""

    async def generate_completion(
        self, 
        messages: List[Dict[str, str]],
        step: str = "default",
        temperature: float = 0.7,
        retries: int = 3,
        response_format: Optional[Dict[str, str]] = None
        ) -> Optional[str]:

        max_tokens = self.STEP_TOKEN_LIMITS.get(step, 800)

        payload = {
        "model": self.model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens
        }
        if response_format:
            payload["response_format"] = response_format

        async with httpx.AsyncClient(timeout=120.0) as client:
            for attempt in range(retries):
                try:
                    response = await client.post(
                        self.base_url,
                        headers=self.headers,
                        json=payload
                    )
                    response.raise_for_status()
                    return response.json()["choices"][0]["message"]["content"]

                except Exception as e:
                    if 'response' in locals():
                        logger.error(f"OpenRouter failed (attempt {attempt+1}): {e} | Status: {response.status_code} | Body: {response.text}")
                    else:
                        logger.error(f"OpenRouter failed (attempt {attempt+1}): {e}")
                    
                    if attempt < retries - 1:
                        await asyncio.sleep(2 ** attempt)

        return None
