import os
import time
import httpx
import base64
import logging
import asyncio
from pathlib import Path
from typing import List, Dict, Optional
from config.ai_config import OPENROUTER
from services.ai_client_base import BaseAIClient
from utils.observability import ObservabilityTracker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)

class OpenRouterClient(BaseAIClient):
    """
    Client for interacting with the OpenRouter API with built-in retry,
    concurrency limiting, and rate-limit handling.
    """

    # GLOBAL limiter for all instances
    _semaphore = asyncio.Semaphore(3)  # max concurrent requests

    def __init__(self, api_key: Optional[str] = None):
        self.observer = ObservabilityTracker()
        self.api_key = api_key or OPENROUTER["api_key"]
        # self.model = OPENROUTER["default_model"]
        self.model_writing = OPENROUTER["models"]["writing"]
        self.model_research = OPENROUTER["models"]["research"]
        # self.base_url = OPENROUTER["base_url"]
        self.base_url_chat = OPENROUTER["base_url_chat"]
        self.base_url_responses = OPENROUTER["base_url_responses"]

        if not self.api_key:
            logger.warning("OPENROUTER_API_KEY is missing")

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "HTTP-Referer": OPENROUTER["site_url"],
            "X-Title": OPENROUTER["site_name"],
            "Content-Type": "application/json"
        }

    @staticmethod
    def load_prompt(path: str) -> str:
        try:
            return Path(path).read_text(encoding="utf-8")
        except Exception as e:
            logger.error(f"Failed to load prompt from {path}: {e}")
            return ""

    async def send(self, prompt: str, step: str = "default") -> str:
        system_prompt = self.load_prompt("prompts/system_persona.txt")

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]

        payload = {
            "model": self.model_writing,
            "messages": messages,
            "temperature": 0.7
        }

        start_time = time.time()

        async with self._semaphore:
            async with httpx.AsyncClient(timeout=25.0) as client:
                r = await client.post(
                    self.base_url_chat,
                    headers=self.headers,
                    json=payload
                )
                r.raise_for_status()
                data = r.json()

        end_time = time.time()

        # --- Extract response text ---
        content = data["choices"][0]["message"]["content"]

        # --- Extract usage if available ---
        usage = data.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens")
        completion_tokens = usage.get("completion_tokens")

        if prompt_tokens is None or completion_tokens is None:
            # fallback rough estimation
            prompt_tokens = int(len(prompt.split()) * 1.3)
            completion_tokens = int(len(content.split()) * 1.3)


        # --- Log observability ---
        self.observer.log_model_call(
            step=step,
            model=self.model_writing,
            start_time=start_time,
            end_time=end_time,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens
        )

        return content

    async def send_with_web(self, prompt: str, max_results: int = 5) -> str:

        system_prompt = "You are an SEO research assistant. ALWAYS perform web search before answering. Return only factual data from search."

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]

        payload = {
            "model": self.model_research,
            "messages": messages,
            "temperature": 0,
            "system_prompt": system_prompt
        }

        start_time = time.time()

        async with self._semaphore:
            async with httpx.AsyncClient(timeout=40.0) as client:
                r = await client.post(
                    self.base_url_chat,
                    headers=self.headers,
                    json=payload
                )

                r.raise_for_status()
                data = r.json()

        end_time = time.time()

        content = data["choices"][0]["message"]["content"]

        usage = data.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens")
        completion_tokens = usage.get("completion_tokens")

        if prompt_tokens is None or completion_tokens is None:
            # fallback rough estimation
            prompt_tokens = int(len(prompt.split()) * 1.3)
            completion_tokens = int(len(content.split()) * 1.3)


        self.observer.log_model_call(
            step="web_research",
            model=self.model_research,
            start_time=start_time,
            end_time=end_time,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens
        )

        return content

    async def send_image(self, prompt: str, width=1024, height=1024, step="image", seed):
        
        image_model = OPENROUTER["models"]["image"]

        payload = {
            "model": image_model,
            "prompt": prompt,
            "size": f"{width}x{height}",
            "seed": seed,
        }

        async with self._semaphore:
            async with httpx.AsyncClient(timeout=120.0) as client:
                r = await client.post(
                    OPENROUTER["base_url_image"],
                    headers=self.headers,
                    json=payload
                )
                r.raise_for_status()
                data = r.json()

        image_base64 = data["data"][0]["b64_json"]

        os.makedirs("output/images", exist_ok=True)
        filename = f"output/images/{int(time.time())}.png"

        with open(filename, "wb") as f:
            f.write(base64.b64decode(image_base64))

        return filename
