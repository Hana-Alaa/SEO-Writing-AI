import asyncio
import logging
import time
from typing import Any, ClassVar, Dict, Optional

import httpx

from src.config.ai_config import AGENTROUTER
from src.services.ai_client_base import BaseAIClient
from src.services.openrouter_client import OpenRouterClient
from src.utils.observability import ObservabilityTracker

logger = logging.getLogger(__name__)


class AgentRouterClient(BaseAIClient):
    """
    OpenAI-compatible AgentRouter text client.

    AgentRouter is used for regular text generation only.

    Web research stays on the existing OpenRouter client because the current
    OpenRouter research model is explicitly web-enabled. Prompting a normal
    AgentRouter chat model to "search" is not enough to guarantee live SERP data.
    """

    _semaphore: ClassVar[Optional[asyncio.Semaphore]] = None

    def __init__(self, api_key: Optional[str] = None, fallback_client: Optional[OpenRouterClient] = None):
        if AgentRouterClient._semaphore is None:
            AgentRouterClient._semaphore = asyncio.Semaphore(10)

        self.observer = ObservabilityTracker()
        self.api_key = api_key or AGENTROUTER["api_key"]
        self.model_writing = AGENTROUTER["models"]["writing"]
        self.base_url = AGENTROUTER["base_url"].rstrip("/")
        self.base_url_chat = f"{self.base_url}/chat/completions"
        self.client = httpx.AsyncClient(timeout=40.0)

        self.fallback_client = fallback_client or OpenRouterClient()
        self.fallback_client.observer = self.observer

        if not self.api_key:
            logger.warning("AGENTROUTER_API_KEY is missing")

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "HTTP-Referer": AGENTROUTER["site_url"],
            "X-Title": AGENTROUTER["site_name"],
            "Content-Type": "application/json",
        }

    @staticmethod
    def load_prompt(path: str) -> str:
        return OpenRouterClient.load_prompt(path)

    async def send(
        self,
        prompt: str,
        step: str = "default",
        max_tokens: Optional[int] = None,
        reasoning: Optional[bool] = None,
    ) -> Dict[str, Any]:
        system_prompt = self.load_prompt("assets/prompts/system_persona.txt")
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]

        payload: Dict[str, Any] = {
            "model": self.model_writing,
            "messages": messages,
            "temperature": 0,
        }

        if max_tokens:
            payload["max_tokens"] = max_tokens

        start_time = time.time()
        data = await self._post_with_retry(self.base_url_chat, payload)

        if not data or "choices" not in data or not data["choices"]:
            logger.error(f"Invalid AgentRouter response in step '{step}': {data}")
            return {
                "content": "Error: AI response failed.",
                "metadata": {
                    "provider": "agentrouter",
                    "tokens": {"total_tokens": 0, "prompt_tokens": 0, "completion_tokens": 0},
                    "prompt": prompt,
                    "response": "Error: AI response failed.",
                    "model": self.model_writing,
                    "duration": time.time() - start_time,
                },
            }

        end_time = time.time()
        message = data["choices"][0]["message"]
        content = message.get("content") or ""
        usage = data.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens")
        completion_tokens = usage.get("completion_tokens")

        if prompt_tokens is None or completion_tokens is None:
            prompt_tokens = int(len(prompt.split()) * 1.3)
            completion_tokens = int(len(content.split()) * 1.3)

        self.observer.log_model_call(
            step=step,
            model=self.model_writing,
            start_time=start_time,
            end_time=end_time,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )

        return {
            "content": content,
            "metadata": {
                "provider": "agentrouter",
                "duration": end_time - start_time,
                "model": self.model_writing,
                "prompt": prompt,
                "response": content,
                "tokens": {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens,
                },
            },
        }

    async def send_with_web(self, prompt: str, max_results: int = 5) -> Dict[str, Any]:
        return await self.fallback_client.send_with_web(prompt, max_results=max_results)

    async def send_image(self, *args, **kwargs):
        return await self.fallback_client.send_image(*args, **kwargs)

    async def describe_image_style(self, *args, **kwargs):
        return await self.fallback_client.describe_image_style(*args, **kwargs)

    async def _post_with_retry(self, url: str, payload: Dict[str, Any]):
        import random

        async with self._semaphore:
            for attempt in range(4):
                try:
                    response = await self.client.post(url, headers=self.headers, json=payload)

                    if response.status_code != 200:
                        logger.error(f"AgentRouter HTTP Error {response.status_code}: {response.text}")
                        wait_time = (2 ** attempt) + random.uniform(0.1, 1.0)
                        await asyncio.sleep(wait_time)
                        continue

                    try:
                        return response.json()
                    except Exception:
                        logger.error(f"Invalid AgentRouter JSON response: {response.text}")
                        return None
                except httpx.HTTPError as exc:
                    logger.error(f"AgentRouter request failed: {exc}")
                    wait_time = (2 ** attempt) + random.uniform(0.1, 1.0)
                    await asyncio.sleep(wait_time)

        return None

    async def close(self):
        await self.client.aclose()
        await self.fallback_client.close()
