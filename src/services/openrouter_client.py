import os
import time
import httpx
import base64
import logging
import asyncio
from pathlib import Path
from typing import List, Dict, Optional, Any, ClassVar
from src.config.ai_config import OPENROUTER
from src.services.ai_client_base import BaseAIClient
from src.utils.observability import ObservabilityTracker

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
    _semaphore: ClassVar[Optional[asyncio.Semaphore]] = None 

    def __init__(self, api_key: Optional[str] = None):
        if OpenRouterClient._semaphore is None:
            OpenRouterClient._semaphore = asyncio.Semaphore(10) # Support more parallel images
        # self.rate_semaphore = asyncio.Semaphore(2)
        self.observer = ObservabilityTracker()
        self.api_key = api_key or OPENROUTER["api_key"]
        # self.model = OPENROUTER["default_model"]
        self.model_writing = OPENROUTER["models"]["writing"]
        self.model_research = OPENROUTER["models"]["research"]
        # self.base_url = OPENROUTER["base_url"]
        self.base_url_chat = OPENROUTER["base_url_chat"]
        self.base_url_responses = OPENROUTER["base_url_responses"]
        self.client = httpx.AsyncClient(timeout=40.0)

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

    async def send(self, prompt: str, step: str = "default", max_tokens: Optional[int] = None, reasoning: Optional[bool] = None) -> Dict[str, Any]:
        system_prompt = self.load_prompt("assets/prompts/system_persona.txt")

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]

        payload = {
            "model": self.model_writing,
            "messages": messages,
            "temperature": 0
        }

        # Handle Reasoning
        if reasoning is True or (reasoning is None and OPENROUTER.get("reasoning", {}).get("enabled")):
            payload["reasoning"] = {
                "enabled": True,
                "effort": OPENROUTER.get("reasoning", {}).get("effort", "medium")
            }

        
        if max_tokens:
            payload["max_tokens"] = max_tokens
        # Removed hardcoded max_tokens for specific steps to avoid truncation

        start_time = time.time()

        # async with self.rate_semaphore:
        #     response = await actual_request()
        # _semaphore = asyncio.Semaphore(1)

        # async with self._semaphore:
        #     async with httpx.AsyncClient(timeout=25.0) as client:
        #         r = await client.post(
        #             self.base_url_chat,
        #             headers=self.headers,
        #             json=payload
        #         )
        #         r.raise_for_status()

        data = await self._post_with_retry(
            self.base_url_chat,
            payload
        )

        if not data or "choices" not in data or not data["choices"]:
            logger.error(f"Invalid API response in step '{step}': {data}")
            return {
                "content": "Error: AI response failed.", 
                "metadata": {
                    "tokens": {"total_tokens": 0, "prompt_tokens": 0, "completion_tokens": 0},
                    "prompt": prompt,
                    "response": "Error: AI response failed.",
                    "model": self.model_writing,
                    "duration": time.time() - start_time
                }
            }

        end_time = time.time()

        # --- Extract response text ---
        message = data["choices"][0]["message"]
        content = message.get("content")
        reasoning_details = message.get("reasoning") or message.get("reasoning_details")


        # --- Extract usage if available ---
        usage = data.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens")
        completion_tokens = usage.get("completion_tokens")
        reasoning_tokens = usage.get("reasoning_tokens", 0)

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

        return {
            "content": content,
            "metadata": {
                "duration": end_time - start_time,
                "model": self.model_writing,
                "prompt": prompt,
                "response": content,
                "tokens": {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "reasoning_tokens": reasoning_tokens,
                    "total_tokens": prompt_tokens + completion_tokens
                },
                "reasoning_details": reasoning_details
            }
        }

    async def send_with_web(self, prompt: str, max_results: int = 5) -> Dict[str, Any]:

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

        # async with self._semaphore:
        #     async with httpx.AsyncClient(timeout=40.0) as client:
        #         r = await client.post(
        #             self.base_url_chat,
        #             headers=self.headers,
        #             json=payload
        #         )

        #         r.raise_for_status()
        data = await self._post_with_retry(
            self.base_url_chat,
            payload
        )

        if not data or "choices" not in data or not data["choices"]:
            logger.error(f"Invalid Web Search response: {data}")
            return {"content": "Error: Web search failed.", "metadata": {"tokens": {"total_tokens": 0}}}

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

        return {
            "content": content,
            "metadata": {
                "duration": end_time - start_time,
                "model": self.model_research,
                "prompt": prompt,
                "response": content,
                "tokens": {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens
                }
            }
        }

    # async def send_image(self, prompt: str, width=1024, height=1024, step="image"):

    #     image_model = OPENROUTER["models"]["image"]

    #     payload = {
    #         "model": image_model,
    #         "prompt": prompt,
    #         "size": f"{width}x{height}",
    #     }

    #     try:
    #         data = await self._post_with_retry(
    #             OPENROUTER["base_url_image"],
    #             payload
    #         )

    #         logger.info(f"Image API raw response: {str(data)[:500]}")

    #         if not data:
    #             logger.error("Empty response from image API")
    #             return None

    #         if "data" not in data or not data["data"]:
    #             logger.error(f"Invalid image response structure: {data}")
    #             return None

    #         image_obj = data["data"][0]

    #         if "b64_json" not in image_obj:
    #             logger.error(f"No base64 image found in response: {image_obj}")
    #             return None

    #         image_base64 = image_obj["b64_json"]

    #         os.makedirs("output/images", exist_ok=True)
    #         filename = f"output/images/{int(time.time()*1000)}.png"

    #         with open(filename, "wb") as f:
    #             f.write(base64.b64decode(image_base64))

    #         return filename

    #     except Exception as e:
    #         logger.error(f"Image generation failed: {e}")
    #         return None

    async def send_image(self, prompt: str, width=1024, height=1024, save_dir: str = None, seed: int = None, reference_path: str = None):
        """Generate an image and save it to save_dir (or default output/images if not given)."""
        payload = {
            "model": OPENROUTER["models"]["image"],
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "modalities": ["image", "text"]
        }
        
        if seed is not None:
            payload["seed"] = seed
            
        if reference_path and os.path.exists(reference_path):
            with open(reference_path, "rb") as f:
                base64_image = base64.b64encode(f.read()).decode("utf-8")
                # Format as a list of content blocks for multimodal input
                payload["messages"][0]["content"] = [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/png;base64,{base64_image}"}
                    }
                ]

        data = await self._post_with_retry(
            self.base_url_chat,
            payload
        )

        if not data:
            logger.error("Empty response from image API")
            return None
            
        # Standard OpenAI/Image API format fallback
        if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
            image_url = data["data"][0].get("url") or data["data"][0].get("b64_json")
            if image_url:
                logger.info(f"Image found via standard format: {image_url[:50]}...")
                return await self._process_image_url(image_url, save_dir)

        if "choices" not in data:
            logger.error(f"Invalid image response: {data}")
            return None

        message = data["choices"][0]["message"]

        if "assets/images" not in message or not message["assets/images"]:
            logger.error(f"No images in response. Raw message content: {message}")
            logger.debug(f"Full response data: {data}")
            return None

        image_url = message["assets/images"][0]["image_url"]["url"]
        return await self._process_image_url(image_url, save_dir)

    async def _process_image_url(self, image_url: str, save_dir: str = None) -> Optional[str]:
        """Downloads/decodes image and saves to save_dir."""
        try:
            # data:image/png;base64,xxxxxx
            if image_url.startswith("data:"):
                header, encoded = image_url.split(",", 1)
                image_bytes = base64.b64decode(encoded)
            else:
                # Handle potential direct URL if OpenRouter returns one
                async with httpx.AsyncClient(timeout=30.0) as client:
                    r = await client.get(image_url)
                    r.raise_for_status()
                    image_bytes = r.content

            # Use provided save_dir or fall back to default
            target_dir = save_dir or "output/images"
            os.makedirs(target_dir, exist_ok=True)
            filename = os.path.join(target_dir, f"{int(time.time()*1000)}.png")

            with open(filename, "wb") as f:
                f.write(image_bytes)

            logger.info(f"Image saved to: {filename}")
            return filename
        except Exception as e:
            logger.error(f"Failed to process image URL: {e}")
            return None

    async def describe_image_style(self, image_path: str) -> Dict[str, Any]:
        """Analyzes a reference image and returns a dict with 'content' (description) and 'metadata'."""
        if not os.path.exists(image_path):
            logger.error(f"Reference image not found: {image_path}")
            return {"content": "Error: Image not found.", "metadata": {}}

        start_time = time.time()
        try:
            with open(image_path, "rb") as f:
                base64_image = base64.b64encode(f.read()).decode("utf-8")

            payload = {
                "model": "google/gemini-2.0-flash-001",
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "Describe the visual style of this image in 20 words or less."},
                            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{base64_image}"}}
                        ]
                    }
                ]
            }

            data = await self._post_with_retry(self.base_url_chat, payload)
            if data and "choices" in data and data["choices"]:
                description = data["choices"][0]["message"]["content"].strip()
                return {
                    "content": description,
                    "metadata": {
                        "model": "google/gemini-2.0-flash-001",
                        "duration": time.time() - start_time,
                        "tokens": data.get("usage", {})
                    }
                }
        except Exception as e:
            logger.error(f"Vision analysis failed: {e}")
        
        return {"content": "Style analysis failed.", "metadata": {"tokens": {}}}

    async def _post_with_retry(self, url, payload):
        import random
        async with self._semaphore:
            for attempt in range(4):
                try:
                    r = await self.client.post(
                        url,
                        headers=self.headers,
                        json=payload
                    )

                    if r.status_code != 200:
                        logger.error(f"HTTP Error {r.status_code}: {r.text}")
                        # await asyncio.sleep(2 ** attempt)
                        # Jittered backoff to prevent synchronized retries
                        wait_time = (2 ** attempt) + random.uniform(0.1, 1.0)
                        await asyncio.sleep(wait_time)
                        continue

                    try:
                        return r.json()
                    except Exception:
                        logger.error(f"Invalid JSON response: {r.text}")
                        return None

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        wait_time = 2 ** attempt
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"HTTP error: {e}")
                        return None

        return None


    async def close(self):
        await self.client.aclose()

