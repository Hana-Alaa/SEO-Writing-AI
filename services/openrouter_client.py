import os
import time
import requests
import logging
from typing import List, Dict, Optional, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OpenRouterClient:
    """
    Client for interacting with the OpenRouter API with built-in retry logic.
    """
    
    def __init__(self, api_key: Optional[str] = None, model: str = "google/gemini-2.0-flash-001"):
        """
        Initialize the client.
        :param api_key: OpenRouter API key. If None, tries to read from OPENROUTER_API_KEY env var.
        :param model: Default model to use.
        """
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            logger.warning("OpenRouter API Key not found. Please set OPENROUTER_API_KEY environment variable.")
            
        self.model = model
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "HTTP-Referer": "https://github.com/Start-SE/SEO-Writing-AI",  ##### url site
            "X-Title": "SEO Writing AI",  #### site title
            "Content-Type": "application/json"
        }

    def generate_completion(
        self, 
        messages: List[Dict[str, str]], 
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 4000,
        response_format: Optional[Dict[str, str]] = None,
        retries: int = 3,
        backoff_factor: float = 2.0
    ) -> Optional[str]:
        """
        Sends a request to OpenRouter with retry logic.
        
        :param messages: List of message objects [{"role": "user", "content": "..."}]
        :param model: Override default model.
        :param temperature: Creativity control.
        :param max_tokens: Max output tokens.
        :param response_format: e.g. {"type": "json_object"}
        :param retries: Number of retry attempts.
        :param backoff_factor: Multiplier for wait time between retries.
        :return: Generated content string, or None if failed.
        """
        target_model = model or self.model
        
        payload = {
            "model": target_model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        if response_format:
            payload["response_format"] = response_format

        attempt = 0
        while attempt < retries:
            try:
                logger.info(f"Sending request to OpenRouter (Model: {target_model}, Attempt: {attempt + 1})")
                response = requests.post(
                    self.base_url, 
                    headers=self.headers, 
                    json=payload, 
                    timeout=60
                )
                
                response.raise_for_status()
                data = response.json()
                
                if "choices" in data and len(data["choices"]) > 0:
                    content = data["choices"][0].get("message", {}).get("content")
                    return content
                else:
                    logger.error(f"Invalid response structure: {data}")
                    return None

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt < retries:
                    sleep_time = backoff_factor * (2 ** attempt)
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error("Max retries reached. Request failed.")
                    return None
            
            attempt += 1
        
        return None
