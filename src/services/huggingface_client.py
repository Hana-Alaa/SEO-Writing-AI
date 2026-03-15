import os
import httpx
from src.services.ai_client_base import BaseAIClient

class HuggingFaceClient(BaseAIClient):
    def __init__(self, model: str):
        self.model = model
        self.api_key = os.getenv("HF_TOKEN")
        self.url = f"https://api-inference.huggingface.co/models/{model}"

    async def send(self, prompt: str) -> str:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "inputs": prompt,
            "parameters": {
                "max_new_tokens": 800,
                "temperature": 0.7,
                "return_full_text": False
            }
        }

        async with httpx.AsyncClient(timeout=90) as client:
            r = await client.post(self.url, headers=headers, json=payload)
            r.raise_for_status()
            data = r.json()

        if isinstance(data, list) and "generated_text" in data[0]:
            return data[0]["generated_text"]

        return ""
