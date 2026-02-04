import requests
from urllib.parse import quote
import os
import logging

logger = logging.getLogger(__name__)

class PollinationsImageService(BaseImageService):
    BASE_URL = "https://enter.pollinations.ai/api/generate"

    def __init__(self, save_dir="output/images"):
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)
        self.api_key = os.getenv("POLLINATIONS_API_KEY")
        if not self.api_key:
            logger.warning("No POLLINATIONS_API_KEY found. API calls will fail.")

    def generate_image_prompts_only(self, outline: list, seo_meta: dict) -> list:
        prompts = []
        for section in outline:
            prompts.append({
                "section_id": section.get("id"),
                "prompt": f"Image for section '{section.get('title')}'",
                "alt_text": f"Alt text for {section.get('title')}",
                "image_type": "Illustration"
            })
        return prompts

    def download_and_process_images(self, image_prompts: list) -> list:
        processed = []
        for item in image_prompts:
            prompt = item.get("prompt")
            filename = f"{item.get('section_id')}.png"
            filepath = os.path.join(self.save_dir, filename)

            if not self.api_key:
                logger.warning("API key not set, skipping download for prompt: %s", prompt)
                url = f"https://fake.url/{item.get('section_id')}.png"
            else:
                try:
                    response = requests.post(
                        self.BASE_URL,
                        headers={"Authorization": f"Bearer {self.api_key}"},
                        json={"prompt": prompt},
                        timeout=30
                    )
                    response.raise_for_status()
                    with open(filepath, "wb") as f:
                        f.write(response.content)
                    url = filepath
                except Exception as e:
                    logger.error(f"Failed to download image for '{prompt}': {e}")
                    url = ""

            processed.append({
                "section_id": item.get("section_id"),
                "image_type": item.get("image_type"),
                "alt_text": item.get("alt_text"),
                "local_path": filepath,
                "url": url
            })
        return processed
