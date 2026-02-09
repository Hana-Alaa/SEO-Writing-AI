import os
import logging
import asyncio
import httpx
import base64
import hashlib
import json
from jinja2 import Template
from typing import List, Dict, Optional, Any
from PIL import Image
from io import BytesIO
from config.ai_config import STABILITY

logger = logging.getLogger(__name__)

class ImagePromptPlanner:
    def __init__(self, ai_client, template_path: str):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read())

    async def generate(self, title: str, keywords: list, outline: list) -> list:
        prompt_text = self.template.render(title=title, keywords=keywords, outline=outline)
        raw_response = await self.ai_client.send(prompt_text, step="image") or "[]"
        try:
            image_prompts = json.loads(raw_response)
            
        except Exception:
            return []

        unique = {}
        for p in image_prompts:
            unique[p.get("section_id")] = p
        return list(unique.values())

class ImageGenerator:
    """
    Handles image generation using Stability.ai API.
    Provides responsive versions and deterministic generation.
    """

    STYLE_PREFIXES = {
        "Featured Image": "High-quality photorealistic featured image, professional lighting, ultra realistic, highly detailed,",
        "Infographic": "Clean infographic style illustration, flat design, clear visual hierarchy, professional vector graphics,",
        "Illustration": "Minimalist conceptual illustration, modern style, soft transitions, professional digital art,"
    }

    def __init__(self, save_dir: str = "output/images", api_key: str = None):
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)
        self.api_key = api_key or STABILITY["api_key"]
        self.model = STABILITY["model"]
        self.base_url = STABILITY["base_url"]

        if not self.api_key:
            logger.warning("Stability.ai API Key is missing. Image generation will fail.")

    async def generate_images(self, image_prompts: List[Dict[str, str]], primary_keyword: str = None) -> List[Dict[str, Any]]:
        """
        Generates actual images using Stability.ai for a list of prompts in parallel.
        """
        if len(image_prompts) < 1:
            logger.warning("No image prompts provided.")
            return []

        # Create tasks for all images
        tasks = []
        for item in image_prompts:
            tasks.append(self._process_single_image(item, primary_keyword))
        
        # Run all generation tasks in parallel
        results = await asyncio.gather(*tasks)
        
        # Filter out None results (failures)
        return [r for r in results if r]

    async def _process_single_image(self, item: Dict[str, str], primary_keyword: str = None) -> Optional[Dict[str, Any]]:
        """Internal worker to process a single image generation task."""
        prompt = item.get("prompt", "").strip()
        alt_text = item.get("alt_text", "").strip()
        section_id = item.get("section_id", "").strip()
        image_type = item.get("image_type", "Illustration")

        if not prompt or not section_id:
            logger.error(f"Invalid image prompt data for section {section_id}")
            return None

        if primary_keyword and primary_keyword.lower() not in alt_text.lower():
            alt_text = f"{primary_keyword} - {alt_text}"

        style_prefix = self.STYLE_PREFIXES.get(image_type, self.STYLE_PREFIXES["Illustration"])
        final_prompt = f"{style_prefix} {prompt}"
        seed = int(hashlib.md5(section_id.encode()).hexdigest(), 16) % 4294967295

        local_path = await self._call_stability_api(final_prompt, seed, section_id)

        if local_path:
            # CPU-bound image processing
            await asyncio.to_thread(self._process_image_versions, local_path)
            
            return {
                "section_id": section_id,
                "image_type": image_type,
                "alt_text": alt_text,
                "local_path": local_path,
                "url": local_path
            }
        
        return None

    async def _call_stability_api(self, prompt: str, seed: int, section_id: str, retries: int = 2) -> str:
        """Internal helper to communicate with Stability.ai with retry logic (async)."""
        if not self.api_key:
            logger.error(f"Cannot call Stability API for {section_id}: API Key is missing.")
            return ""

        url = f"{self.base_url}/{self.model}/text-to-image"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        body = {
            "text_prompts": [{"text": prompt, "weight": 1}],
            "cfg_scale": 7,
            "height": 768, 
            "width": 1344, 
            "samples": 1,
            "steps": 30,
            "seed": seed,
            "sampler": "K_DPM_2_ANCESTRAL", 
            "clip_guidance_preset": "FAST_BLUE"
        }

        logger.info(f"Generated prompt for {section_id}: {prompt[:100]}...")
        
        async with httpx.AsyncClient(timeout=120.0) as client:
            for attempt in range(retries + 1):
                try:
                    logger.info(f"Stability API call for {section_id} (Attempt {attempt+1}/{retries+1})...")
                    response = await client.post(url, headers=headers, json=body)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if "artifacts" in data and len(data["artifacts"]) > 0:
                            image_data = data["artifacts"][0].get("base64")
                            filepath = os.path.join(self.save_dir, f"{section_id}.png")
                            with open(filepath, "wb") as f:
                                f.write(base64.b64decode(image_data))
                            logger.info(f"Successfully generated and saved {section_id}.png")
                            return filepath
                        else:
                            logger.error(f"Unexpected API response structure for {section_id}: {data}")
                    elif response.status_code == 429:
                        logger.warning(f"Rate limited on attempt {attempt + 1} for {section_id}. Retrying...")
                        await asyncio.sleep(5 * (attempt + 1))
                    else:
                        logger.error(f"Stability API error {response.status_code} for {section_id}: {response.text}")
                        if attempt < retries:
                            await asyncio.sleep(2 ** attempt)
                
                except httpx.TimeoutException:
                    logger.warning(f"Timeout on attempt {attempt + 1} for {section_id}. Retrying...")
                    if attempt < retries:
                        await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Unexpected error in Stability API call for {section_id}: {e}")
                    if attempt < retries:
                        await asyncio.sleep(1)
        
        logger.error(f"All {retries + 1} attempts failed for section {section_id}")
        return ""

    def _process_image_versions(self, filepath: str):
        """Generates 1200x630 (Featured), 800x420 (Inline), and 400x210 (Thumbnail)."""
        try:
            with Image.open(filepath) as img:
                base_name, _ = os.path.splitext(filepath)
                
                versions = {
                    "featured": (1200, 630),
                    "inline": (800, 420),
                    "thumbnail": (400, 210)
                }

                for suffix, (tw, th) in versions.items():
                    # Aspect-aware crop-to-fill
                    img_work = img.copy()
                    img_ratio = img_work.width / img_work.height
                    target_ratio = tw / th

                    if img_ratio > target_ratio:
                        new_width = int(target_ratio * img_work.height)
                        offset = (img_work.width - new_width) // 2
                        img_work = img_work.crop((offset, 0, offset + new_width, img_work.height))
                    else:
                        new_height = int(img_work.width / target_ratio)
                        offset = (img_work.height - new_height) // 2
                        img_work = img_work.crop((0, offset, img_work.width, offset + new_height))

                    img_work = img_work.resize((tw, th), Image.Resampling.LANCZOS)
                    save_path = f"{base_name}_{suffix}.png"
                    img_work.save(save_path, optimize=True, quality=85)
                    
                    if suffix == "featured":
                        img_work.save(filepath, optimize=True, quality=90) # Overwrite original with main 1200x630

                logger.info(f"Generated responsive versions for {filepath}")

        except Exception as e:
            logger.error(f"Processing image {filepath} failed: {e}")
