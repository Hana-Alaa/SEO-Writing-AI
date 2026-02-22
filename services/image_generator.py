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

logger = logging.getLogger(__name__)

class ImagePromptPlanner:
    def __init__(self, ai_client, template_path: str):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read())

    # async def generate(self, title: str, primary_keyword, keywords: list, outline: list) -> list:
    #     prompt_text = self.template.render(
    #         title=title,
    #         primary_keyword=primary_keyword,
    #         keywords=keywords,
    #         outline=outline
    #     )
    #     raw_response = await self.ai_client.send(prompt_text, step="image") or "[]"
    #     try:
    #         raw_response = raw_response.strip()

    #         if raw_response.startswith("```"):
    #             raw_response = raw_response.split("```")[1].strip()

    #         image_prompts = json.loads(raw_response)

    #         for p in image_prompts:
    #             p["image_type"] = p.get("image_type", "").strip().capitalize()

    #         # keep featured safely
    #         featured = next((p for p in image_prompts if p["image_type"] == "Featured"), None)
    #         others = [p for p in image_prompts if p["image_type"] != "Featured"]

    #         if not featured:
    #             logger.error("No Featured image found.")
    #             return []

    #         image_prompts = [featured] + others[:2]
    #         # required_images = sum(
    #         #     1 for s in outline
    #         #     if s.get("image_plan", {}).get("required", False)
    #         # )

    #         # if len(image_prompts) != required_images:
    #             # logger.error("Image planner did not return exactly 7 images.")
    #         if len(image_prompts) > 3:
    #             image_prompts = image_prompts[:3]


    #             # return []
    #         logger.info(f"Extracted image prompts: {image_prompts}")

    #         if featured_count != 1:
    #             logger.error("There must be exactly ONE Featured Image.")
    #             return []

    #         featured_count = sum(1 for p in image_prompts if p.get("image_type") == "Featured")

    #         for p in image_prompts:
    #             if p.get("section_id") not in outline_ids:
    #                 logger.error(f"Invalid section_id in image prompt: {p.get('section_id')}")
    #                 return []
            
    #         ids = [p.get("section_id") for p in image_prompts]
    #         if len(ids) != len(set(ids)):
    #             logger.error("Duplicate section_id detected in image prompts.")
    #             return []
            
    #         allowed_types = {"Featured", "Infographic", "Illustration"}

    #         # for p in image_prompts:
    #         #     if p.get("image_type") not in allowed_types:
    #         #         logger.error("Invalid image_type returned.")
    #         #         return []

    #         for p in image_prompts:
    #             p["image_type"] = p.get("image_type", "").capitalize()


    #     except Exception:
    #         return []

    #     unique = {}
    #     for p in image_prompts:
    #         unique[p.get("section_id")] = p
    #     return list(unique.values())


    async def generate(self, title: str, primary_keyword, keywords: list, outline: list) -> list:
        prompt_text = self.template.render(
            title=title,
            primary_keyword=primary_keyword,
            keywords=keywords,
            outline=outline
        )

        raw_response = await self.ai_client.send(prompt_text, step="image") or "[]"

        try:
            raw_response = raw_response.strip()

            if raw_response.startswith("```"):
                raw_response = raw_response.split("```")[1].strip()

            image_prompts = json.loads(raw_response)

            if not isinstance(image_prompts, list):
                return []

            # Normalize types
            for p in image_prompts:
                p["image_type"] = p.get("image_type", "").strip().capitalize()

            # Keep exactly 1 Featured
            featured = next((p for p in image_prompts if p["image_type"] == "Featured"), None)
            if not featured:
                logger.error("No Featured image found.")
                return []

            others = [p for p in image_prompts if p["image_type"] != "Featured"]

            # Limit to 3 images total
            image_prompts = [featured] + others[:2]

            # Validate section_ids
            outline_ids = {s.get("section_id") for s in outline}

            for p in image_prompts:
                if p.get("section_id") not in outline_ids:
                    logger.error(f"Invalid section_id: {p.get('section_id')}")
                    return []

            # Remove duplicates safely
            unique = {}
            for p in image_prompts:
                unique[p.get("section_id")] = p

            final_list = list(unique.values())

            logger.info(f"FINAL IMAGE PROMPTS COUNT: {len(final_list)}")

            return final_list

        except Exception as e:
            logger.error(f"Image prompt parsing failed: {e}")
            return []
            
class ImageGenerator:
    """
    Handles image generation using Stability.ai API.
    Provides responsive versions and deterministic generation.
    """

    STYLE_PREFIXES = {
        "Featured": "High-quality photorealistic featured image, professional lighting, ultra realistic, highly detailed,",
        "Infographic": "Clean infographic style illustration, flat design, clear visual hierarchy, professional vector graphics,",
        "Illustration": "Minimalist conceptual illustration, modern style, soft transitions, professional digital art,"
    }

    def __init__(self, ai_client, save_dir: str = "output/images"):
        self.save_dir = save_dir
        self.ai_client = ai_client
        os.makedirs(self.save_dir, exist_ok=True)

    async def generate_images(self, image_prompts: List[Dict[str, str]], primary_keyword: str = None) -> List[Dict[str, Any]]:
        """
        Generates actual images using Stability.ai for a list of prompts in parallel.
        """
        if len(image_prompts) < 1:
            logger.warning("No image prompts provided.")
            return []
        
        # Run all generation tasks in parallel
        sem = asyncio.Semaphore(2)

        async def limited_task(task):
            async with sem:
                return await task

        tasks = [
            self._process_single_image(item, primary_keyword)
            for item in image_prompts
        ]

        results = await asyncio.gather(*(limited_task(t) for t in tasks))
        
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

        style_prefix = self.STYLE_PREFIXES.get(image_type, self.STYLE_PREFIXES["Illustration"])
        final_prompt = f"{style_prefix} {prompt}"
        seed = int(hashlib.md5(section_id.encode()).hexdigest(), 16) % 4294967295

        logger.info("\n================ FINAL PROMPT (ImageGenerator) ================\n")
        logger.info(final_prompt)
        logger.info("\n=============================================================\n")

        local_path = await self._call_openrouter(final_prompt, section_id, image_type)

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

    async def _call_openrouter(self, prompt: str, section_id: str, image_type: str):

        # if image_type == "Featured":
        #     filepath = await self.ai_client.send_image(prompt, 1344, 768)
        # elif image_type == "Infographic":
        #     filepath = await self.ai_client.send_image(prompt, 1024, 1024)
        # else:
        #     filepath = await self.ai_client.send_image(prompt, 1024, 768)

        filepath = await self.ai_client.send_image(prompt, 1024, 1024, save_dir=self.save_dir)

        # response = await client.post(url, headers=headers, json=body)
        logger.info(f"Image API returned path for {section_id}: {filepath}")

        if not filepath:
            logger.error(f"Image generation failed for {section_id}")
            return ""

        return filepath

    def _process_image_versions(self, filepath: str):
        """Testing mode: keep single optimized image only."""
        try:
            with Image.open(filepath) as img:
                img = img.convert("RGB")
                img = img.resize((1200, 675), Image.Resampling.LANCZOS)
                img.save(filepath, format="WEBP", quality=80, optimize=True)
        except Exception as e:
            logger.error(f"Processing image {filepath} failed: {e}")
