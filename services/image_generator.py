import os
import re
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
from utils.safe_json import recover_json

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
    #     return list(unique.values()

    async def generate(self, title: str, primary_keyword, keywords: list, outline: list, brand_visual_style: str = "") -> list:
        prompt_text = self.template.render(
            title=title,
            primary_keyword=primary_keyword,
            keywords=keywords,
            outline=outline,
            brand_visual_style=brand_visual_style
        )

        raw_response = await self.ai_client.send(prompt_text, step="image") or "[]"

        try:
            image_prompts = recover_json(raw_response)

            if not image_prompts or not isinstance(image_prompts, list):
                logger.error(f"Image planner returned non-list or empty structure: {type(image_prompts)}")
                return []

            # Normalize types
            for p in image_prompts:
                p["image_type"] = p.get("image_type", "").strip().capitalize()

            # Keep exactly 1 Featured
            featured = next((p for p in image_prompts if p["image_type"] == "Featured"), None)
            if not featured:
                logger.error("No Featured image found.")
                # Fallback: make the first one featured if missing
                if image_prompts:
                    image_prompts[0]["image_type"] = "Featured"
                    featured = image_prompts[0]
                else:
                    return []

            others = [p for p in image_prompts if p["image_type"] != "Featured"]

            # Limit to 7 images total
            image_prompts = ([featured] + others)[:7]

            # Validate section_ids
            outline_ids = {s.get("section_id") for s in outline}

            for p in image_prompts:
                if p.get("section_id") not in outline_ids:
                    logger.error(f"Invalid section_id: {p.get('section_id')}")
                    return []

            # Remove duplicates safely
            # unique = {}
            # for p in image_prompts:
            #     unique[p.get("section_id")] = p

            # final_list = list(unique.values())
            final_list = image_prompts[:7]

            # enforce featured first + intro
            intro_id = outline[0].get("section_id") if outline else "sec_01"
            featured_idx = next((i for i, p in enumerate(final_list) if p.get("image_type") == "Featured"), None)

            if featured_idx is None and final_list:
                final_list[0]["image_type"] = "Featured"
                final_list[0]["section_id"] = intro_id
            elif featured_idx is not None and featured_idx != 0:
                final_list[0], final_list[featured_idx] = final_list[featured_idx], final_list[0]

            # ensure first is intro
            final_list[0]["section_id"] = intro_id
            final_list[0]["image_type"] = "Featured"

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
        "Illustration": "Minimalist conceptual illustration, modern style, soft transitions, professional digital art,",
        "Mockup": "Professional minimalist product mockup, clean desk setting, premium presentation, high quality 3D render,"
    }

    def __init__(self, ai_client, save_dir: str = "output/images", logo_path: str = None):
        self.save_dir = save_dir
        self.ai_client = ai_client
        self.logo_path = logo_path
        os.makedirs(self.save_dir, exist_ok=True)

    async def generate_images(self, image_prompts: List[Dict[str, str]], primary_keyword: str = None, logo_path: str = None, reference_path: str = None, brand_visual_style: str = "") -> List[Dict[str, Any]]:
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
            self._process_single_image(
                item=item,
                primary_keyword=primary_keyword,
                logo_path=logo_path or self.logo_path,
                reference_path=reference_path,
                brand_visual_style=brand_visual_style
            )
            for item in image_prompts
        ]

        results = await asyncio.gather(*(limited_task(t) for t in tasks))
        return [r for r in results if r]

    # async def _process_single_image(self, item: Dict[str, str], primary_keyword: str = None, logo_path: str = None) -> Optional[Dict[str, Any]]:
    #     """Internal worker to process a single image generation task."""
    #     prompt = item.get("prompt", "").strip()
    #     alt_text = item.get("alt_text", "").strip()
    #     section_id = item.get("section_id", "").strip()
    #     image_type = item.get("image_type", "Illustration")

    #     if not prompt or not section_id:
    #         logger.error(f"Invalid image prompt data for section {section_id}")
    #         return None

    #     style_prefix = self.STYLE_PREFIXES.get(image_type, self.STYLE_PREFIXES["Illustration"])
    #     final_prompt = f"{style_prefix} {prompt}"
    #     seed = int(hashlib.md5(section_id.encode()).hexdigest(), 16) % 4294967295

    #     logger.info("\n================ FINAL PROMPT (ImageGenerator) ================\n")
    #     logger.info(final_prompt)
    #     logger.info("\n=============================================================\n")

    #     local_path = await self._call_openrouter(final_prompt, section_id, image_type)

    #     if local_path:
    #         # CPU-bound image processing
    #         await asyncio.to_thread(self._process_image_versions, local_path, logo_path)
            
    #         return {
    #             "section_id": section_id,
    #             "image_type": image_type,
    #             "alt_text": alt_text,
    #             "local_path": local_path,
    #             "url": local_path
    #         }
        
    #     return None

    async def _process_single_image(self, item, primary_keyword=None, logo_path=None, reference_path=None, brand_visual_style=""):
        """Internal worker to process a single image generation task."""
        prompt = item.get("prompt", "").strip()
        section_id = item.get("section_id", "").strip()
        image_type = item.get("image_type", "Illustration")

        if not prompt or not section_id:
            return None

        style_prefix = self.STYLE_PREFIXES.get(image_type, self.STYLE_PREFIXES["Illustration"])
        style_hint = f" Brand style cues: {brand_visual_style}." if brand_visual_style else ""
        final_prompt = f"{style_prefix} {prompt}.{style_hint}"
        seed = int(hashlib.md5(section_id.encode()).hexdigest(), 16) % 4294967295

        local_path = await self._call_openrouter(final_prompt, section_id, image_type, seed, reference_path)
        if not local_path:
            return None 

        apply_logo = image_type in {"Featured", "Mockup"}
        processed_path = await asyncio.to_thread(self._process_image_versions, local_path, logo_path, apply_logo)

        return {
            "section_id": section_id,
            "image_type": image_type,
            "alt_text": item.get("alt_text", "").strip(),
            "local_path": processed_path,
            "url": processed_path
        }

    # async def _call_openrouter(self, prompt: str, section_id: str, image_type: str):

    #     # if image_type == "Featured":
    #     #     filepath = await self.ai_client.send_image(prompt, 1344, 768)
    #     # elif image_type == "Infographic":
    #     #     filepath = await self.ai_client.send_image(prompt, 1024, 1024)
    #     # else:
    #     #     filepath = await self.ai_client.send_image(prompt, 1024, 768)

    #     filepath = await self.ai_client.send_image(prompt, 1024, 1024, save_dir=self.save_dir)

    #     # response = await client.post(url, headers=headers, json=body)
    #     logger.info(f"Image API returned path for {section_id}: {filepath}")

    #     if not filepath:
    #         logger.error(f"Image generation failed for {section_id}")
    #         return ""

    #     return filepath


    async def _call_openrouter( self, prompt: str, section_id: str, image_type: str, seed: int = None, reference_path: str = None) -> str:
        try:
            try:
                filepath = await self.ai_client.send_image(
                    prompt,
                    1024,
                    1024,
                    save_dir=self.save_dir,
                    seed=seed,
                    reference_path=reference_path
                )
            except TypeError:
                filepath = await self.ai_client.send_image(
                    prompt,
                    1024,
                    1024,
                    save_dir=self.save_dir
                )

            logger.info(f"Image API returned path for {section_id}: {filepath}")
            return filepath or ""
        except Exception as e:
            logger.error(f"Image generation failed for {section_id}: {e}")
            return ""

    # def _process_image_versions(self, filepath: str, logo_path: str = None):
        # """Optimizes image for speed (WebP) and adds brand logo gracefully if provided."""
        # try:
        #     with Image.open(filepath) as img:
        #         img = img.convert("RGBA")
                
        #         # Resize to standard responsive size
        #         img = img.resize((1200, 675), Image.Resampling.LANCZOS)
                
        #         # Safely handle logo if path exists and is valid
        #         target_logo = logo_path or self.logo_path
        #         if target_logo and isinstance(target_logo, str) and os.path.exists(target_logo):
        #             img = self._add_logo(img, target_logo)
        #         else:
        #             logger.info(f"No valid logo found at {target_logo}. Proceeding without logo.")
                
        #         # Final conversion to RGB for WebP saving (WebP supports RGB)
        #         img = img.convert("RGB")
        #         img.save(filepath, format="WEBP", quality=85, optimize=True)
                
        # except Exception as e:
        #     logger.error(f"Processing image {filepath} failed: {e}")

    def _process_image_versions(self, filepath: str, logo_path: str = None, apply_logo: bool = True) -> str:
        with Image.open(filepath) as img:
            img = img.convert("RGBA").resize((1200, 675), Image.Resampling.LANCZOS)

            if apply_logo and logo_path and os.path.exists(logo_path):
                img = self._add_logo(img, logo_path)

            webp_path = os.path.splitext(filepath)[0] + ".webp"
            img.convert("RGB").save(webp_path, format="WEBP", quality=85, optimize=True)

        if os.path.exists(filepath) and filepath != webp_path:
            os.remove(filepath)

        return webp_path
    
    def _add_logo(self, base_image: Image.Image, logo_path: str) -> Image.Image:
        """Overlays a logo professionally on the base image."""
        try:
            with Image.open(logo_path) as logo:
                logo = logo.convert("RGBA")
                
                # Scale logo to exactly 12% of the base image width (looks more premium than 15%)
                base_w, base_h = base_image.size
                logo_w, logo_h = logo.size
                scale_ratio = (base_w * 0.12) / float(logo_w)
                new_logo_w = max(int(logo_w * scale_ratio), 1)
                new_logo_h = max(int(logo_h * scale_ratio), 1)
                
                logo = logo.resize((new_logo_w, new_logo_h), Image.Resampling.LANCZOS)
                
                # Dynamic Padding (Margin) based on image size
                margin_x = int(base_w * 0.04) # 4% margin from right
                margin_y = int(base_h * 0.04) # 4% margin from bottom
                
                # Positioning: Bottom Right
                position = (base_w - new_logo_w - margin_x, base_h - new_logo_h - margin_y)
                
                # Create a transparent layer for composition
                overlay = Image.new("RGBA", base_image.size, (0, 0, 0, 0))
                overlay.paste(logo, position, mask=logo)
                
                # Alpha composite merges them perfectly
                return Image.alpha_composite(base_image, overlay)
                
        except Exception as e:
            logger.error(f"Logo overlay failed for {logo_path}: {e}")
            # If logo fails, return the original image without crashing
            return base_image

