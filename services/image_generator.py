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

        response_data = await self.ai_client.send(prompt_text, step="image")
        raw_response = response_data.get("content", "[]") if isinstance(response_data, dict) else str(response_data or "[]")
        
        # DEBUG: Print exact raw response
        logger.debug(f"RAW IMAGE PLANNER RESPONSE:\n{raw_response}\n{'='*40}")

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

            # Validate section_ids and fix hallucinatory IDs
            outline_ids_list = [s.get("section_id") for s in outline] if outline else ["sec_01"]
            outline_ids_set = set(outline_ids_list)

            for p in image_prompts:
                if p.get("section_id") not in outline_ids_set:
                    logger.warning(f"Invalid section_id '{p.get('section_id')}' found in plan. Mapping to fallback.")
                    p["section_id"] = outline_ids_list[0]

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

            # PAD TO EXACTLY 7 IMAGES
            if final_list and len(final_list) < 7:
                original_len = len(final_list)
                for i in range(7 - original_len):
                    src_prompt = final_list[i % original_len].copy()
                    # Do not duplicate Featured 
                    src_prompt["image_type"] = "Illustration" if src_prompt["image_type"] == "Featured" else src_prompt["image_type"]
                    final_list.append(src_prompt)
                    
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
        "Featured": "Premium hero header, award-winning cinematic studio lighting, UNCLUTTERED, ultra-realistic 8k texture, sophisticated MINIMALIST modern composition, professional advertising photography, VERY WIDE SAFE MARGINS, KEEP SUBJECTS CENTERED AND AWAY FROM EDGES, STRICTLY NO TEXT, NO ARABIC LETTERS, NO GIBBERISH,",
        "Infographic": "Exclusive custom-designed 3D isometric process flow, UNCLUTTERED, high-end corporate visualization, clean structural elegance, soft ambient occlusion shadows, VERY WIDE SAFE MARGINS, KEEP CONTENT CENTERED AND AWAY FROM EDGES, STRICTLY NO ARABIC TEXT, MINIMALIST PERFECT ENGLISH TEXT ONLY, PERFECT SPELLING,",
        "Illustration": "Bespoke digital art, UNCLUTTERED, minimalist editorial style, soft color transitions, premium conceptual depth, professional stroke-work, high-end finish, VERY WIDE SAFE MARGINS, KEEP SUBJECTS CENTERED AND AWAY FROM EDGES, STRICTLY NO TEXT, NO ARABIC LETTERS, NO GIBBERISH,",
        "Mockup": "Ultra-premium 3D product render, UNCLUTTERED, elegant minimalist environment, soft blurred background, realistic materials (glass/metal/matte), high-end presentation, VERY WIDE SAFE MARGINS, KEEP SUBJECTS CENTERED AND AWAY FROM EDGES, STRICTLY NO TEXT, NO ARABIC LETTERS, NO GIBBERISH,"
    }

    def __init__(self, ai_client, save_dir: str = "output/images", image_frame_path: str = None):
        self.save_dir = save_dir
        self.ai_client = ai_client
        self.image_frame_path = image_frame_path
        os.makedirs(self.save_dir, exist_ok=True)

    async def generate_images(self, image_prompts: List[Dict[str, str]], primary_keyword: str = None, image_frame_path: str = None, logo_path: str = None, brand_visual_style: str = "", workflow_logger: Any = None) -> List[Dict[str, Any]]:
        """
        Generates actual images using Stability.ai for a list of prompts in parallel.
        """
        logger.info(f"[generate_images] Received {len(image_prompts)} image prompts to process.")
        
        if len(image_prompts) < 1:
            logger.warning("[generate_images] No image prompts provided. Returning empty list.")
            return []
        
        # Run all generation tasks in parallel
        sem = asyncio.Semaphore(2)

        async def limited_task(task):
            async with sem:
                return await task

        target_frame = image_frame_path or self.image_frame_path

        tasks = [
            self._process_single_image(
                item=item,
                primary_keyword=primary_keyword,
                image_frame_path=target_frame,
                logo_path=logo_path,
                brand_visual_style=brand_visual_style,
                workflow_logger=workflow_logger
            )
            for item in image_prompts
        ]

        results = await asyncio.gather(*(limited_task(t) for t in tasks))
        final_results = [r for r in results if r]
        logger.info(f"[generate_images] Finished processing. Returning {len(final_results)} successful images.")
        return final_results

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

    async def _process_single_image(self, item, primary_keyword=None, image_frame_path=None, logo_path=None, brand_visual_style="", workflow_logger: Any = None):
        """Internal worker to process a single image generation task."""
        prompt = item.get("prompt", "").strip()
        section_id = item.get("section_id", "").strip()
        image_type = item.get("image_type", "Illustration")

        if not prompt or not section_id:
            logger.error(f"[_process_single_image] Skipped generation for {section_id} due to missing prompt or section_id.")
            return None

        style_prefix = self.STYLE_PREFIXES.get(image_type, self.STYLE_PREFIXES["Illustration"])
        style_hint = f" Brand style cues: {brand_visual_style}." if brand_visual_style else ""
        final_prompt = f"{style_prefix} {prompt}.{style_hint}"
        # Use modulo 2147483647 (max signed INT32) to avoid 400 errors from Google/OpenRouter
        seed = int(hashlib.md5(section_id.encode()).hexdigest(), 16) % 2147483647

        logger.info(f"[_process_single_image] Calling OpenRouter for {section_id} with prompt: {final_prompt[:50]}...")
        
        start_time = workflow_logger.start_step(f"IMAGE_{image_type.upper()}_{section_id}") if workflow_logger else None
        
        local_path = await self._call_openrouter(final_prompt, section_id, image_type, seed)
        
        if workflow_logger and start_time:
            workflow_logger.end_step(
                step_name=f"IMAGE_{image_type.upper()}_{section_id}",
                start_time=start_time,
                prompt=final_prompt,
                response={"local_path": local_path},
                tokens={"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            )

        if not local_path:
            logger.error(f"[_process_single_image] OpenRouter failed to return a valid local_path for {section_id}.")
            return None 

        logger.info(f"[_process_single_image] Successfully downloaded image for {section_id} to {local_path}.")

        # Apply brand frame to ALL image types for consistent exclusivity
        apply_brand = True 
        processed_path = await asyncio.to_thread(self._process_image_versions, local_path, image_frame_path, logo_path, apply_brand)

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
                    seed=seed
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

    def _process_image_versions(self, filepath: str, image_frame_path: str = None, logo_path: str = None, apply_brand: bool = True) -> str:
        logger.info(f"[_process_image_versions] Processing: {filepath} | Frame: {image_frame_path}")
        
        with Image.open(filepath) as img:
            img = img.convert("RGBA")

            # Apply Brand Frame
            frame_exists = image_frame_path and os.path.exists(image_frame_path)
            logo_exists = logo_path and os.path.exists(logo_path)
            
            logger.info(f"[_process_image_versions] Frame path exists: {frame_exists} ({image_frame_path})")
            logger.info(f"[_process_image_versions] Logo path exists: {logo_exists} ({logo_path})")

            if apply_brand and frame_exists:
                logger.info(f"[_process_image_versions] Applying branded frame overlay")
                try:
                    img = self._composite_with_template(img, image_frame_path)
                except Exception as e:
                    logger.error(f"[_process_image_versions] Frame composition failed: {e}. Falling back to resize/logo.")
                    img = img.resize((1200, 675), Image.Resampling.LANCZOS)
                    if logo_exists:
                        img = self._add_logo(img, logo_path)
            
            # Simple resize if no frame provided or branding disabled
            elif logo_exists:
                logger.info(f"[_process_image_versions] No frame found. Applying simple logo overlay.")
                img = img.resize((1200, 675), Image.Resampling.LANCZOS)
                img = self._add_logo(img, logo_path)
            
            # No branding, just standard resize
            else:
                logger.info(f"[_process_image_versions] No frame or logo found. Standard resize only.")
                img = img.resize((1200, 675), Image.Resampling.LANCZOS)

            webp_path = os.path.splitext(filepath)[0] + ".webp"
            img.convert("RGB").save(webp_path, format="WEBP", quality=92, method=6, optimize=True)

        if os.path.exists(filepath) and filepath != webp_path:
            os.remove(filepath)

        return webp_path

    def _composite_with_template(self, base_image: Image.Image, template_path: str) -> Image.Image:
        """
        Composites an AI generated image into a branded template.
        Priority:
        1. Transparency (Alpha Channel) Detection
        2. White-space Bounding Box Detection
        3. Split-Frame fallback
        """
        try:
            with Image.open(template_path) as template:
                template = template.convert("RGBA")
                tw, th = template.size
                
                # Check for transparency (Alpha < 255)
                # This is the most accurate way for .png templates
                mask_bbox = template.getbbox() # This gets the non-zero alpha area, but we want the ZERO alpha area
                
                # We need to find the "hole" (transparent area)
                # Let's scan for a transparent bounding box
                pixels = list(template.getdata())
                hole_bbox = [tw, th, 0, 0]
                has_transparency = False
                
                for i, p in enumerate(pixels):
                    if p[3] < 128: # More lenient: detect semi-transparent areas too
                        x, y = i % tw, i // tw
                        hole_bbox[0] = min(hole_bbox[0], x)
                        hole_bbox[1] = min(hole_bbox[1], y)
                        hole_bbox[2] = max(hole_bbox[2], x)
                        hole_bbox[3] = max(hole_bbox[3], y)
                        has_transparency = True
                
                # Fallback check for "white box" areas in non-transparent templates
                white_box = None
                if not has_transparency:
                    # Scan for a large white/near-white block (e.g., for JPG templates)
                    for i, p in enumerate(pixels):
                        if p[0] > 240 and p[1] > 240 and p[2] > 240: # More lenient white detection
                            x, y = i % tw, i // tw
                            if white_box is None:
                                white_box = [x, y, x, y]
                            else:
                                white_box[0] = min(white_box[0], x)
                                white_box[1] = min(white_box[1], y)
                                white_box[2] = max(white_box[2], x)
                                white_box[3] = max(white_box[3], y)

                if has_transparency or white_box:
                    logger.info(f"Applying Smart Fit composition. Transparency: {has_transparency}, WhiteBox: {bool(white_box)}")
                    tx1, ty1, tx2, ty2 = hole_bbox if has_transparency else white_box
                    box_w, box_h = tx2 - tx1 + 1, ty2 - ty1 + 1
                    
                    # 1. Prepare AI image foreground (FIT / CONTAIN)
                    base_w, base_h = base_image.size
                    aspect = base_w / base_h
                    # Apply a 5% safety margin to ensure content isn't touched by frame edges
                    safe_box_w = int(box_w * 0.95)
                    safe_box_h = int(box_h * 0.95)
                    
                    if aspect > box_aspect: # Image is wider than hole
                        fit_w = safe_box_w
                        fit_h = int(safe_box_w / aspect)
                    else: # Image is taller than hole
                        fit_h = safe_box_h
                        fit_w = int(safe_box_h * aspect)

                    foreground = base_image.resize((fit_w, fit_h), Image.Resampling.LANCZOS)
                    
                    # 2. Prepare AI image background (FILL + BLUR)
                    # We reuse the FILL logic but blur it to handle aspect mismatches elegantly
                    if aspect > box_aspect:
                        fill_h = box_h
                        fill_w = int(box_h * aspect)
                    else:
                        fill_w = box_w
                        fill_h = int(box_w / aspect)
                    
                    background = base_image.resize((fill_w, fill_h), Image.Resampling.LANCZOS)
                    left = (fill_w - box_w) // 2
                    top = (fill_h - box_h) // 2
                    background = background.crop((left, top, left + box_w, top + box_h))
                    
                    from PIL import ImageFilter, ImageEnhance
                    background = background.filter(ImageFilter.GaussianBlur(radius=20))
                    # Darken background slightly to emphasize foreground
                    background = ImageEnhance.Brightness(background).enhance(0.85)

                    # 3. Layering
                    content_block = Image.new("RGBA", (box_w, box_h), (255, 255, 255, 255))
                    content_block.paste(background, (0, 0))
                    
                    # Center the foreground on the blurred background
                    offset_x = (box_w - fit_w) // 2
                    offset_y = (box_h - fit_h) // 2
                    content_block.paste(foreground, (offset_x, offset_y), mask=foreground if foreground.mode == 'RGBA' else None)

                    # Final composite
                    result = Image.new("RGBA", (tw, th), (255, 255, 255, 255))
                    result.paste(content_block, (tx1, ty1))
                    result.alpha_composite(template)
                    return result

                # Final Fallback: Split-Frame logic (unchanged essentially but ensures safe framing)
                logger.info("Using split-frame fallback")
                split_y = int(th * 0.78)
                frame_region = template.crop((0, split_y, tw, th))
                box_w, box_h = tw, split_y
                
                # Apply same Smart Fit logic for split frame if needed, but usually templates match
                base_w, base_h = base_image.size
                resized_base = base_image.resize((tw, int(tw / (base_w/base_h))), Image.Resampling.LANCZOS)
                
                result = Image.new("RGBA", (tw, th))
                result.paste(resized_base, (0, 0))
                result.paste(frame_region, (0, split_y))
                return result

        except Exception as e:
            logger.error(f"Template composition failed: {e}")
            return base_image

    def _find_white_rectangle(self, img: Image.Image) -> Optional[tuple]:
        """
        Sophisticated detection of the central white rectangle.
        Finds a contiguous white area starting from the center.
        """
        try:
            w, h = img.size
            cx, cy = w // 2, h // 2
            
            # 1. Verification: Is the center actually white?
            gray = img.convert("L")
            if gray.getpixel((cx, cy)) < 240:
                # If center isn't white, try searching a bit around it
                found = False
                for dx in range(-50, 51, 10):
                    for dy in range(-50, 51, 10):
                        if gray.getpixel((cx+dx, cy+dy)) >= 240:
                            cx, cy = cx+dx, cy+dy
                            found = True
                            break
                    if found: break
                if not found: return None

            # 2. Expand from center to find boundaries
            # Expand Left
            left = cx
            while left > 0 and gray.getpixel((left-1, cy)) >= 240:
                left -= 1
            # Expand Right
            right = cx
            while right < w-1 and gray.getpixel((right+1, cy)) >= 240:
                right += 1
            # Expand Top
            top = cy
            while top > 0 and gray.getpixel((cx, top-1)) >= 240:
                top -= 1
            # Expand Bottom
            bottom = cy
            while bottom < h-1 and gray.getpixel((cx, bottom+1)) >= 240:
                bottom += 1

            # 3. Validation: Minimum size
            if (right - left) < w * 0.2 or (bottom - top) < h * 0.2:
                return None

            return (left, top, right, bottom)
        except Exception as e:
            logger.error(f"Hole detection failed: {e}")
            return None

    def _detect_logo_position_from_template(self, template_path: str) -> Optional[Dict[str, Any]]:
        """
        Analyzes the template to find where a logo might be placed.
        Checks bottom corners for non-background content.
        """
        try:
            with Image.open(template_path) as img:
                img = img.convert("RGBA")
                w, h = img.size
                gray = img.convert("L")
                
                # Search regions: Bottom Left and Bottom Right
                regions = {
                    "bottom_right": (int(w*0.7), int(h*0.7), w-20, h-20),
                    "bottom_left": (20, int(h*0.7), int(w*0.3), h-20)
                }
                
                best_region = None
                max_density = 0
                
                for name, (x1, y1, x2, y2) in regions.items():
                    content_pixels = 0
                    total_pixels = (x2-x1) * (y2-y1)
                    
                    # Sample density
                    for y in range(y1, y2, 5):
                        for x in range(x1, x2, 5):
                            p = img.getpixel((x, y))
                            # If not white and not transparent
                            if p[0] < 240 and p[3] > 50:
                                content_pixels += 1
                    
                    density = content_pixels / (total_pixels / 25) # adj for sampling
                    if density > 0.1 and density > max_density:
                        max_density = density
                        best_region = name
                
                if best_region:
                    logger.info(f"Detected logo region: {best_region} (density: {max_density:.2f})")
                    return {"region": best_region}
                
            return None
        except Exception as e:
            logger.error(f"Logo detection failed: {e}")
            return None
    
    def _add_logo(self, base_image: Image.Image, logo_path: str, position_hint: Dict = None) -> Image.Image:
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
                margin_x = int(base_w * 0.04) 
                margin_y = int(base_h * 0.04)
                
                # Position logic based on hint or default
                region = position_hint.get("region", "bottom_right") if position_hint else "bottom_right"
                
                if region == "bottom_left":
                    position = (margin_x, base_h - new_logo_h - margin_y)
                else: # bottom_right
                    position = (base_w - new_logo_w - margin_x, base_h - new_logo_h - margin_y)
                
                # Create a rounded white background for the logo to make it "pop"
                padding = 10
                bg_rect = [
                    position[0] - padding, 
                    position[1] - padding, 
                    position[0] + new_logo_w + padding, 
                    position[1] + new_logo_h + padding
                ]
                
                overlay = Image.new("RGBA", base_image.size, (0, 0, 0, 0))
                from PIL import ImageDraw
                draw = ImageDraw.Draw(overlay)
                draw.rounded_rectangle(bg_rect, radius=8, fill=(255, 255, 255, 255))
                
                # Paste logo on top of the white background
                overlay.paste(logo, position, mask=logo)
                
                # Alpha composite merges them perfectly
                return Image.alpha_composite(base_image, overlay)
                
        except Exception as e:
            logger.error(f"Logo overlay failed for {logo_path}: {e}")
            # If logo fails, return the original image without crashing
            return base_image

