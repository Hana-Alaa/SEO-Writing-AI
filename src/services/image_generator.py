import os
import re
import logging
import asyncio
import httpx
import base64
from datetime import datetime
import hashlib
import json
from jinja2 import Template
from typing import List, Dict, Optional, Any
from PIL import Image, ImageDraw, ImageFilter, ImageEnhance, ImageStat
from io import BytesIO
from src.utils.json_utils import recover_json

logger = logging.getLogger(__name__)

class ImagePromptPlanner:
    def __init__(self, ai_client, template_path: str):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read())

    async def generate(self, title: str, primary_keyword, keywords: list, outline: list, brand_visual_style: str = "", num_images: int = 7, image_style: str = "illustration") -> list:
        prompt_text = self.template.render(
            title=title,
            primary_keyword=primary_keyword,
            keywords=keywords,
            outline=outline,
            brand_visual_style=brand_visual_style,
            num_images=num_images,
            image_style=image_style
        )
        response_data = await self.ai_client.send(prompt_text, step="image")
        raw_response = response_data.get("content", "[]") if isinstance(response_data, dict) else str(response_data or "[]")
        try:
            image_prompts = recover_json(raw_response)
            if not image_prompts or not isinstance(image_prompts, list):
                return []
            for p in image_prompts:
                p["image_type"] = p.get("image_type", "").strip().capitalize()
            featured = next((p for p in image_prompts if p["image_type"] == "Featured"), None)
            if not featured and image_prompts:
                image_prompts[0]["image_type"] = "Featured"
                featured = image_prompts[0]
            if not featured: return []
            others = [p for p in image_prompts if p["image_type"] != "Featured"]
            image_prompts = ([featured] + others)[:7]
            outline_ids_list = [s.get("section_id") for s in outline] if outline else ["sec_01"]
            for p in image_prompts:
                if p.get("section_id") not in set(outline_ids_list):
                    p["section_id"] = outline_ids_list[0]
            final_list = image_prompts[:7]
            intro_id = outline[0].get("section_id") if outline else "sec_01"
            featured_idx = next((i for i, p in enumerate(final_list) if p.get("image_type") == "Featured"), 0)
            final_list[0], final_list[featured_idx] = final_list[featured_idx], final_list[0]
            final_list[0]["section_id"] = intro_id
            final_list[0]["image_type"] = "Featured"
            if len(final_list) < 7:
                for i in range(7 - len(final_list)):
                    p = final_list[i % len(final_list)].copy()
                    p["image_type"] = "Illustration" if p["image_type"] == "Featured" else p["image_type"]
                    final_list.append(p)
            return final_list
        except Exception as e:
            logger.error(f"Image prompt parsing failed: {e}")
            return []

class ImageGenerator:
    STYLE_PREFIXES = {
        "Featured": "Premium hero header, UNCLUTTERED, sophisticated minimalist modern, professional photography, VERY WIDE SAFE MARGINS, SUBJECTS CENTERED, NO TEXT BY DEFAULT,",
        "Infographic": "3D isometric process flow, UNCLUTTERED, clean structural elegance, VERY WIDE SAFE MARGINS, CONTENT CENTERED, legible Arabic/English typography,",
        "Illustration": "Bespoke digital art, UNCLUTTERED, minimalist editorial, VERY WIDE SAFE MARGINS, SUBJECTS CENTERED,",
        "Mockup": "Ultra-premium 3D product render, UNCLUTTERED, elegant minimalist, VERY WIDE SAFE MARGINS, SUBJECTS CENTERED,"
    }

    def __init__(self, ai_client, save_dir: str, logo_path: str = None, template_path: str = None):
        self.ai_client = ai_client
        self.save_dir = save_dir
        self.logo_path = logo_path
        self.template_path = template_path
        os.makedirs(self.save_dir, exist_ok=True)

    async def generate_images(self, items, primary_keyword="", image_frame_path=None, logo_path=None, brand_visual_style: str = "", workflow_logger=None):
        """Orchestrates parallel image generation and processing."""
        tasks = []
        for i, item in enumerate(items):
            tasks.append(self._process_single_image(
                item, 
                item.get("section_id", f"img_{i}"), 
                item.get("image_type", "Illustration"), 
                seed=None, 
                image_frame_path=image_frame_path, 
                logo_path=logo_path, 
                workflow_logger=workflow_logger
            ))
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    async def _process_single_image(self, item, section_id, image_type, seed, image_frame_path=None, logo_path=None, workflow_logger=None):
        start_time = datetime.now() if workflow_logger else None
        prompt = item.get("prompt", "")
        style_prefix = self.STYLE_PREFIXES.get(image_type.capitalize(), self.STYLE_PREFIXES["Illustration"])
        final_prompt = f"{style_prefix} {prompt}"
        local_path = await self._call_openrouter(final_prompt, section_id, image_type, seed)
        if not local_path: return None
        apply_brand = (image_type != "MasterFrame")
        processed_path = await asyncio.to_thread(self._process_image_versions, local_path, image_frame_path, logo_path, apply_brand)
        return {"section_id": section_id, "image_type": image_type, "alt_text": item.get("alt_text", "").strip(), "local_path": processed_path, "url": processed_path}

    async def _call_openrouter(self, prompt: str, section_id: str, image_type: str, seed: int = None) -> str:
        try:
            filepath = await self.ai_client.send_image(prompt, 1024, 1024, save_dir=self.save_dir, seed=seed)
            return filepath or ""
        except Exception as e:
            logger.error(f"Image generation failed for {section_id}: {e}")
            return ""

    def _process_image_versions(self, filepath: str, image_frame_path: str = None, logo_path: str = None, apply_brand: bool = True) -> str:
        with Image.open(filepath) as img:
            img = img.convert("RGBA")
            frame_exists = image_frame_path and os.path.exists(image_frame_path)
            logo_exists = logo_path and os.path.exists(logo_path)
            if apply_brand and frame_exists:
                try:
                    img = self._composite_with_template(img, image_frame_path, logo_path=logo_path)
                except Exception as e:
                    logger.error(f"Frame composition failed: {e}")
                    img = img.resize((1200, 675), Image.Resampling.LANCZOS)
                    if logo_exists: img = self._add_logo(img, logo_path)
            elif logo_exists:
                img = img.resize((1200, 675), Image.Resampling.LANCZOS)
                img = self._add_logo(img, logo_path)
            else:
                img = img.resize((1200, 675), Image.Resampling.LANCZOS)
            webp_path = os.path.splitext(filepath)[0] + ".webp"
            img.convert("RGB").save(webp_path, format="WEBP", quality=92, method=6, optimize=True)
        if os.path.exists(filepath) and filepath != webp_path: os.remove(filepath)
        return webp_path

    def _composite_with_template(self, base_image: Image.Image, template_path: str, logo_path: str = None) -> Image.Image:
        try:
            with Image.open(template_path) as template:
                template = template.convert("RGBA")
                tw, th = template.size
                pixels = list(template.getdata())
                hole_bbox = [tw, th, 0, 0]
                has_transparency = False
                for i, p in enumerate(pixels):
                    if p[3] < 128:
                        x, y = i % tw, i // tw
                        hole_bbox[0], hole_bbox[1] = min(hole_bbox[0], x), min(hole_bbox[1], y)
                        hole_bbox[2], hole_bbox[3] = max(hole_bbox[2], x), max(hole_bbox[3], y)
                        has_transparency = True
                
                white_box = None
                if not has_transparency:
                    for i, p in enumerate(pixels):
                        if p[0] > 240 and p[1] > 240 and p[2] > 240:
                            x, y = i % tw, i // tw
                            if white_box is None: white_box = [x, y, x, y]
                            else:
                                white_box[0], white_box[1] = min(white_box[0], x), min(white_box[1], y)
                                white_box[2], white_box[3] = max(white_box[2], x), max(white_box[3], y)

                if has_transparency or white_box:
                    if has_transparency and (hole_bbox[2]-hole_bbox[0]) > tw*0.5:
                        # Overlay Mode
                        template_aspect = tw / th
                        img_aspect = base_image.size[0] / base_image.size[1]
                        if img_aspect > template_aspect:
                            fill_w, fill_h = int(th * img_aspect), th
                        else:
                            fill_w, fill_h = tw, int(tw / img_aspect)
                        resized_base = base_image.resize((fill_w, fill_h), Image.Resampling.LANCZOS).crop(((fill_w - tw)//2, (fill_h - th)//2, (fill_w + tw)//2, (fill_h + th)//2))
                        
                        bottom_region = (0, int(th * 0.85), tw, th)
                        if self._is_region_busy(resized_base, bottom_region):
                            if logo_path: return self._add_logo(resized_base, logo_path, position="bottom_right")
                            return resized_base
                        final = resized_base.convert("RGBA")
                        final.alpha_composite(template)
                        return final

                    # Hole Mode
                    tx1, ty1, tx2, ty2 = hole_bbox if has_transparency else white_box
                    box_w, box_h = tx2 - tx1 + 1, ty2 - ty1 + 1
                    box_aspect = box_w / box_h
                    img_aspect = base_image.size[0] / base_image.size[1]
                    s_w, s_h = int(box_w * 0.95), int(box_h * 0.95)
                    if img_aspect > box_aspect: fit_w, fit_h = s_w, int(s_w / img_aspect)
                    else: fit_h, fit_w = s_h, int(s_h * img_aspect)
                    foreground = base_image.resize((fit_w, fit_h), Image.Resampling.LANCZOS).filter(ImageFilter.SHARPEN)
                    if img_aspect > box_aspect: f_w, f_h = int(box_h * img_aspect), box_h
                    else: f_w, f_h = box_w, int(box_w / img_aspect)
                    background = base_image.resize((f_w, f_h), Image.Resampling.LANCZOS).crop(((f_w-box_w)//2, (f_h-box_h)//2, (f_w+box_w)//2, (f_h+box_h)//2)).filter(ImageFilter.GaussianBlur(radius=25))
                    background = ImageEnhance.Brightness(background).enhance(0.8)
                    block = Image.new("RGBA", (box_w, box_h), (0, 0, 0, 0))
                    block.paste(background, (0,0))
                    block.paste(foreground, ((box_w-fit_w)//2, (box_h-fit_h)//2), mask=foreground if foreground.mode == 'RGBA' else None)
                    result = Image.new("RGBA", (tw, th), (0, 0, 0, 0))
                    result.paste(block, (tx1, ty1))
                    return Image.alpha_composite(result, template)

                # Fallback Split-Frame
                split_y = int(th * 0.78)
                target_aspect = tw / split_y
                base_w, base_h = base_image.size
                if (base_w / base_h) > target_aspect: f_w, f_h = int(split_y * (base_w/base_h)), split_y
                else: f_w, f_h = tw, int(tw / (base_w/base_h))
                resized_base = base_image.resize((f_w, f_h), Image.Resampling.LANCZOS).crop(((f_w-tw)//2, (f_h-split_y)//2, (f_w+tw)//2, (f_h+split_y)//2))
                result = Image.new("RGBA", (tw, th))
                result.paste(resized_base, (0, 0))
                bottom_region = (0, int(th * 0.78), tw, th)
                if self._is_region_busy(base_image.resize((tw, th), Image.Resampling.LANCZOS), bottom_region):
                    if logo_path: return self._add_logo(result, logo_path, position="bottom_right")
                    return result
                frame_region = template.crop((0, split_y, tw, th))
                result.paste(frame_region, (0, split_y), mask=frame_region)
                return result
        except Exception as e:
            logger.error(f"Template composition failed: {e}")
            return base_image

    def _add_logo(self, canvas: Image.Image, logo_path: str, position: str = "top_left") -> Image.Image:
        if not logo_path or not os.path.exists(logo_path): return canvas
        try:
            with Image.open(logo_path) as logo:
                logo = logo.convert("RGBA")
                cw, ch = canvas.size
                scale = min((cw * 0.20) / logo.size[0], (ch * 0.10) / logo.size[1])
                logo = logo.resize((int(logo.size[0] * scale), int(logo.size[1] * scale)), Image.Resampling.LANCZOS)
                lw, lh = logo.size
                m = int(cw * 0.02)
                if position == "bottom_center": x, y = (cw - lw)//2, ch - lh - m
                elif position == "bottom_right": x, y = cw - lw - m, ch - lh - m
                else: x, y = m, m
                canvas.paste(logo, (x, y), mask=logo)
                return canvas
        except Exception as e:
            logger.error(f"Logo overlay failed: {e}")
            return canvas

    def _is_region_busy(self, img: Image.Image, region_bbox: tuple, threshold: float = 12.0) -> bool:
        try:
            region = img.crop(region_bbox).convert("L")
            edges = region.filter(ImageFilter.FIND_EDGES)
            stat = ImageStat.Stat(edges)
            return stat.mean[0] > threshold
        except Exception as e:
            logger.error(f"Error in _is_region_busy: {e}")
            return False

    def create_branded_template(self, base_frame_path: str, logo_path: str, output_path: str) -> Optional[str]:
        try:
            with Image.open(base_frame_path) as base:
                base = base.convert("RGBA")
                import numpy as np
                img_data = np.array(base).astype(float)
                whiteness = np.min(img_data[:, :, :3], axis=2)
                t_low, t_high = 235.0, 254.0
                alpha_mask = 255.0 * (t_high - whiteness) / (t_high - t_low)
                img_data[:, :, 3] = np.clip(alpha_mask, 0, 255).astype(np.uint8)
                base = Image.fromarray(img_data.astype(np.uint8))
                base = self._add_logo(base, logo_path, position="bottom_right")
                base.save(output_path, "PNG")
                return output_path
        except Exception as e:
            logger.error(f"Failed to create branded template: {e}"); return None
