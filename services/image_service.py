import os
import logging
import requests
import base64
import hashlib
from PIL import Image
from io import BytesIO
from typing import List, Dict
from config.ai_config import STABILITY

logger = logging.getLogger(__name__)

class StabilityImageService:
    """
    Handles image generation using Stability.ai API.
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
            logger.warning("Stability.ai API Key is missing. Generation will fail.")

    def generate_image_prompts_only(self, outline: List[Dict], seo_meta: Dict) -> List[Dict]:
        """
        Generates 7 image prompts based on the article outline and primary keyword.
        One is designated as 'Featured Image', others as 'Illustration'.
        """
        main_keyword = seo_meta.get("main_keyword", "")
        prompts = []
        
        for i, section in enumerate(outline[:7]):
            image_type = "Featured Image" if i == 0 else "Illustration"
            section_title = section.get("title", f"Section {i+1}")
            
            prompts.append({
                "section_id": section.get("id", f"sec_{i+1}"),
                "prompt": f"Visual representation of '{section_title}' in the context of {main_keyword}",
                "alt_text": f"{main_keyword} - {section_title}",
                "image_type": image_type
            })
            
        # Ensure we have 7
        while len(prompts) < 7:
            idx = len(prompts) + 1
            prompts.append({
                "section_id": f"sec_extra_{idx}",
                "prompt": f"Professional SEO visual for {main_keyword}",
                "alt_text": f"{main_keyword} related visual",
                "image_type": "Illustration"
            })
            
        return prompts


    async def download_and_process_images(self, image_prompts: List[Dict]) -> List[Dict]:
        """
        Calls Stability.ai for each prompt in parallel, saves, and resizes the images.
        """
        import asyncio
        sem = asyncio.Semaphore(7)

        async def _process_item(item):
            async with sem:
                section_id = item.get("section_id")
                image_type = item.get("image_type", "Illustration")
                base_prompt = item.get("prompt")
                alt_text = item.get("alt_text")
                
                # 1. Apply Style Prefix
                style_prefix = self.STYLE_PREFIXES.get(image_type, self.STYLE_PREFIXES["Illustration"])
                final_prompt = f"{style_prefix} {base_prompt}"
                
                # 2. Generate Deterministic Seed
                seed = int(hashlib.md5(section_id.encode()).hexdigest(), 16) % 4294967295 # max seed for stability
                
                # 3. Call Stability API
                local_path = await asyncio.to_thread(self._generate_stability_image, final_prompt, seed, section_id)
                
                if local_path:
                    # 4. Resize to 1200x630
                    await asyncio.to_thread(self._resize_image, local_path)
                    
                return {
                    "section_id": section_id,
                    "image_type": image_type,
                    "alt_text": alt_text,
                    "local_path": local_path,
                    "url": local_path # Using local path as URL per requirements
                }

        tasks = [_process_item(item) for item in image_prompts]
        results = await asyncio.gather(*tasks)
        return list(results)

    def _generate_stability_image(self, prompt: str, seed: int, section_id: str) -> str:
        """Helper to call Stability.ai and save the image."""
        if not self.api_key:
            return ""

        url = f"{self.base_url}/{self.model}/text-to-image"
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        body = {
            "text_prompts": [{"text": prompt}],
            "cfg_scale": 7,
            "height": 1024, # Will be resized later
            "width": 1024,
            "samples": 1,
            "steps": 30,
            "seed": seed
        }
        
        try:
            logger.info(f"Generating Stability.ai image for {section_id}...")
            response = requests.post(url, headers=headers, json=body, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            for i, image in enumerate(data.get("artifacts", [])):
                filename = f"{section_id}.png"
                path = os.path.join(self.save_dir, filename)
                
                with open(path, "wb") as f:
                    f.write(base64.b64decode(image.get("base64")))
                
                return path
                
        except Exception as e:
            logger.error(f"Stability.ai generation failed for {section_id}: {e}")
            
        return ""

    def _resize_image(self, filepath: str):
        """Resizes image to 1200x630 while maintaining aspect ratio (filling/cropping)."""
        try:
            with Image.open(filepath) as img:
                target_w, target_h = 1200, 630
                
                # Maintain aspect ratio by cropping if needed
                img_ratio = img.width / img.height
                target_ratio = target_w / target_h
                
                if img_ratio > target_ratio:
                    # Image is wider than target
                    new_width = int(target_ratio * img.height)
                    offset = (img.width - new_width) // 2
                    img = img.crop((offset, 0, offset + new_width, img.height))
                else:
                    # Image is taller than target
                    new_height = int(img.width / target_ratio)
                    offset = (img.height - new_height) // 2
                    img = img.crop((0, offset, img.width, offset + new_height))
                
                img = img.resize((target_w, target_h), Image.Resampling.LANCZOS)
                img.save(filepath, optimize=True, quality=90)
                logger.info(f"Resized image saved: {filepath}")
        except Exception as e:
            logger.error(f"Failed to resize image {filepath}: {e}")
