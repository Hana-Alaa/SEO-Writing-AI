import logging
from typing import List, Dict
import urllib.parse
import os
import requests
from PIL import Image
from PIL import Image

def resize_image(filepath: str, max_width: int, max_height: int, quality: int = 85):
    """
    Resizes an image to fit within max_width/max_height while maintaining aspect ratio.
    """
    try:
        with Image.open(filepath) as img:
            img.thumbnail((max_width, max_height))
            img.save(filepath, optimize=True, quality=quality)
    except Exception as e:
        logger.error(f"Failed to resize image {filepath}: {e}")

logger = logging.getLogger(__name__)

class ImageGenerator:
    """
    Handles image generation using Pollinations.ai
    """
    def __init__(self, save_dir: str = "images"):
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)

    def generate_images(self, image_prompts: List[Dict[str, str]], primary_keyword: str = None) -> List[Dict[str, str]]:
        if len(image_prompts) != 7:
            raise ValueError("Exactly 7 images are required per article.")

        generated_images = []

        STYLE_PREFIX = {
            "Featured Image": "High-quality photorealistic featured image, professional lighting, ultra realistic,",
            "Infographic": "Clean infographic style illustration, flat design, clear visual hierarchy,",
            "Illustration": "Minimalist conceptual illustration, modern style, soft colors,"
        }

        for item in image_prompts:
            # Extract inputs
            prompt = item.get("prompt", "").strip()
            alt_text = item.get("alt_text", "").strip()
            section_id = item.get("section_id", "").strip()
            image_type = item.get("image_type", "Illustration")

            # Validation layer
            if not prompt or not alt_text or not section_id:
                raise ValueError("Invalid image prompt object")
            
            # ALT text check
            if primary_keyword and primary_keyword.lower() not in alt_text.lower():
                raise ValueError(f"ALT text for section {section_id} must contain the primary keyword '{primary_keyword}'.")

            # Style injection
            style_prefix = STYLE_PREFIX.get(image_type, "")
            final_prompt = f"{style_prefix} {prompt}"
            encoded_prompt = urllib.parse.quote(final_prompt)

            # Deterministic seed
            seed = hash(section_id) % 100000

            # Build image URL
            width = 1200
            height = 630
            image_url = (
                f"https://image.pollinations.ai/prompt/{encoded_prompt}"
                f"?width={width}&height={height}&seed={seed}"
            )

            logger.info(f"Generated AI image for section {section_id}")

            generated_images.append({
                "url": image_url,
                "alt_text": alt_text,
                "caption": f"Image: {prompt[:100]}...",
                "section_id": section_id,
                "image_type": image_type
            })

        # Featured Image enforcement
        featured_count = sum(1 for item in generated_images if item.get("image_type") == "Featured Image")
        if featured_count != 1:
            raise ValueError("Exactly one Featured Image is required.")

        return generated_images
    
    def generate_image_url(self, prompt: str) -> str:
        encoded_prompt = urllib.parse.quote(prompt)
        return f"https://image.pollinations.ai/prompt/{encoded_prompt}"

    def download_image(self, prompt: str, filename: str = None) -> str:
        """
        Downloads image from Pollinations API and resizes it for blog templates.
        """
        url = self.generate_image_url(prompt)
        filename = filename or f"{prompt[:30].replace(' ', '_')}.png"
        filepath = os.path.join(self.save_dir, filename)

        try:
            logger.info(f"Downloading image for prompt: '{prompt}'")
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            with open(filepath, "wb") as f:
                f.write(response.content)

            # **Resize and compress image for blog**
            resize_image(filepath, max_width=1200, max_height=630, quality=85)

            logger.info(f"Image saved and resized to: {filepath}")
            return filepath

        except requests.RequestException as e:
            logger.error(f"Failed to download image: {e}")
            return ""
    
    def save_responsive_versions(self, filepath: str):
        """
        Generate multiple sizes for blog templates.
        """
        base, ext = os.path.splitext(filepath)
        sizes = {
            "featured": (1200, 630),
            "inline": (800, 420),
            "thumbnail": (400, 210)
        }

        img = Image.open(filepath)
        for name, (w, h) in sizes.items():
            img_copy = img.copy()
            img_copy.thumbnail((w, h))
            new_path = f"{base}_{name}{ext}"
            img_copy.save(new_path, optimize=True, quality=85)
            logger.info(f"Saved {name} version: {new_path}")

