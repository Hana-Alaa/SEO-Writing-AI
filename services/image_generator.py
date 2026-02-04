import logging
from typing import List, Dict
import urllib.parse

logger = logging.getLogger(__name__)

class ImageGenerator:
    """
    Handles image generation using Pollinations.ai
    """

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