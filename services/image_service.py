import os
import requests
import logging
from urllib.parse import quote

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PollinationsClient:
    """
    Client to interact with Pollinations API for AI-generated images.
    """

    BASE_URL = "https://image.pollinations.ai/prompt/"

    def __init__(self, save_dir: str = "images"):
        """
        :param save_dir: Local folder to save downloaded images.
        """
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)

    def generate_image_url(self, prompt: str) -> str:
        """
        Return the Pollinations URL for a given prompt.
        :param prompt: Text prompt for image generation
        :return: URL string
        """
        encoded_prompt = quote(prompt)
        return f"{self.BASE_URL}{encoded_prompt}"

    def download_image(self, prompt: str, filename: str = None) -> str:
        """
        Downloads image from Pollinations API.
        :param prompt: Text prompt for image generation
        :param filename: Optional local filename (with extension .png or .jpg)
        :return: Local file path of saved image
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

            logger.info(f"Image saved to: {filepath}")
            return filepath

        except requests.RequestException as e:
            logger.error(f"Failed to download image: {e}")
            return ""

# ======================
# Example usage
# ======================
if __name__ == "__main__":
    client = PollinationsClient(save_dir="generated_images")
    
    # Generate Pollinations URL (optional)
    url = client.generate_image_url("SEO article hero image, digital style")
    print("Image URL:", url)
    
    # Download image locally
    local_file = client.download_image("SEO article hero image, digital style")
    print("Saved image path:", local_file)
