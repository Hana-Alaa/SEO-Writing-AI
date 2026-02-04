import requests

class PollinationsClient:
    BASE_URL = "https://image.pollinations.ai/prompt/"

    def generate_image(self, prompt: str) -> str:
        """
        Return URL of generated image from Pollinations API.
        """
        return f"{self.BASE_URL}{prompt}"
