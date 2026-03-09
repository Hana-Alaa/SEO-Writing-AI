import asyncio
import os
import sys
import logging

# Set up simple logging
logging.basicConfig(level=logging.DEBUG)

# Add the project scope
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.openrouter_client import OpenRouterClient

async def main():
    client = OpenRouterClient()
    print("Testing image generation...")
    try:
        result = await client.send_image("A futuristic web design agency office in Riyadh, digital art")
        print(f"Result: {result}")
    except Exception as e:
        print(f"Exception caught: {e}")

if __name__ == "__main__":
    asyncio.run(main())
