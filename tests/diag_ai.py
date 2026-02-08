import asyncio
import httpx
import os

async def test_key(key):
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "openai/gpt-4o-mini",
        "messages": [{"role": "user", "content": "Hi"}],
        "max_tokens": 5
    }
    
    print(f"Testing key: {key[:15]}...")
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, headers=headers, json=payload, timeout=20.0)
            print(f"Status: {response.status_code}")
            if response.status_code == 200:
                print("SUCCESS!")
                return True
            else:
                print(f"Error: {response.text}")
                return False
        except Exception as e:
            print(f"Exception: {e}")
            return False

async def main():
    raw_key = "sk-or-v1 1abcca93967907a6aef98aef773024fc7f910d8d005a994c76dfbfa48c337d0e"
    # Format 1: Original
    if await test_key(raw_key): return
    # Format 2: With dash
    if await test_key(raw_key.replace(" ", "-")): return
    # Format 3: Hex only
    if await test_key(raw_key.split()[-1]): return

if __name__ == "__main__":
    asyncio.run(main())
