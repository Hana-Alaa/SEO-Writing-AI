"""
Single source of truth for all AI providers.
"""

import os
from openai import OpenAI

# =========================
# TEXT MODELS
# =========================
OPENROUTER = {
    "api_key": os.getenv("OPENROUTER_API_KEY"),
    "default_model": "openai/gpt-4o-mini",
    "base_url": "https://openrouter.ai/api/v1/chat/completions",
    "site_url": "https://github.com/Start-SE/SEO-Writing-AI",
    "site_name": "SEO Writing AI"
}

GROQ = {
    "enabled": True,
    "api_key": os.getenv("GROQ_API_KEY"),
    "default_model": "llama-3.3-70b-versatile",
    "max_tokens": {
        "outline": 800,
        "section": 1200,
        "image": 300,
        "assembly": 700,
        "default": 700
    }
}

# =========================
# IMAGE MODELS
# =========================
POLLINATIONS = {
    "model": "stable-diffusion",
    "size": "1024x1024",
    "base_url": "https://image.pollinations.ai/prompt"
}

STABILITY = {
    "api_key": "sk-gjcxfako96pgqMBhn33UsDVh69pBCo4Z9Wqa62sSeHEEPD0s",
    "model": "stable-diffusion-xl-1024-v1-0",
    "base_url": "https://api.stability.ai/v1/generation",
    "size": "1024x1024"
}


IMAGES = {
    "provider": "mock"  
}
