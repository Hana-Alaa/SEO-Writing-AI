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
    "base_url_chat": "https://openrouter.ai/api/v1/chat/completions",
    "base_url_responses": "https://openrouter.ai/api/v1/responses",
    "base_url_image": "https://openrouter.ai/api/v1/chat/completions",
    "site_url": "https://github.com/Start-SE/SEO-Writing-AI",
    "site_name": "SEO Writing AI",

    "models": {
        "writing": "openai/gpt-5-mini",          #9
        # "writing": "google/gemini-3-flash-preview",
        # "Writing": "qwen/qwen3.6-plus:free",
        # "Writing": "openai/gpt-5.4-nano",     #47
        # "Writing": "google/gemini-3.1-flash-lite-preview",         #7
        "research": "openai/o4-mini:online",
        # "image": "black-forest-labs/flux.2-pro" 
        # "image": "google/gemini-3-pro-image-preview"
        # "image": "google/gemini-2.5-flash-image-preview"
        "image": "google/gemini-3.1-flash-image-preview"
    }
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
    "api_key": "STABILITY_API_KEY",
    "model": "stable-diffusion-xl-1024-v1-0",
    "base_url": "https://api.stability.ai/v1/generation",
    "size": "1024x1024"
}


IMAGES = {
    "provider": "mock"  
}


# config.py
STRUCTURE_RULES = {
    "informational": {
        "required_h2": [
            "Pros",
            "Cons",
            "Who is it for?",
            "Who should avoid it?",
            "Alternatives"
        ],
        "faq_required": True,
        "conclusion_required": True
    },
    "brand_commercial": {
        "benefits_required": True,
        "faq_required": False,
        "strong_cta_first_core": True
    }
}