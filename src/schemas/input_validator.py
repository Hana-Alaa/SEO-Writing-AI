from typing import List, Optional
from pydantic import BaseModel, HttpUrl, Field, field_validator

class URLItem(BaseModel):
    link: HttpUrl = Field(..., description="The target URL for the link")
    text: str = Field(..., min_length=1, description="The anchor text for the link")

    @field_validator('link', mode='before')
    def validate_link(cls, v):
        if v and not v.startswith(('http://', 'https://')):
            return f"https://{v}"
        return v

class ArticleInput(BaseModel):
    title: str = Field(..., min_length=5, max_length=200)
    keywords: List[str] = Field(...)
    urls: Optional[List[URLItem]] = Field(default=[])

    @field_validator('keywords')
    def validate_keywords(cls, v):
        cleaned = [k.strip() for k in v if k.strip()]
        if not cleaned:
            raise ValueError("At least one non-empty keyword is required")
        return cleaned
        
def normalize_urls(urls):
    out = []
    for u in urls:
        link = u.link if hasattr(u, "link") else u["link"]
        text = u.text if hasattr(u, "text") else u["text"]
        out.append({
            "url": str(link),
            "anchor_text": text,
            "link_type": "internal"
        })
    return out