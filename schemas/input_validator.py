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
    title: str = Field(..., min_length=5, max_length=200, description="The main topic/idea for the article")
    keywords: List[str] = Field(..., min_length=1, description="List of target keywords, first one is primary")
    urls: Optional[List[URLItem]] = Field(default=[], description="List of URLs to be included in the article")

    @field_validator('keywords')
    def validate_keywords(cls, v):
        cleaned = [k.strip() for k in v if k.strip()]
        if not cleaned:
            raise ValueError("At least one non-empty keyword is required")
        return cleaned


