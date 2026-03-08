from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class ArticleRequest(BaseModel):
    title: str = Field(..., description="The main title or topic of the article")
    keywords: List[str] = Field(..., description="List of target keywords, starting with the primary keyword")
    article_language: Optional[str] = Field("ar", description="Language code (e.g., 'ar', 'en')")
    area: Optional[str] = Field(None, description="Target geographic area (e.g., 'Riyadh')")
    brand_url: Optional[str] = Field(None, description="The client's main website URL")
    urls: Optional[List[Dict[str, str]]] = Field(
        default_factory=list, 
        description="List of internal links to use in the format [{'link': 'url', 'text': 'anchor'}]"
    )
    include_meta_keywords: Optional[bool] = Field(True, description="Whether to generate meta keywords")
    brand_visual_style: Optional[str] = Field(None, description="Description of the brand's visual style")
    image_frame_path: Optional[str] = Field(None, description="Path to a visual frame/template for images")

class ArticleResponse(BaseModel):
    status: str
    message: str
    slug: Optional[str] = None
    output_dir: Optional[str] = None
    html_content: Optional[str] = None
    markdown_content: Optional[str] = None
