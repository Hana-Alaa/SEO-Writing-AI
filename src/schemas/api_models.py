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
    
    # Advanced Customization Fields
    workflow_mode: str = Field("core", description="Mode of generation: 'core' (automated) or 'advanced' (customized)")
    article_type: Optional[str] = Field(None, description="Type of article: informational, commercial, comparison")
    tone: Optional[str] = Field(None, description="Tone of voice: professional, persuasive, casual, technical")
    pov: Optional[str] = Field(None, description="Point of view: 1st person singular, 1st person plural, 2nd person, 3rd person")
    article_size: Optional[str] = Field("1000", description="Target word count: 1000, 2000, 3000")
    brand_voice_description: Optional[str] = Field(None, description="Textual description of the brand voice")
    
    # Structure Controls
    include_conclusion: bool = Field(True, description="Whether to include a conclusion section")
    include_faq: bool = Field(True, description="Whether to include an FAQ section")
    include_tables: bool = Field(True, description="Whether to include tables where relevant")
    include_bullet_lists: bool = Field(True, description="Whether to include bullet lists")
    include_comparison_blocks: bool = Field(True, description="Whether to include comparison blocks")
    bold_key_terms: bool = Field(True, description="Whether to bold key terms in the content")
    
    # Media Controls
    num_images: int = Field(7, description="Number of images to generate")
    image_style: str = Field("illustration", description="Image style: illustration, infographic, mockup, mixed")
    image_size: str = Field("1024x1024", description="Size of generated images: 1024x1024, 1792x1024, 1024x1792")
    include_featured_image: bool = Field(True, description="Whether to include a featured image")
    custom_branding_frame: bool = Field(False, description="Whether to apply a custom branding frame to images")
    
    # SEO Controls
    custom_keyword_density: Optional[float] = Field(None, description="Specific target keyword density (0.5 to 3.0)")
    secondary_keywords: List[str] = Field(default_factory=list, description="Additional keywords to target")
    competitor_count: int = Field(5, description="Number of competitors to analyze: 3, 5, 10")
    
    external_urls: Optional[List[Dict[str, str]]] = Field(
        default_factory=list, 
        description="List of external links to use in the format [{'link': 'url', 'text': 'anchor'}]"
    )

class ArticleMetadata(BaseModel):
    title: str
    meta_title: str
    meta_description: str
    meta_keywords: str
    article_schema: Dict[str, Any] = Field(default_factory=dict)
    faq_schema: Dict[str, Any] = Field(default_factory=dict)

class ArticleImage(BaseModel):
    url: str
    alt_text: str
    image_type: str
    section_id: Optional[str] = None

class ArticleResponse(BaseModel):
    status: str
    message: str
    slug: Optional[str] = None
    output_dir: Optional[str] = None
    html_content: Optional[str] = None
    markdown_content: Optional[str] = None
    metadata: Optional[ArticleMetadata] = None
    images: Optional[List[ArticleImage]] = None
