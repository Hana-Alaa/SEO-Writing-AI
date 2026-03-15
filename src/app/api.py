import os
import json
import logging
from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.exceptions import RequestValidationError
import shutil
import uuid
from fastapi.middleware.cors import CORSMiddleware
from src.schemas.api_models import ArticleResponse, ArticleMetadata, ArticleImage
from src.services.workflow_controller import AsyncWorkflowController

# Ensure required directories exist
os.makedirs("output", exist_ok=True)
os.makedirs("logs", exist_ok=True)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="SEO Writing AI API",
    description="API for the autonomous SEO content generation pipeline.",
    version="1.0.0"
)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Log validation errors clearly in the terminal."""
    error_details = exc.errors()
    # Remove non-serializable objects from context (like ValueError)
    serializable_errors = []
    for err in error_details:
        err_copy = dict(err)
        if 'ctx' in err_copy:
            # Context often contains Exception objects which fail JSON serialization
            err_copy['ctx'] = {k: str(v) for k, v in err_copy['ctx'].items()}
        serializable_errors.append(err_copy)

    logger.error(f"Validation Error 422: {serializable_errors}")
    print(f"\n[VALIDATION ERROR 422] Received invalid request format:")
    for err in serializable_errors:
        print(f" -> Field: {err.get('loc')}, Error: {err.get('msg')}")
    print("\n")
    return JSONResponse(status_code=422, content={"detail": serializable_errors})

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Mount static files
app.mount("/static", StaticFiles(directory="src/app/static"), name="static")
app.mount("/output", StaticFiles(directory="output"), name="output")

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    """Serve the Web UI."""
    ui_path = "src/app/static/index.html"
    if os.path.exists(ui_path):
        with open(ui_path, "r", encoding="utf-8") as f:
            return f.read()
    return "UI not found. Please ensure src/app/static/index.html exists."

@app.get("/health")
async def health_check():
    """Simple health check endpoint."""
    return {"status": "ok", "message": "SEO Writing AI is running."}

@app.post("/generate", response_model=ArticleResponse)
async def generate_article(
    request: Request,
    title: str = Form(...),
    keywords: str = Form(...),
    article_language: str = Form(None),
    area: str = Form(None),
    urls: str = Form("[]"),
    external_urls: str = Form("[]"),
    include_meta_keywords: bool = Form(True),
    generate_images: bool = Form(True),
    logo_image: UploadFile = File(None),
    reference_image: UploadFile = File(None),
    brand_voice_guidelines: UploadFile = File(None),
    brand_voice_examples: UploadFile = File(None),
    
    # Advanced Customization
    workflow_mode: str = Form("core"),
    article_type: str = Form(None),
    tone: str = Form(None),
    pov: str = Form(None),
    article_size: str = Form("1000"),
    brand_voice_description: str = Form(None),
    
    # Structure Controls
    include_conclusion: bool = Form(True),
    include_faq: bool = Form(True),
    include_tables: bool = Form(True),
    include_bullet_lists: bool = Form(True),
    include_comparison_blocks: bool = Form(True),
    bold_key_terms: bool = Form(True),
    
    # Media Controls
    num_images: int = Form(7),
    image_style: str = Form("illustration"),
    image_size: str = Form("1024x1024"),
    include_featured_image: bool = Form(True),
    custom_branding_frame: bool = Form(False),
    
    # SEO Controls
    custom_keyword_density: float = Form(None),
    secondary_keywords: str = Form("[]"),
    competitor_count: int = Form(5)
):
    """
    Generate an SEO-optimized article based on the input parameters.
    This runs the full asynchronous workflow pipeline.
    """
    logger.info(f"Received generation request for title: '{title}', generate_images: {generate_images}")
    
    import re
    if not article_language:
        # Detect Arabic characters in the title
        if re.search(r'[\u0600-\u06FF]', title):
            article_language = "ar"
        else:
            article_language = "en"
        logger.info(f"Language not specified. Auto-detected '{article_language}' from title.")
    
    # Parse JSON strings
    try:
        keywords_list = json.loads(keywords) if keywords else []
    except json.JSONDecodeError:
        keywords_list = [k.strip() for k in keywords.split(",")]
        
    try:
        urls_list = json.loads(urls) if urls else []
    except json.JSONDecodeError:
        urls_list = []

    try:
        external_urls_list = json.loads(external_urls) if external_urls else []
    except json.JSONDecodeError:
        external_urls_list = []

    try:
        secondary_keywords_list = json.loads(secondary_keywords) if secondary_keywords else []
    except json.JSONDecodeError:
        secondary_keywords_list = [k.strip() for k in secondary_keywords.split(",")] if secondary_keywords else []


    # Handle file uploads
    # Ensure upload directory exists
    upload_dir = os.path.join(os.getcwd(), "output", "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    
    saved_logo_path = None
    saved_ref_path = None
    
    # Save Logo
    if logo_image and logo_image.filename:
        ext = logo_image.filename.split(".")[-1] if "." in logo_image.filename else "png"
        safe_filename = f"logo_{uuid.uuid4().hex[:8]}.{ext}"
        saved_logo_path = os.path.join(upload_dir, safe_filename)
        with open(saved_logo_path, "wb") as buffer:
            shutil.copyfileobj(logo_image.file, buffer)
            
    # Save Reference Image
    if reference_image and reference_image.filename:
        ext = reference_image.filename.split(".")[-1] if "." in reference_image.filename else "png"
        safe_filename = f"ref_{uuid.uuid4().hex[:8]}.{ext}"
        saved_ref_path = os.path.join(upload_dir, safe_filename)
        with open(saved_ref_path, "wb") as buffer:
            shutil.copyfileobj(reference_image.file, buffer)

    saved_guidelines_path = None
    if brand_voice_guidelines and brand_voice_guidelines.filename:
        ext = brand_voice_guidelines.filename.split(".")[-1] if "." in brand_voice_guidelines.filename else "pdf"
        safe_filename = f"guidelines_{uuid.uuid4().hex[:8]}.{ext}"
        saved_guidelines_path = os.path.join(upload_dir, safe_filename)
        with open(saved_guidelines_path, "wb") as buffer:
            shutil.copyfileobj(brand_voice_guidelines.file, buffer)

    saved_examples_path = None
    if brand_voice_examples and brand_voice_examples.filename:
        ext = brand_voice_examples.filename.split(".")[-1] if "." in brand_voice_examples.filename else "txt"
        safe_filename = f"examples_{uuid.uuid4().hex[:8]}.{ext}"
        saved_examples_path = os.path.join(upload_dir, safe_filename)
        with open(saved_examples_path, "wb") as buffer:
            shutil.copyfileobj(brand_voice_examples.file, buffer)

    # Initialize the centralized orchestrator
    work_dir = os.path.join(os.getcwd(), "output")
    controller = AsyncWorkflowController(work_dir=work_dir)
    
    # Prepare the initial state
    initial_state = {
        "input_data": {
            "title": title,
            "keywords": keywords_list,
            "article_language": article_language,
            "area": area,
            "urls": urls_list,
            "external_urls": external_urls_list,
            "generate_images": generate_images,
            "logo_image": saved_logo_path,
            "reference_image": saved_ref_path,
            "brand_voice_guidelines": saved_guidelines_path,
            "brand_voice_examples": saved_examples_path,
            "workflow_mode": workflow_mode,
            "tone": tone,
            "article_type": article_type,
            "pov": pov,
            "article_size": article_size,
            "brand_voice_description": brand_voice_description,
            "include_meta_keywords": include_meta_keywords,
            "include_conclusion": include_conclusion,
            "include_faq": include_faq,
            "include_tables": include_tables,
            "include_bullet_lists": include_bullet_lists,
            "include_comparison_blocks": include_comparison_blocks,
            "bold_key_terms": bold_key_terms,
            "num_images": num_images,
            "image_style": image_style,
            "image_size": image_size,
            "include_featured_image": include_featured_image,
            "custom_branding_frame": custom_branding_frame,
            "custom_keyword_density": custom_keyword_density,
            "secondary_keywords": secondary_keywords_list,
            "competitor_count": competitor_count
        }
    }
    
    try:
        # Run the entire workflow
        final_state = await controller.run_workflow(initial_state)
        
        # Extract the results from the final state
        slug = final_state.get("slug")
        output_dir = final_state.get("output_dir")
        
        # Load the final HTML output from the workflow's output directory
        html_content = ""
        markdown_content = ""
        
        if output_dir:
            html_path = os.path.join(output_dir, "page.html")
            md_path = os.path.join(output_dir, "article_final.md")
            
            if os.path.exists(html_path):
                with open(html_path, "r", encoding="utf-8") as f:
                    html_content = f.read()
                    
            if os.path.exists(md_path):
                with open(md_path, "r", encoding="utf-8") as f:
                    markdown_content = f.read()
                    
        # Fallback to memory if file read failed or was empty
        if not markdown_content:
            markdown_content = final_state.get("final_markdown", "")

        # --- SEO Metadata ---
        meta_dict = ArticleMetadata(
            title=final_state.get("title", ""),
            meta_title=final_state.get("meta_title", ""),
            meta_description=final_state.get("meta_description", ""),
            meta_keywords=final_state.get("meta_keywords", ""),
            article_schema=final_state.get("article_schema", {}),
            faq_schema=final_state.get("faq_schema", {})
        )

        # --- Image URLs ---
        base_url = str(request.base_url)
        # Ensure base_url doesn't end with a slash for clean joins later
        if base_url.endswith("/"):
            base_url = base_url[:-1]

        image_list = []
        for img in final_state.get("assets/images", []):
            rel_path = img.get("local_path", "")
            # Convert internal path to URL
            # e.g. "output/slug/images/img.webp" -> "http://.../output/slug/images/img.webp"
            if rel_path.startswith(os.getcwd()):
                rel_path = os.path.relpath(rel_path, os.getcwd())
            
            image_url = f"{base_url}/{rel_path.replace(os.sep, '/')}"
            
            image_list.append(ArticleImage(
                url=image_url,
                alt_text=img.get("alt_text", ""),
                image_type=img.get("image_type", "Standard"),
                section_id=img.get("section_id")
            ))

        return ArticleResponse(
            status="success",
            message=f"Article generated successfully. Slug: {slug}",
            slug=slug,
            output_dir=output_dir,
            html_content=html_content,
            markdown_content=markdown_content,
            metadata=meta_dict,
            images=image_list
        )
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Error during workflow execution: {error_details}")
        raise HTTPException(status_code=500, detail={"message": str(e), "traceback": error_details})
