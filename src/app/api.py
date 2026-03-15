import os
import json
import logging
from fastapi import FastAPI, HTTPException, BackgroundTasks, File, UploadFile, Form, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
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
    include_meta_keywords: bool = Form(True),
    generate_images: bool = Form(True),
    logo_image: UploadFile = File(None),
    reference_image: UploadFile = File(None)
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
            "include_meta_keywords": include_meta_keywords,
            "generate_images": generate_images,
            "logo_image_path": saved_logo_path,
            "image_frame_path": saved_ref_path
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
