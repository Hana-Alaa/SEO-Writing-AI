"""
Phase 5 - Orchestration Layer (Asynchronous)
- Fully asynchronous pipeline for high-performance article generation.
- Parallelizes section writing and image generation.
- Implements robust error handling, logging, and retries.
"""

import logging
import os
import time
import re
import json
import asyncio
from pathlib import Path
from urllib.parse import urlparse
from langdetect import detect  
from jinja2 import Template, StrictUndefined
import hashlib
import requests
from typing import Dict, Any, List, Optional, Callable, ClassVar
from collections import Counter
from langdetect import detect_langs, DetectorFactory
from services.image_generator import ImageGenerator, ImagePromptPlanner
from services.openrouter_client import OpenRouterClient
from schemas.input_validator import normalize_urls
from utils.injector import DataInjector
# from services.groq_client import GroqClient
# from services.gemini_client import GeminiClient
# from services.huggingface_client import HuggingFaceClient
from services.title_generator import TitleGenerator
from services.content_generator import OutlineGenerator, SectionWriter, Assembler, ContentGeneratorError
# from services.section_validator import SectionValidator
from services.image_inserter import ImageInserter
from services.meta_schema_generator import MetaSchemaGenerator
from services.article_validator import ArticleValidator
from utils.json_utils import recover_json
# from utils.observability import Observability
from utils.observability import ObservabilityTracker
from utils.seo_utils import enforce_meta_lengths
from utils.html_renderer import render_html_page
from urllib.parse import urlparse
from utils.workflow_logger import WorkflowLogger
BASE_DIR = Path(__file__).resolve().parents[1] 

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO, format="%(message)s")

DetectorFactory.seed = 0
PARALLEL_SECTIONS = False

class AsyncExecutor:
    """Executes async workflow steps with logging and retries."""
    def __init__(self, observer=None):
        self.observer = observer

    async def run_step(self, step_name: str, func: Callable[[Dict[str, Any]], Any], state: Dict[str, Any], retries: int = 0) -> Dict[str, Any]:
        """Runs an async step with retry logic."""
        attempt = 0
        while attempt <= retries:
            logger.info(f"--- Starting Step: {step_name} (Attempt {attempt + 1}/{retries + 1}) ---")
            
            # Use WorkflowLogger if available in state
            workflow_logger = state.get("workflow_logger")
            start_time = 0
            if workflow_logger:
                start_time = workflow_logger.start_step(step_name)
            else:
                start_time = time.time()
            
            try:
                # Capture state BEFORE execution for logging
                input_state = state.copy() if isinstance(state, dict) else state
                
                # Execute the async coordination step
                new_state = await func(state)
                
                if new_state is None:
                    new_state = state
                
                duration = time.time() - start_time
                
                if workflow_logger:
                    # Log step completion with inputs and outputs
                    workflow_logger.log_step_details(
                        step_name=step_name,
                        duration=duration,
                        input_data=input_state,
                        output_data=new_state
                    )
                    
                    # Collect token info if available in new_state (requires AI clients to report tokens)
                    tokens = new_state.get("last_step_tokens")
                    workflow_logger.end_step(
                        step_name=f"STEP_TOTAL: {step_name}",
                        start_time=start_time,
                        prompt=new_state.get("last_step_prompt"),
                        response=new_state.get("last_step_response"),
                        tokens=tokens
                    )
                
                if self.observer:
                    self.observer.log_workflow_step(step_name, duration)
                logger.info(f"--- Finished Step: {step_name} (Duration: {duration:.2f}s) ---")
                return {"status": "success", "step": step_name, "duration": duration, "data": new_state}
            
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"Error in step '{step_name}' attempt {attempt + 1}: {e}")
                
                if workflow_logger:
                    workflow_logger.log_step_details(
                        step_name=step_name,
                        duration=duration,
                        input_data=state,
                        error=str(e)
                    )
                
                attempt += 1
                if attempt <= retries:
                    await asyncio.sleep(1) # Simple backoff
                else:
                    return {"status": "error", "step": step_name, "duration": duration, "error": str(e), "data": state}
        
        return {"status": "error", "step": step_name, "error": "Max retries exceeded", "data": state}

class AsyncWorkflowController:
    """Central async orchestrator for SEO article generation."""

    def __init__(self, work_dir: str = "."):
        # AI Client
        self.ai_client = OpenRouterClient()
        self.observer = self.ai_client.observer
        # self.ai_client = GeminiClient()
        # self.ai_client = GroqClient()
        
        # self.ai_client = HuggingFaceClient(
        #     model="TheBloke/Llama-2-7B-Chat-GGML"
        # )
        self.enable_images = True
        self.work_dir = work_dir
        # self.executor = AsyncExecutor()
        self.executor = AsyncExecutor(self.ai_client.observer)
        self.image_prompt_planner = ImagePromptPlanner(
            ai_client=self.ai_client,
            template_path=BASE_DIR / "prompts" / "templates" / "06_image_planner.txt"
            
        )
        with open("prompts/templates/00_intent_classifier.txt", "r", encoding="utf-8") as f:
            self.intent_template = Template(f.read(), undefined=StrictUndefined)
        
        base_strategy = Path("prompts/templates/00_content_strategy_base.txt").read_text(encoding="utf-8")
        commercial_strategy = Path("prompts/templates/00_content_strategy_brand_commercial.txt").read_text(encoding="utf-8")
        informational_strategy = Path("prompts/templates/00_content_strategy_informational.txt").read_text(encoding="utf-8")
        comparison_strategy = Path("prompts/templates/00_content_strategy_comparison.txt").read_text(encoding="utf-8")
        
        self.content_strategy_templates = {
            "brand_commercial": Template(base_strategy + "\n\n" + commercial_strategy, undefined=StrictUndefined),
            "informational": Template(base_strategy + "\n\n" + informational_strategy, undefined=StrictUndefined),
            "comparison": Template(base_strategy + "\n\n" + comparison_strategy, undefined=StrictUndefined),
        }

        # Content generation services
        self.title_generator = TitleGenerator(self.ai_client)
        self.outline_gen = OutlineGenerator(self.ai_client)
        self.section_writer = SectionWriter(self.ai_client)
        self.assembler = Assembler(self.ai_client)
        # self.section_validator = SectionValidator(self.ai_client)
        self.image_inserter = ImageInserter()
        self.meta_schema = MetaSchemaGenerator(self.ai_client)
        self.article_validator = ArticleValidator(self.ai_client)
        
        # Image generator
        # api_key = os.getenv("STABILITY_API_KEY")
        self.image_client = ImageGenerator(
            ai_client=self.ai_client,
            save_dir=os.path.join(work_dir, "images"), 
        )

        # Semantic Memory Model
        try:
            from sentence_transformers import SentenceTransformer
            self.semantic_model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
            logger.info("Semantic Cross-Section Memory model loaded successfully.")
        except ImportError:
            self.semantic_model = None
            logger.warning("sentence-transformers not installed. Semantic memory disabled.")

    async def run_workflow(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Main entry point for the async pipeline."""
        self.observer.reset()
        # Initialize state keys
        state.setdefault("input_data", {})
        state.setdefault("seo_meta", {})
        state.setdefault("outline", [])
        state.setdefault("sections", {})
        state.setdefault("images", [])
        state.setdefault("final_output", {})
        state.setdefault("content_type", "informational")
        state.setdefault("brand_link_used", False)
        state.setdefault("used_internal_links", [])
        state.setdefault("used_external_links", []) 
        state["max_external_links"] = 3

        steps = [
            # ("semantic_layer", self._step_semantic_layer, 1),
            ("analysis_init", self._step_0_init, 0),
            ("brand_discovery", self._step_0_brand_discovery, 1),
            ("web_research", self._step_0_web_research, 1),
            ("serp_analysis", self._step_0_serp_analysis, 1),
            ("intent_title", self._step_0_intent_title, 0),
            ("style_analysis", self._step_0_style_analysis, 1),
            ("content_strategy", self._step_0_content_strategy, 3),
            ("outline_generation", self._step_1_outline, 1),

            ("content_writing", self._step_2_write_sections, 1),
            # ("image_prompting", self._step_4_generate_image_prompts, 0),
            # ("master_frame", self._step_4_1_generate_master_frame, 1),
            # ("image_generation", self._step_4_5_download_images, 2),
            # ("section_validation", self._step_4_validate_sections, 0),
            ("assembly", self._step_5_assembly, 0),
            # ("image_inserter", self._step_6_image_inserter, 0),
            ("meta_schema", self._step_7_meta_schema, 0),
            # ("article_validation", self._step_8_article_validation, 0),
            ("render_html", self._step_render_html, 0)
        ]

        for name, func, retries in steps:
            result = await self.executor.run_step(name, func, state, retries=retries)
            state = result.get("data", state)
            
            if result["status"] == "error":
                logger.error(f"Workflow stopped at critical step: {name}")
                break

        # Final Export
        if state.get("workflow_logger"):
            state["workflow_logger"].export_csv()

        return self._assemble_final_output(state)

    # ---------------- COORDINATION STEPS (ASYNC) ----------------
    async def _detect_intent_ai(self, raw_title: str, primary_keyword: str, state: Dict[str, Any] = None) -> str:

        prompt = self.intent_template.render(
            raw_title=raw_title,
            primary_keyword=primary_keyword
        )

        res = await self.ai_client.send(prompt, step="intent")
        content = res["content"]
        # Store metadata in state if provided
        if state is not None:
            state["last_step_prompt"] = res["metadata"]["prompt"]
            state["last_step_response"] = res["metadata"]["response"]
            state["last_step_tokens"] = res["metadata"]["tokens"]
            
        return content.strip()

    async def _step_0_init(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Setup unique directories and sluggification."""

        input_data = state.get("input_data", {})
        raw_title = input_data.get("title", "Untitled Article")
        keywords = input_data.get("keywords", [])
        if isinstance(keywords, str):
            keywords = [k.strip() for k in keywords.split(",") if k.strip()]
        
        primary_keyword = keywords[0] if keywords else raw_title
        user_lang = input_data.get("article_language")
        # article_language = user_lang if user_lang else (detect(raw_title) if raw_title else "en")
        # article_language = detect(raw_title) if raw_title else "en"
        article_language = self._resolve_article_language(raw_title, user_lang)
        area = input_data.get("area")
        state["area"] = area
        state["include_meta_keywords"] = input_data.get("include_meta_keywords", True)
        # area_neighborhoods will be populated by AI in _step_0_brand_discovery
        state["area_neighborhoods"] = []
        state["article_language"] = article_language
        state["primary_keyword"] = primary_keyword
        state["raw_title"] = raw_title
        state["keywords"] = keywords
        
        # New: Derive brand_url from the FIRST URL provided in the UI list
        urls = state.get("input_data", {}).get("urls", [])
        brand_url = urls[0].get("link") if urls else None
        state["brand_url"] = brand_url
        
        # PRE-INITIALIZE internal_resources with user-provided URLs immediately
        state["internal_resources"] = []
        seen_canons = set()
        
        if brand_url:
            state["internal_resources"].append({
                "link": brand_url,
                "text": "Homepage",
                "is_manual": True,
                "is_homepage": True
            })
            seen_canons.add(self._canon_url(brand_url))
        
        # Helper for junk slugs (restore manual link protection)
        junk_slugs = {'contact', 'about', 'login', 'signup', 'account', 'cart', 'checkout', 'privacy', 'terms', 'help', 'faq'}
        def is_junk_init(url_str):
            try:
                from urllib.parse import urlparse
                path = urlparse(url_str).path.lower().rstrip('/')
                return path.split('/')[-1] in junk_slugs
            except: return False

        for u in urls:
            link = u.get("link", "")
            if link:
                state["internal_resources"].append({
                    "link": link, 
                    "text": u.get("text") or "Internal Resource",
                    "is_manual": True  # Mark as manual to avoid junk filtering
                })
                seen_canons.add(self._canon_url(link))

        state["image_frame_path"] = input_data.get("image_frame_path") or input_data.get("image_template_path")
        state["logo_image_path"] = input_data.get("logo_image_path")
        state["brand_visual_style"] = "" # Removed from UI, setting to empty
        # keep input_data in sync for downstream steps
        state.setdefault("input_data", {})
        state["input_data"]["article_language"] = article_language
        state["input_data"]["keywords"] = keywords

        # Generate slug and directory
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        slug_base = self._sluggify(primary_keyword)
        slug = f"{slug_base}_{timestamp}"
        state["slug"] = slug
        
        output_dir = os.path.join(self.work_dir, slug)
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize WorkflowLogger
        state["workflow_logger"] = WorkflowLogger(output_dir)
        state["workflow_logger"].log_event("Initialization", {
            "title": raw_title,
            "language": article_language,
            "primary_keyword": primary_keyword,
            "output_dir": output_dir
        })
        
        state["output_dir"] = output_dir
        state["used_phrases"] = []
        
        # Initialize external link controls
        state["max_external_links"] = 6
        state["blocked_external_domains"] = set()
        state["allowed_external_domains"] = set()
        state["used_external_links"] = []
        state["used_all_urls"] = set()

        return state

    async def _step_0_brand_discovery(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep brand discovery:
        1. Crawls the homepage to discover all internal links.
        2. Scores each link by relevance to the primary keyword.
        3. Fetches the top 3-5 most relevant subpages.
        4. Indexes raw page text by URL.
        5. Uses AI to extract a factual brand context from the most relevant pages.
        """
        brand_url = state.get("brand_url")
        if not brand_url:
            urls = state.get("input_data", {}).get("urls", [])
            if urls:
                brand_url = urls[0].get("link")
        
        if not brand_url or not brand_url.startswith("http"):
            logger.info("Skipping brand discovery: No valid brand_url found.")
            return state

        primary_keyword = state.get("primary_keyword", "").lower()
        kw_tokens = [t for t in primary_keyword.split() if len(t) > 2]

        logger.info(f"Starting deep brand discovery for: {brand_url}")
        domain = self._domain(brand_url)
        
        try:
            import requests
            from bs4 import BeautifulSoup
            from urllib.parse import urljoin

            # Preservation: Modern Browser User-Agent for better crawl success
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

            # Preservation: Advanced Logo Discovery (Fixes SVG and Nested Branding)
            await self._discover_logo_and_colors(brand_url, state)

            def fetch_text(url: str) -> str:
                """
                Fetch a URL and return clean, structured body text.
                Groups paragraphs under their heading context to give the AI
                meaningful, specific content rather than navigation/UI noise.
                """
                try:
                    r = requests.get(url, timeout=10, headers=headers)
                    if r.status_code != 200:
                        return ""
                    s = BeautifulSoup(r.text, "html.parser")

                    # Strip ALL noise tags aggressively
                    for tag in s(["nav", "footer", "script", "style", "header",
                                   "aside", "form", "button", "iframe", "svg",
                                   "noscript", "meta", "link"]):
                        tag.decompose()

                    # Find the main content area if available
                    main = s.find("main") or s.find(id="main") or s.find(class_="content") or s

                    # Build structured text: "Heading\nParagraph\nParagraph\n..."
                    blocks = []
                    current_heading = ""
                    current_paras = []

                    for tag in main.find_all(["h1", "h2", "h3", "p", "li"]):
                        text = tag.get_text(separator=" ", strip=True)
                        # Skip very short items (buttons, labels, menu items)
                        if len(text) < 40:
                            continue

                        if tag.name in ("h1", "h2", "h3"):
                            # Save previous group
                            if current_paras:
                                group = (f"## {current_heading}\n" if current_heading else "") + "\n".join(current_paras)
                                blocks.append(group)  # no longer capping each group to 800 chars
                            current_heading = text
                            current_paras = []
                        else:
                            current_paras.append(text)

                    # Save the last group
                    if current_paras:
                        group = (f"## {current_heading}\n" if current_heading else "") + "\n".join(current_paras)
                        blocks.append(group)

                    # Return up to 6000 characters to provide sufficient context but avoid massive token usage
                    return "\n\n".join(blocks)[:6000]

                except Exception as ex:
                    logger.warning(f"Failed to fetch {url}: {ex}")
                    return ""

            def relevance_score(url: str, anchor: str) -> int:
                """Score a URL by how relevant it appears to the primary keyword."""
                text = (url + " " + anchor).lower()
                score = 0
                
                # Massive boost for exact primary keyword match
                if primary_keyword.lower() in text:
                    score += 20
                
                # Heavy boost for individual token matches
                score += sum(3 for t in kw_tokens if t in text and len(t) > 2)
                
                # Minor boost for general structure pages if they relate to the brand persona
                for boost_word in ["about", "work", "portfolio", "project", "offer", "services", "contact", "faq", "events"]:
                    if boost_word in text:
                        score += 5 # Increased boost for structural pages to ensure they pass filters
                return score

            # --- Step 1: Crawl homepage and discover all internal links ---
            homepage_html = requests.get(brand_url, timeout=15, headers=headers)
            if homepage_html.status_code != 200:
                logger.warning(f"Brand discovery failed (status {homepage_html.status_code})")
                return state

            homepage_soup = BeautifulSoup(homepage_html.text, "html.parser")
            discovered_links = {}  # canon_url -> (anchor_text, score)
            
            # Helper to clean anchor text
            def clean_anchor(text: str) -> str:
                import re
                # Strip date patterns like 12/23/2021, 2022, 02/25/2021
                text = re.sub(r'\b\d{2}/\d{2}/\d{4}\b', '', text).strip()
                text = re.sub(r'\b(19|20)\d{2}\b', '', text).strip()
                # Collapse extra spaces
                text = re.sub(r'\s+', ' ', text).strip()
                return text
            
            GENERIC_ANCHORS = {"click here", "read more", "learn more", "lets talk", 
                               "let's talk", "contact us", "see all", "اقرأ أكثر", "انقر هنا"}

            # Helper to process a soup for links
            def extract_links(soup, base_url):
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    anchor_raw = a.get_text(strip=True)
                    anchor = clean_anchor(anchor_raw)
                    full_url = urljoin(base_url, href)

                    if self._domain(full_url) != domain:
                        continue
                    canon = self._canon_url(full_url)
                    if canon == self._canon_url(brand_url):
                        continue
                    if not anchor or len(anchor) < 3 or len(anchor) > 80:
                        continue
                    # Skip if anchor is generic after cleaning
                    if anchor.lower() in GENERIC_ANCHORS:
                        continue

                    score = relevance_score(canon, anchor)
                    if canon not in discovered_links or score > discovered_links[canon][1]:
                        discovered_links[canon] = (anchor, score)

            extract_links(homepage_soup, brand_url)

            # --- Anchor Deduplication & Service Boosting ---
            filtered_links = {}
            for canon, (anchor, score) in discovered_links.items():
                # Boost score for services/products ONLY IF it's already relevant to the topic
                if score > 0:
                    if any(k in canon.lower() for k in ["service", "product", "solution", "خدمات", "منتجات", "برامج"]):
                        score += 2
                
                # Deduplicate by anchor text: keep highest score for a given anchor
                if anchor not in filtered_links or score > filtered_links[anchor][1]:
                    filtered_links[anchor] = (canon, score)
            
            # Re-map discovered_links to the deduplicated set
            discovered_links = {canon: (anchor, score) for anchor, (canon, score) in filtered_links.items()}

            # DEEP DISCOVERY: Identify "Hub" pages (Services, Solutions) and crawl them too
            hub_keywords = ["service", "solution", "product", "offer", "خدمات", "حلول"]
            hub_links = []
            for canon, (anchor, score) in discovered_links.items():
                if any(k in canon.lower() or k in anchor.lower() for k in hub_keywords):
                    if score >= 0: # Crawl hubs even if they lack the exact keyword, to find subpages
                        hub_links.append(canon)
            
            # Limit to top 2 hubs to avoid massive crawl
            for hub_url in hub_links[:2]:
                try:
                    logger.info(f"Deep crawling hub page: {hub_url}")
                    hub_html = requests.get(hub_url, timeout=10, headers=headers)
                    if hub_html.status_code == 200:
                        extract_links(BeautifulSoup(hub_html.text, "html.parser"), hub_url)
                except Exception as e:
                    logger.warning(f"Failed deep crawl for {hub_url}: {e}")

            # --- Step 2: Sort by relevance, pick top 10 subpages ---
            sorted_links = sorted(discovered_links.items(), key=lambda x: x[1][1], reverse=True)
            top_links = sorted_links[:10]

            logger.info(f"Top relevant brand pages: {[l[0] for l in top_links]}")

            # --- Step 3: Store internal resources (for linking in the outline) ---
            if "internal_resources" not in state:
                state["internal_resources"] = []

            junk_slugs = {'contact', 'about', 'login', 'signup', 'account', 'cart', 'checkout', 'privacy', 'terms', 'help', 'faq'}
            def is_junk(url_str):
                try:
                    path = urlparse(url_str).path.lower().rstrip('/')
                    return path.split('/')[-1] in junk_slugs
                except: return False

            # Existing manual link protection is already handled in _step_0_init
            seen_canons = {self._canon_url(r['link']) for r in state["internal_resources"] if r.get("link")}

            added_count = 0
            for canon, (anchor, score) in sorted_links:
                # Removed strict filtering by relevance (score < 1). 
                # allow structural pages to be collected so AI can decide if they are useful.

                if canon not in seen_canons:
                    state["internal_resources"].append({"link": canon, "text": anchor})
                    seen_canons.add(canon)
                    added_count += 1
                if added_count >= 30: # Increased from 10 to provide more options
                    break

            logger.info(f"Discovered {added_count} brand resources.")

            # --- Step 4: Fetch and index the content of the top relevant subpages ---
            brand_pages_index = {}  # url -> raw text

            # Always include homepage
            homepage_text_raw = fetch_text(brand_url)
            if homepage_text_raw:
                brand_pages_index[brand_url] = homepage_text_raw[:2500] # Increased limit

            for canon, (anchor, score) in top_links:
                page_text = fetch_text(canon)
                if page_text:
                    brand_pages_index[canon] = page_text[:2500] # Increased limit
                    logger.info(f"Indexed brand page: {canon} (score={score}, ~{len(page_text)} chars)")

            state["brand_pages_index"] = brand_pages_index

            # --- Step 5: Build AI Brand Context from the most relevant pages ---
            # Cap increased to allow more context from the 30 pages
            combined_text = "\n\n".join(
                f"[Page: {url}]\n{text}"
                for url, text in brand_pages_index.items()
            )[:12000]

            if combined_text:
                context_prompt = f"""You are a Brand Intelligence Analyst.

        Below is real text scraped from multiple pages of a company's website.
        The article we are writing is STRICTLY about: "{primary_keyword}"

        Website Content:
        \"\"\"
        {combined_text}
        \"\"\"

        Your task:
        1. Read through all pages and extract detailed factual information ONLY related to: "{primary_keyword}".
        2. Write a COMPREHENSIVE FACT SHEET (not a brief summary). Include:
           - Specific services, sub-services, or features they offer related to this topic.
           - Exact processes, methodologies, or steps they outline.
           - Specific technologies, tools, platforms, or frameworks they use.
           - Unique selling propositions (USPs) and what differentiates their approach.
           - Target audience, industries served, or specific client types.
           - Any statistics, numbers, guarantees (SLAs), or case study outcomes mentioned.
        3. CRITICAL RULE: DO NOT compress the information into a single paragraph. Use bullet points and detailed notes so we don't lose the richness of the data. 
        4. CRITICAL RULE: IGNORE completely any other services, products, or departments mentioned in the text that are not "{primary_keyword}".
        5. Only use information found in the text above. Do NOT invent or assume anything.

        Write the detailed fact sheet now:"""

                res = await self.ai_client.send(context_prompt, step="brand_discovery")
                brand_content = res["content"]
                metadata = res["metadata"]
                
                if state.get("workflow_logger"):
                    state["workflow_logger"].log_ai_call(
                        step_name="brand_discovery",
                        prompt=context_prompt,
                        response=brand_content,
                        tokens=metadata.get("tokens", {}),
                        duration=metadata.get("duration", 0)
                    )
                
                state["last_step_prompt"] = metadata["prompt"]
                state["last_step_response"] = metadata["response"]
                state["last_step_tokens"] = metadata["tokens"]
                
                brand_data = recover_json(brand_content)
                if brand_data and isinstance(brand_data, dict) and brand_data.get("summary"):
                    state["brand_context"] = brand_data["summary"].strip()
                    logger.info(f"Brand Context extracted successfully from JSON:\n{state['brand_context']}")
                elif brand_content and len(brand_content) > 20:
                    state["brand_context"] = brand_content.strip()
                    logger.info(f"Brand Context extracted successfully:\n{state['brand_context']}")
                else:
                    state["brand_context"] = ""
            else:
                state["brand_context"] = ""

            # --- Step 6: AI-Powered Local Neighborhood Discovery ---
            area = state.get("area")
            if area:
                neighborhood_prompt = f"""You are a Local SEO expert.
        Your task: List the top 8-10 most well-known neighborhoods, districts, or business zones in "{area}" that are most relevant to the service: "{primary_keyword}".

        Rules:
        - Output ONLY a valid JSON array of strings. No explanations, no markdown, just the array.
        - Use the local language (Arabic if the city is Arab, etc.)
        - Focus on areas where businesses would search for this service.
        - Example output: ["العليا", "النخيل", "الملقا", "الروضة", "الزهراء", "الملز"]

        Output the JSON array now:"""
                try:
                    neighborhoods_res = await self.ai_client.send(neighborhood_prompt, step="local_seo")
                    neighborhoods_raw = neighborhoods_res["content"]
                    metadata = neighborhoods_res["metadata"]
                    
                    if state.get("workflow_logger"):
                        state["workflow_logger"].log_ai_call(
                            step_name="local_neighborhoods",
                            prompt=neighborhood_prompt,
                            response=neighborhoods_raw,
                            tokens=metadata.get("tokens", {}),
                            duration=metadata.get("duration", 0)
                        )
                        
                    state["last_step_prompt"] = metadata["prompt"]
                    state["last_step_response"] = metadata["response"]
                    state["last_step_tokens"] = metadata["tokens"]

                    # Parse the JSON array from AI response
                    import re as _re
                    match = _re.search(r'\[.*?\]', neighborhoods_raw, _re.DOTALL)
                    if match:
                        import json as _json
                        neighborhoods = _json.loads(match.group(0))
                        if isinstance(neighborhoods, list) and len(neighborhoods) > 0:
                            state["area_neighborhoods"] = [str(n) for n in neighborhoods if n]
                            logger.info(f"AI discovered {len(state['area_neighborhoods'])} neighborhoods for '{area}': {state['area_neighborhoods']}")
                        else:
                            state["area_neighborhoods"] = []
                    else:
                        state["area_neighborhoods"] = []
                except Exception as ne:
                    logger.warning(f"Neighborhood discovery failed: {ne}")
                    state["area_neighborhoods"] = []
                
        except Exception as e:
            logger.error(f"Error during brand discovery: {e}", exc_info=True)
            state["brand_context"] = ""
            state["brand_pages_index"] = {}
            
        return state

    async def _discover_logo_and_colors(self, url: str, state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extracts company logo URL and dominant colors from a website.
        """
        try:
            import requests
            from bs4 import BeautifulSoup
            from urllib.parse import urljoin
            from PIL import Image
            from io import BytesIO
            import numpy as np
            import hashlib
            import os

            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            r = requests.get(url, timeout=10, headers=headers)
            if r.status_code != 200:
                return None
                
            soup = BeautifulSoup(r.text, "html.parser")
            logo_url = None

            # 1. Search for a descriptive brand logo in <img> tags (Prioritize Header/Logo classes)
            logo_candidates = soup.find_all("img", alt=lambda x: x and 'logo' in x.lower()) + \
                             soup.find_all("img", id=lambda x: x and 'logo' in x.lower()) + \
                             soup.find_all("img", class_=lambda x: x and 'logo' in x.lower())
            
            # Filter and rank candidates (Largest image with 'logo' is usually the main one)
            best_img = None
            max_res = 0
            for img in logo_candidates:
                src = img.get("src")
                if not src: continue
                # Basic resolution estimation from width/height params if present
                try:
                    w = int(img.get("width", 0)) or 1
                    h = int(img.get("height", 0)) or 1
                except: w, h = 1, 1
                if w * h > max_res:
                    max_res = w * h
                    best_img = img

            if best_img:
                logo_url = urljoin(url, best_img.get("src"))
                logger.info(f"Logo found via image candidate search: {logo_url}")

            # 2. Search for <a> links with 'logo' or 'brand' classes/ids (Handles SVG or text-logos)
            if not logo_url:
                # Look for links in header/nav first
                header = soup.find("header") or soup.find("nav") or soup.find("div", id=lambda x: x and 'header' in x.lower())
                search_scope = header if header else soup
                
                logo_link = search_scope.find("a", class_=lambda x: x and any(k in x.lower() for k in ['logo', 'brand'])) or \
                            search_scope.find("a", id=lambda x: x and any(k in x.lower() for k in ['logo', 'brand']))
                
                if logo_link:
                    # Check for img inside
                    inner_img = logo_link.find("img")
                    if inner_img:
                        logo_url = urljoin(url, inner_img.get("src"))
                        logger.info(f"Logo found via nested link image: {logo_url}")
                    else:
                        # Check for SVG inside
                        inner_svg = logo_link.find("svg")
                        if inner_svg:
                            # We found an inline SVG! We'll mark it for special handling or try to find its source if it's an <use>
                            logo_url = "inline_svg"
                            state["inline_svg_content"] = str(inner_svg)
                            logger.info("Found inline SVG logo in header link.")

            # 3. Fallback to OpenGraph logo (usually high quality)
            if not logo_url:
                og_image = soup.find("meta", property="og:image") or soup.find("meta", attrs={"name": "og:image"})
                if og_image:
                    logo_url = og_image.get("content")
                    logger.info(f"Logo found via OpenGraph: {logo_url}")

            # 4. Fallback: Search for FIRST SVG in header, OR look in FOOTER
            if not logo_url:
                header = soup.find("header") or soup.find("nav")
                if header:
                    svg = header.find("svg")
                    if svg:
                        logo_url = "inline_svg"
                        state["inline_svg_content"] = str(svg)
                        logger.info("Found first inline SVG in header as logo.")
            
            # New Step: Footer Search (High Priority Fallback requested by user)
            if not logo_url:
                footer = soup.find("footer") or soup.find("div", id=lambda x: x and 'footer' in x.lower()) or \
                         soup.find("div", class_=lambda x: x and 'footer' in x.lower())
                if footer:
                    # Look for img with logo keyword in footer
                    footer_logo = footer.find("img", alt=lambda x: x and 'logo' in x.lower()) or \
                                  footer.find("img", class_=lambda x: x and 'logo' in x.lower())
                    if footer_logo:
                        logo_url = urljoin(url, footer_logo.get("src"))
                        logger.info(f"Logo found via Footer image search: {logo_url}")
                    else:
                        # Look for SVG in footer
                        footer_svg = footer.find("svg")
                        if footer_svg:
                            logo_url = "inline_svg"
                            state["inline_svg_content"] = str(footer_svg)
                            logger.info("Found inline SVG logo in footer.")
                        else:
                            # Look for brand link in footer
                            footer_link = footer.find("a", class_=lambda x: x and any(k in x.lower() for k in ['logo', 'brand']))
                            if footer_link:
                                footer_img = footer_link.find("img")
                                if footer_img:
                                    logo_url = urljoin(url, footer_img.get("src"))
                                    logger.info(f"Logo found via footer brand link image: {logo_url}")

            # 5. Last fallback: manifest icons or high-res favicons
            if not logo_url:
                icon_link = soup.find("link", rel=lambda x: x and 'icon' in x.lower()) or \
                          soup.find("link", attrs={"rel": "shortcut icon"})
                if icon_link:
                    logo_url = urljoin(url, icon_link.get("href"))
                    logger.info(f"Logo found via link icon: {logo_url}")

            # 6. Extreme Final Fallback: Google Favicon service or similar
            if not logo_url:
                from urllib.parse import urlparse
                domain = urlparse(url).netloc
                logo_url = f"https://www.google.com/s2/favicons?sz=128&domain={domain}"
                logger.info(f"Logo fallback to Google Favicon service: {logo_url}")

            if not logo_url:
                return None

            # Download and Process Logo
            img_data = None
            is_svg = False
            
            if logo_url == "inline_svg":
                img_data = state.get("inline_svg_content", "").encode('utf-8')
                is_svg = True
            else:
                lr = requests.get(logo_url, timeout=5, headers=headers)
                if lr.status_code == 200:
                    img_data = lr.content
                    if logo_url.lower().endswith(".svg") or b"<svg" in img_data[:100].lower():
                        is_svg = True

            if not img_data:
                return None

            # Save logo to work directory
            output_dir = state.get("output_dir", self.work_dir)
            ext = ".svg" if is_svg else ".png"
            logo_filename = f"brand_logo_{hashlib.md5(url.encode()).hexdigest()[:8]}{ext}"
            logo_local_path = os.path.join(output_dir, "images", logo_filename)
            os.makedirs(os.path.dirname(logo_local_path), exist_ok=True)
            
            if is_svg:
                with open(logo_local_path, "wb") as f:
                    f.write(img_data)
                
                # Extract colors from SVG text via regex
                svg_text = img_data.decode('utf-8', errors='ignore')
                hex_colors = re.findall(r'#(?:[0-9a-fA-F]{3}){1,2}', svg_text)
                # Filter out obvious white/black if others exist
                meaningful_colors = [c for c in hex_colors if c.lower() not in ['#ffffff', '#000000', '#fff', '#000']]
                brand_colors = meaningful_colors if meaningful_colors else (hex_colors[:3] if hex_colors else ["#333333"])
                
                return {
                    "logo_url": logo_url,
                    "local_path": logo_local_path,
                    "colors": brand_colors,
                    "is_svg": True
                }
            else:
                img = Image.open(BytesIO(img_data)).convert("RGBA")
                img.save(logo_local_path, "PNG")
                # ... rest of color extraction ...

                # Extract Dominant Colors
                filtered_colors = self._extract_colors_from_image(logo_local_path)

                return {
                    "logo_path": logo_local_path,
                    "colors": filtered_colors
                }

        except Exception as e:
            logger.warning(f"Logo discovery failed: {e}")
        return None

    def _extract_colors_from_image(self, image_path: str) -> List[str]:
        """Helper to extract dominant colors from a local image file (Supports Raster and SVG)."""
        if not image_path or not os.path.exists(image_path):
            return []
            
        try:
            # Handle SVGs via regex color parsing
            if image_path.lower().endswith(".svg"):
                with open(image_path, "r", encoding="utf-8", errors="ignore") as f:
                    svg_text = f.read()
                    hex_colors = re.findall(r'#(?:[0-9a-fA-F]{3}){1,2}', svg_text)
                    # Filter out noise (white/black)
                    meaningful = [c.lower() for c in hex_colors if c.lower() not in ['#ffffff', '#000000', '#fff', '#000']]
                    if meaningful:
                        # Convert to rgb() format for prompt consistency
                        rgb_colors = []
                        for hc in meaningful[:3]:
                            hc = hc.lstrip('#')
                            if len(hc) == 3: hc = ''.join([c*2 for c in hc])
                            r, g, b = int(hc[0:2], 16), int(hc[2:4], 16), int(hc[4:6], 16)
                            rgb_colors.append(f"rgb({r},{g},{b})")
                        return rgb_colors
                return ["#333333"] # Fallback

            from PIL import Image
            with Image.open(image_path) as img:
                img = img.convert("RGBA")
                img_small = img.resize((50, 50))
                colors = img_small.getcolors(50 * 50)
                filtered_colors = []
                if colors:
                    for count, color in sorted(colors, reverse=True):
                        if color[3] < 50: continue # Skip transparent
                        if sum(color[:3]) > 720 or sum(color[:3]) < 40: continue # Skip white/black
                        filtered_colors.append(f"rgb({color[0]},{color[1]},{color[2]})")
                        if len(filtered_colors) >= 3: break
                return filtered_colors
        except Exception as e:
            logger.error(f"Color extraction failed for {image_path}: {e}")
            return []

    async def _step_0_web_research(self, state):

        primary_keyword = state["primary_keyword"]
        area = state.get("area")
        search_query = f"{primary_keyword} in {area}" if state.get("area") else primary_keyword

        with open("prompts/templates/seo_web_research.txt") as f:
            template = Template(f.read())

        research_prompt = template.render(
            primary_keyword=search_query
        )

        res = await self.ai_client.send_with_web(
            prompt=research_prompt,
            max_results=3
        )
        raw = res["content"]
        metadata = res["metadata"]
        
        if state.get("workflow_logger"):
            state["workflow_logger"].log_ai_call(
                step_name="web_research",
                prompt=research_prompt,
                response=raw,
                tokens=metadata.get("tokens", {}),
                duration=metadata.get("duration", 0)
            )
            
        state["last_step_prompt"] = metadata["prompt"]
        state["last_step_response"] = metadata["response"]
        state["last_step_tokens"] = metadata.get("tokens", {})

        logger.info(f"RAW SERP RESPONSE:\n{raw}")

        clean_raw = raw.strip()

        # remove markdown wrapping if exists
        # if clean_raw.startswith("```"):
        #     clean_raw = clean_raw.replace("```json", "").replace("```", "").strip()
        
        clean_raw = clean_raw.strip()
        clean_raw = re.sub(r"```json|```", "", clean_raw).strip()

        serp_data = recover_json(clean_raw) or {}

        if not serp_data.get("top_results"):
            raise RuntimeError("SERP returned no top results")

        state["serp_data"] = serp_data
        state["seo_intelligence"] = serp_data

        logger.info(f"SERP stored successfully: {len(serp_data.get('top_results', []))} results")

        return state

    async def _step_0_serp_analysis(self, state):

        serp_data = state.get("serp_data", {})
        primary_keyword = state.get("primary_keyword")

        with open("prompts/templates/seo_serp_analysis.txt") as f:
            template = Template(f.read())
        
        paa_raw = serp_data.get("paa_questions", [])
        paa_clean = []
        for q in paa_raw[:10]:
            if isinstance(q, dict):
                paa_clean.append(q.get("question", ""))
            elif isinstance(q, str):
                paa_clean.append(q)

        light_serp = {
            "paa": paa_clean,
            "lsi": serp_data.get("lsi_keywords", [])[:20],
            "related": serp_data.get("related_searches", [])[:15],
            "titles_pattern": [
                r.get("title", "")[:120]
                for r in serp_data.get("top_results", [])
                if isinstance(r, dict)
            ][:5]
        }

        analysis_prompt = template.render(
            primary_keyword=primary_keyword,
            serp_data=json.dumps(light_serp)
        )


        res = await self.ai_client.send(
            analysis_prompt,
            step="serp_analysis"
        )
        raw = res["content"]
        metadata = res["metadata"]
        
        if state.get("workflow_logger"):
            state["workflow_logger"].log_ai_call(
                step_name="serp_analysis",
                prompt=analysis_prompt,
                response=raw,
                tokens=metadata.get("tokens", {}),
                duration=metadata.get("duration", 0)
            )
            
        state["last_step_tokens"] = metadata.get("tokens", {})

        serp_insights = recover_json(raw) or {}
        serp_insights["semantic_assets"] = {
            "paa_questions": serp_data.get("paa_questions", []),
            "lsi_keywords": serp_data.get("lsi_keywords", []),
            "related_searches": serp_data.get("related_searches", []),
            "autocomplete_suggestions": serp_data.get("autocomplete_suggestions", [])
        }

        if "strategic_intelligence" not in serp_insights:
            serp_insights["strategic_intelligence"] = {}
            
        if not serp_insights["strategic_intelligence"].get("keyword_clusters"):
            # Robust fallback: use LSI and related keywords if AI fails to cluster
            # Ensure all fallback keywords are strings before deduplicating
            raw_fallback = [primary_keyword] + lsi[:5] + related[:5]
            safe_fallback = []
            for kw in raw_fallback:
                if isinstance(kw, dict):
                    safe_kw = kw.get("keyword") or kw.get("text", str(kw))
                    safe_fallback.append(str(safe_kw))
                else:
                    safe_fallback.append(str(kw))

            serp_insights["strategic_intelligence"]["keyword_clusters"] = [
                {
                    "cluster_name": "Semantic Cluster (Fallback)",
                    "keywords": list(dict.fromkeys(safe_fallback)) # Remove duplicates
                }
            ]

        # existing = state.get("seo_intelligence", {})
        # existing.update(serp_insights)
        # state["seo_intelligence"] = existing

        state["seo_intelligence"] = {
           "serp_raw": state.get("serp_data", {}),
            "strategic_analysis": serp_insights
        }
        return state

    async def _step_0_intent_title(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Classify user intent and refine the title via AI."""
        raw_title = state.get("raw_title") or "Untitled"
        primary_keyword = state.get("primary_keyword") or raw_title
        article_language = state.get("article_language") or "en"
        area = state.get("area")
        serp_data = state.get("serp_data", {})

        top_titles = [
            r.get("title", "")
            for r in serp_data.get("top_results", [])
            if isinstance(r, dict)
        ][:5]

        cta_styles = [
            r.get("cta_style", "")
            for r in serp_data.get("top_results", [])
            if isinstance(r, dict)
        ]

        res = await self.title_generator.generate(
            raw_title=raw_title,
            primary_keyword=primary_keyword,
            article_language=article_language,
            serp_titles=top_titles,
            serp_cta_styles=cta_styles,
            area=area
        )
        
        if state.get("workflow_logger"):
            state["workflow_logger"].log_ai_call(
                step_name="intent_title",
                prompt=res.get("prompt"),
                response=res,  # Log the whole dict as JSON
                tokens=res.get("metadata", {}),
                duration=res.get("metadata", {}).get("duration", 0)
            )

        intent_raw = res.get("intent", "Informational")
        optimized_title = res.get("optimized_title", raw_title)

        # Logic for local SEO intent refinement
        serp_confirmed = (
            state.get("seo_intelligence", {})
                .get("strategic_analysis", {})
                .get("intent_analysis", {})
                .get("confirmed_intent")
        )
        confidence = (
            state.get("seo_intelligence", {})
                .get("strategic_analysis", {})
                .get("intent_analysis", {})
                .get("intent_confidence_score", 0)
        )

        if confidence > 0.6 and serp_confirmed:
            intent_raw = serp_confirmed

        intent_normalized = intent_raw.strip().lower()
        state["intent"] = intent_normalized

        if any(x in intent_normalized for x in ["commercial", "transactional"]):
            state["content_type"] = "brand_commercial"
        elif any(x in intent_normalized for x in ["comparison", "comparative"]):
            state["content_type"] = "comparison"
        else:
            state["content_type"] = "informational"

        state["input_data"]["title"] = optimized_title
        
        # Finally, perform the classifier step for logging
        await self._detect_intent_ai(raw_title, primary_keyword, state=state)
        
        return state

    async def _step_0_style_analysis(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Analyzes the reference image if provided to determine the brand's visual style."""
        input_data = state.get("input_data", {})
        ref_path = input_data.get("logo_reference_path")
        logo_path = input_data.get("logo_path")
        # ref_path = state.get("input_data", {}).get("logo_reference_path")

        state["brand_visual_style"] = ""

        if ref_path and isinstance(ref_path, str) and os.path.exists(ref_path):
            logger.info(f"Analyzing brand style from reference: {ref_path}")
            try:
                style_desc = await self.ai_client.describe_image_style(ref_path)
                state["brand_visual_style"] = style_desc
            except Exception as e:
                logger.error(f"Failed to analyze reference image: {e}")
                state["brand_visual_style"] = "Professional, modern corporate identity, clean lighting"
        else:
            logger.info("No reference image provided. Using generic professional visual style.")
            
        return state

    async def _step_0_content_strategy(self, state: Dict[str, Any]) -> Dict[str, Any]:
        primary_keyword = state.get("primary_keyword")
        intent = state.get("intent")
        seo_intelligence = state.get("seo_intelligence", {})
        content_type = state.get("content_type")
        area = state.get("area") or "Global"
        full_intel = seo_intelligence.get("strategic_analysis", {})

        intent_layer = full_intel.get("intent_analysis", {})
        structural_layer = full_intel.get("structural_intelligence", {})
        strategic_layer = full_intel.get("strategic_intelligence", {})

        clusters = strategic_layer.get("keyword_clusters", [])
        if not clusters:
            semantic = full_intel.get("semantic_assets", {})
            lsi = semantic.get("lsi_keywords", [])
            related = semantic.get("related_searches", [])
            
            # Ensure all fallback keywords are strings before deduplicating
            raw_fallback = [primary_keyword] + lsi[:5] + related[:5]
            safe_fallback = []
            for kw in raw_fallback:
                if isinstance(kw, dict):
                    safe_kw = kw.get("keyword") or kw.get("text", str(kw))
                    safe_fallback.append(str(safe_kw))
                else:
                    safe_fallback.append(str(kw))

            clusters = [{
                "cluster_name": "Semantic Keywords Cluster (Safety Fallback)",
                "keywords": list(dict.fromkeys(safe_fallback))
            }]

        template = self.content_strategy_templates.get(
            content_type,
            self.content_strategy_templates["informational"]
        )

        prompt = template.render(
            primary_keyword=primary_keyword,
            intent=intent,
            serp_intent_analysis=json.dumps(intent_layer),
            serp_structural_intelligence=json.dumps(structural_layer),
            serp_strategic_intelligence=json.dumps(strategic_layer),
            keyword_clusters=json.dumps(clusters),
            content_type=content_type,
            area=area
        )

        final_data = None
        for attempt in range(3):
            res = await self.ai_client.send(prompt, step="content_strategy")
            raw = res["content"]
            metadata = res["metadata"]

            if state.get("workflow_logger"):
                state["workflow_logger"].log_ai_call(
                    step_name="content_strategy",
                    prompt=metadata.get("prompt"),
                    response=raw,
                    tokens=metadata.get("tokens"),
                    duration=metadata.get("duration", 0)
                )

            state["last_step_prompt"] = metadata["prompt"]
            state["last_step_response"] = metadata["response"]
            state["last_step_tokens"] = metadata["tokens"]

            if not raw:
                logger.error("Content Strategy AI returned empty response")
                state["content_strategy"] = {}
                return state
            
            json_text = self._extract_first_json_object(raw)
            parsed = recover_json(json_text)

            if isinstance(parsed, dict) and parsed:
                normalized = self._normalize_content_strategy(
                    parsed, primary_keyword, content_type, area
                )
                if self._is_valid_content_strategy(normalized):
                    final_data = normalized
                    break

            logger.warning(f"Content Strategy invalid on attempt {attempt+1}/3. Retrying...")
            await asyncio.sleep(1)

        if final_data is None:
            logger.error("Content Strategy failed after retries. Using deterministic fallback.")
            final_data = self._normalize_content_strategy(
                {}, primary_keyword, content_type, area
            )

        state["content_strategy"] = final_data
        logger.info(f"CONTENT STRATEGY GENERATED:\n{json.dumps(final_data, indent=2, ensure_ascii=False)}")
        return state
    
    async def _step_1_outline(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Generates the article outline with a soft retry loop for validation failures."""
        
        input_data = state.get("input_data", {})
        title = input_data.get("title") or "Untitled"
        keywords = input_data.get("keywords") or []
        urls_raw = input_data.get("urls", [])
        urls_norm = []
        
        # We use state["internal_resources"] which was populated in brand_discovery
        # Junk link filter (avoid Contact, Login, etc.)
        junk_slugs = {'contact', 'about', 'login', 'signup', 'account', 'cart', 'checkout', 'privacy', 'terms', 'help', 'faq'}
        
        def is_junk(url):
            path = urlparse(url).path.lower().rstrip('/')
            last_segment = path.split('/')[-1]
            return last_segment in junk_slugs

        internal_resources = state.get("internal_resources", [])
        
        # Filter internal_resources based on junk slugs, BUT PROTECT manual URLs
        filtered_internal_resources = [
            r for r in internal_resources 
            if r.get("is_manual") or not is_junk(r.get('link', ''))
        ]

        # Deduplicate based on 'link' (using the canonical URL for matching)
        # Prioritize manual entries during deduplication to keep their specific anchor text
        temp_map = {}
        for r in filtered_internal_resources:
            canon = self._canon_url(r.get("link", ""))
            if not canon: continue
            if canon not in temp_map or (r.get("is_manual") and not temp_map[canon].get("is_manual")):
                temp_map[canon] = r
                
        deduplicated_internal_resources = list(temp_map.values())
        
        logger.info(f"Final internal pool: {len(deduplicated_internal_resources)} resources ({sum(1 for r in deduplicated_internal_resources if r.get('is_manual'))} manual, {sum(1 for r in deduplicated_internal_resources if not r.get('is_manual'))} discovered).")

        for res in deduplicated_internal_resources:
            urls_norm.append({
                "text": res.get("text", "Internal Resource"), 
                "link": res.get("link"),
                "is_manual": res.get("is_manual", False)
            })

        for u in urls_norm:
            u["type"] = "internal" 

        seo_intelligence = state.get("seo_intelligence", {})
        content_strategy = state.get("content_strategy", {})
        area = state.get("area")
        
        content_type = state.get("content_type", "informational") or "informational"
        intent = state.get("intent") or "informational"
        # article_language = input_data.get("article_language", "en")
        # article_language =state.get("article_language", "en")
        article_language = state.get("article_language") or state.get("input_data", {}).get("article_language", "en")
        content_strategy = state.get("content_strategy", {})

        mandatory = set(self.REQUIRED_STRUCTURE_BY_TYPE[content_type]["mandatory"])

        structural = seo_intelligence.get("strategic_analysis", {}).get("structural_intelligence", {})
        pricing_ratio = structural.get("pricing_presence_ratio", 0)

        if pricing_ratio > 0.4:
            mandatory.add("pricing")
            
        # Conditionally require case study
        has_case_study = False
        if content_type == "brand_commercial":
            case_keywords = ["case", "portfolio", "project", "work", "أعمال", "مشاريع", "success", "client", "study"]
            for u in urls_norm:
                t_lower = u.get("text", "").lower()
                l_lower = u.get("link", "").lower()
                if any((kw in t_lower or kw in l_lower) for kw in case_keywords):
                    has_case_study = True
                    break
        if has_case_study:
            mandatory.add("case_study")
    
        
        feedback = None
        outline = []
        outline_data = {}

        for attempt in range(3):
            logger.info(f"Generating outline (Attempt {attempt + 1}/3)...")
            outline_data = await self.outline_gen.generate(
                title=title,
                keywords=keywords,
                urls=urls_norm,
                article_language=article_language,
                intent=intent,
                seo_intelligence=seo_intelligence,
                content_type=content_type,
                content_strategy=content_strategy,
                brand_context=state.get("brand_context", ""),
                area=area,
                feedback=feedback,
                mandatory_section_types = list(mandatory)
            )
            
            # Store metadata for WorkflowLogger
            if "metadata" in outline_data:
                state["last_step_prompt"] = outline_data["metadata"]["prompt"]
                state["last_step_response"] = outline_data["metadata"]["response"]
                state["last_step_tokens"] = outline_data["metadata"]["tokens"]

            if not outline_data or not outline_data.get("outline"):
                if attempt < 2:
                    feedback = "Outline generation returned empty result. Please provide a full, structured JSON outline."
                    continue
                raise RuntimeError("Outline generation returned empty result after 3 attempts.")
            
            outline = outline_data.get("outline", [])
            
            # Validation Layer
            errors = []
            
            # 0. FAQ Consolidation (Robustness)
            outline = self._consolidate_faq(outline)
            
            # 1. Intent Distribution
            outline, dist_errors = self._enforce_intent_distribution(
                outline,
                intent,
                content_type
            )
            errors.extend(dist_errors)

            # 2. Local SEO
            outline, local_errors = await self._inject_local_seo(outline, area)
            errors.extend(local_errors)

            # 3. Quality (Thin, Duplicates, CTAs)
            quality_errors = self._validate_outline_quality(outline, intent)
            errors.extend(quality_errors)

            if not errors:
                logger.info(f"Outline validated successfully on attempt {attempt + 1}.")
                break
            
            feedback = "Validation failed. Please correct the following issues and regenerate the outline:\n- " + "\n- ".join(errors)
            logger.warning(f"Outline validation failed (attempt {attempt + 1}): {feedback}")

        # Post-validation enhancements (non-critical, so we don't retry)
        outline = self._enforce_outline_structure(
            outline,
            intent=intent,
            area=area,
            content_type=content_type
        )

        outline = await self._enforce_content_angle(
            outline,
            content_strategy
        )

        outline = self._adjust_paa_by_intent(
            outline,
            intent
        )

        # Final metadata and normalization
        # paa_questions = seo_intelligence["strategic_analysis"]["semantic_assets"]
        paa_questions = (
            seo_intelligence
            .get("strategic_analysis", {})
            .get("semantic_assets", {})
            .get("paa_questions", [])
        )
        paa_check = self.enforce_paa_sections(outline, paa_questions, min_percent=0.15)
        if not paa_check["paa_ok"]:
            logger.warning(
                f"[paa_validate] PAA coverage too low: {paa_check['paa_ratio']:.0%} "
                f"(missing ~{paa_check['missing_count']} PAA-inspired H2s). "
                f"Prompt 01_outline_generator.txt should produce ≥15% PAA coverage."
            )
        
        # Ensure mandatory sections exist (for logging/debugging)
        present_types = {(s.get("section_type") or "").lower().strip() for s in outline}
        if "faq" not in present_types:
            logger.warning("[outline_validate] Missing section_type='faq'.")
        if "conclusion" not in present_types:
            logger.warning("[outline_validate] Missing section_type='conclusion'.")

        # Prevent duplicate H2 headings
        seen_h2 = set()
        unique_outline = []
        for sec in outline:
            if (sec.get("heading_level") or "").upper() == "H2" and sec["heading_text"] in seen_h2:
                sec["heading_text"] += f" ({len(seen_h2)+1})"
            seen_h2.add(sec["heading_text"])
            unique_outline.append(sec)
        outline = unique_outline

        keyword_expansion = outline_data.get("keyword_expansion", {})
        state["global_keywords"] = keyword_expansion

        # Normalize sections first
        for idx, sec in enumerate(outline):
            self.outline_gen._normalize_section(
                sec, idx, content_type, content_strategy, area
            )
            sec.setdefault("assigned_keywords", [])

        # LSI distribution safely
        lsi_keywords = keyword_expansion.get("lsi", [])
        if lsi_keywords:
            lsi_pool = lsi_keywords.copy()
            for sec in outline:
                sec_lsi = lsi_pool[:3]
                sec["assigned_keywords"].extend(sec_lsi)
                lsi_pool = lsi_pool[3:]

        # state["brand_url"] = urls_norm[0].get("link") if urls_norm else ""

        state["internal_url_set"] = {
            self._canon_url(u.get("link", ""))
            for u in urls_norm if u.get("link")
        }

        state["blocked_external_domains"] = self._extract_competitor_domains(
            state.get("serp_data", {}),
            brand_url=state.get("brand_url") or ""
        )

        state["link_strategy"] = {
            "internal_topics": urls_norm,
            "affiliate_policy": {"max_per_section": 3, "placement": "distributed", "tone": "neutral"}
        }
                
        # primary_keyword = keywords[0] if keywords else title
        primary_keyword = state.get("primary_keyword")
        for sec in outline:
            sec["primary_keyword"] = primary_keyword
            sec["article_language"] = article_language
            if not sec.get("assigned_keywords"):
                 # Robust safety fallback
                 sec["assigned_keywords"] = keywords[:3] if keywords else [primary_keyword]
        
        # --- Article-Level Link Deduplication ---
        # Ensure no URL is assigned to more than one section in the entire article
        all_assigned_urls = set()
        
        for section in outline:
            assigned = section.get("assigned_links", [])
            valid_assigned = []
            for link in assigned:
                url = link.get("url") if isinstance(link, dict) else link
                if not url: continue
                
                norm = self._normalize_url_for_dedup(url)
                if norm not in all_assigned_urls:
                    all_assigned_urls.add(norm)
                    valid_assigned.append(link)
                else:
                    logger.warning(f"Removing duplicate link assignment in outline: {url}")
            
            section["assigned_links"] = valid_assigned

        state["outline"] = outline
        present_types = {sec.get("section_type") for sec in outline}

        user_urls = state.get("input_data", {}).get("urls", [])

        internal_links = [
            u["link"] for u in user_urls if u.get("link")
        ]

        state["internal_url_set"] = set(internal_links)

        missing = mandatory - present_types

        if missing:
            logger.error(f"[outline_validate] Missing mandatory sections: {missing}")
            # we could raise error or just log depending on strictness
            # raise ValueError(f"Missing mandatory sections: {missing}")

        return state
    
    async def _step_2_write_sections(self, state: Dict[str, Any]) -> Dict[str, Any]:
        input_data = state.get("input_data", {})
        title = input_data.get("title", "Untitled")
        outline = state.get("outline", [])
        global_keywords = state.get("global_keywords", {})
        intent = state.get("intent", "Informational")
        seo_intelligence = state.get("seo_intelligence", {})
        link_strategy = state.get("link_strategy", {})

        if not outline:
            raise RuntimeError("No outline found for section writing.")

        content_type = state.get("content_type", "informational")

        if PARALLEL_SECTIONS:
            tasks = [
                self._write_single_section(
                    title=title,
                    global_keywords=global_keywords,
                    section=section,
                    article_intent=intent,
                    seo_intelligence=seo_intelligence,
                    content_type=content_type,
                    link_strategy=link_strategy,
                    state=state,
                    section_index=idx,
                    total_sections=len(outline)
                )
                for idx, section in enumerate(outline)
            ]
            logger.info(f"Writing {len(tasks)} sections in PARALLEL mode")
            results = await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logger.info(f"Writing {len(outline)} sections in SEQUENTIAL mode")
            results = []
            for idx, section in enumerate(outline):
                res = await self._write_single_section(
                    title=title,
                    global_keywords=global_keywords,
                    section=section,
                    article_intent=intent,
                    seo_intelligence=seo_intelligence,
                    content_type=content_type,
                    link_strategy=link_strategy,
                    state=state,
                    section_index=idx,
                    total_sections=len(outline)
                )
                results.append(res)

        sections_content = {}
        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Section failed: {res}")
                continue
            if not res:
                continue

            if res.get("brand_link_used"):
                state["brand_link_used"] = True

            sections_content[res["section_id"]] = res

        state["sections"] = sections_content

        # Local SEO Enforcement (Retry first section if area is missing)
        area = state.get("area")
        if area and sections_content:
            first_id = outline[0]["section_id"]
            first_res = sections_content.get(first_id)

            if first_res and area.lower() not in first_res["generated_content"].lower():
                logger.info(f"Local area '{area}' missing in first section. Retrying with enforcement...")

                retry_res = await self._write_single_section(
                    title=title,
                    global_keywords=global_keywords,
                    section=outline[0],
                    article_intent=intent,
                    seo_intelligence=seo_intelligence,
                    content_type=content_type,
                    link_strategy=link_strategy,
                    state=state,
                    force_local=True,
                    section_index=0,
                    total_sections=len(outline)
                )

                if retry_res:
                    sections_content[first_id] = retry_res
                    state["sections"] = sections_content
                    logger.info("First section regenerated successfully with Local SEO enforcement.")
                else:
                    logger.warning("Retry of first section failed.")

        logger.info(f"Successfully wrote {len(sections_content)} sections.")
        return state

    async def _write_single_section(
        self,
        title: str,
        global_keywords: Dict[str, Any],
        section: Dict[str, Any],
        article_intent: str,
        seo_intelligence: Dict[str, Any],
        content_type: str,
        link_strategy: Dict[str, Any],
        state: Dict[str, Any],
        force_local: bool = False,
        section_index: int = 0,
        total_sections: int = 1
    ) -> Optional[Dict[str, Any]]:
        """Worker to write one section."""
        
        section_id = section.get("section_id") or section.get("id")
        brand_url = state.get("brand_url")
        brand_link_used = state.get("brand_link_used", False)
        section_type = (section.get("section_type") or "").lower()
        
        # Always allow the introduction to use the brand link, regardless of state.
        is_introduction = section_type == "introduction"
        can_use_brand_link = bool(brand_url) and (is_introduction or not brand_link_used)

        execution_plan = self._build_execution_plan(section, state)
        if force_local:
            execution_plan["local_context_required"] = True
            
        execution_plan["brand_link_allowed"] = can_use_brand_link
        execution_plan["brand_url"] = brand_url

        # --- GUARANTEE: Inject the brand homepage link into the Introduction's assigned links ---
        # This ensures the AI ALWAYS has the brand link available for the introduction,
        # even if the outline generator failed to assign it.
        if is_introduction and brand_url:
            assigned = section.setdefault("assigned_links", [])
            existing_urls = {
                (lnk.get("url") if isinstance(lnk, dict) else lnk)
                for lnk in assigned
            }
            if brand_url not in existing_urls:
                assigned.insert(0, {"url": brand_url, "text": f"Brand Homepage ({brand_url})"})
                logger.info(f"[brand_link] Injected brand homepage link into introduction: {brand_url}")

        used_phrases = state.get("used_phrases", [])

        # --- Find the most relevant brand page for this specific section ---
        brand_context = state.get("brand_context", "")
        brand_pages_index = state.get("brand_pages_index", {})
        section_source_text = ""

        if brand_pages_index:
            # Score each indexed page by relevance to this specific section
            section_heading = (section.get("heading_text") or "").lower()
            section_type = (section.get("section_type") or "").lower()
            section_goal = (section.get("content_goal") or "").lower()
            section_query = f"{section_heading} {section_type} {section_goal}"
            section_tokens = [t for t in section_query.split() if len(t) > 2]

            best_url, best_score, best_text = "", 0, ""
            for url, page_text in brand_pages_index.items():
                text_lower = page_text.lower()
                score = sum(1 for t in section_tokens if t in text_lower)
                if score > best_score:
                    best_score, best_url, best_text = score, url, page_text

            if best_text and best_score > 0:
                # Trim to avoid token bloat
                section_source_text = best_text[:2500]
                logger.info(f"Section '{section_heading}' -> using brand page: {best_url} (score={best_score})")

        # --- Extract curated external sources from SERP ---
        external_sources = []
        serp_results = state.get("serp_data", {}).get("top_results", [])
        blocked_domains = state.get("blocked_external_domains", set())
        brand_domain = self._domain(state.get("brand_url", ""))
        
        for r in serp_results:
            url = r.get("url")
            if not url: continue
            dom = self._domain(url)
            if dom == brand_domain or dom in blocked_domains:
                continue
            external_sources.append({"url": url, "text": r.get("title", "External Resource")})
            if len(external_sources) >= 8: # Cap to 8 sources
                break
        
        logger.info(f"Extracted {len(external_sources)} external sources for section '{section.get('heading_text')}'")

        # Try 1

        res_data = await self.section_writer.write(
            title=title,
            global_keywords=global_keywords,
            section=section,
            article_intent=article_intent,
            seo_intelligence=seo_intelligence,
            content_type=content_type,
            link_strategy=link_strategy,
            brand_url=brand_url,
            brand_link_used=state.get("brand_link_used", False),
            brand_link_allowed=execution_plan.get("brand_link_allowed", False),
            allow_external_links=True,
            execution_plan=execution_plan,
            area=state.get("area", ""),
            used_phrases=used_phrases,
            used_internal_links=state.get("used_internal_links", []),
            used_external_links=state.get("used_external_links", []),
            section_index=section_index,
            total_sections=total_sections,
            brand_context=brand_context,
            section_source_text=section_source_text,
            external_sources=external_sources,
            workflow_logger=state.get("workflow_logger")
        )
        
        content = res_data.get("content", "")
        used_links = res_data.get("used_links", [])
        brand_link_used_in_sec = res_data.get("brand_link_used", False)
        
        # Store metadata for WorkflowLogger
        if "metadata" in res_data:
            state["last_step_prompt"] = res_data["metadata"]["prompt"]
            state["last_step_response"] = res_data["metadata"]["response"]
            state["last_step_tokens"] = res_data["metadata"]["tokens"]

        # Semantic Overlap Rejection
        if content and getattr(self, "semantic_model", None) and state.get("used_claims"):
            is_rejected, overlap_score, overlap_sentence = self._check_semantic_overlap(content, state.get("used_claims", []), threshold=0.85)
            if is_rejected:
                logger.warning(f"Semantic Overlap Rejected ({overlap_score:.2f}) for '{title}'. Sentence: '{overlap_sentence}'. Retrying...")
                res_data = await self.section_writer.write(
                    title=title,
                    global_keywords=global_keywords,
                    section=section,
                    article_intent=article_intent,
                    seo_intelligence=seo_intelligence,
                    content_type=content_type,
                    link_strategy=link_strategy,
                    brand_url=brand_url,
                    brand_link_used=brand_link_used,
                    brand_link_allowed=can_use_brand_link,
                    allow_external_links=True,
                    execution_plan={
                        **execution_plan, 
                        "writing_mode": "creative rephrasing",
                        "structure_rule": "AVOID PARAPHRASING PREVIOUS CLAIMS. YOU MUST INTRODUCE A COMPLETELY NEW ANGLE OR ABORT."
                    },
                    area=state.get("area"),
                    used_phrases=used_phrases,
                    used_internal_links=state.get("used_internal_links", []),
                    used_external_links=state.get("used_external_links", []), 
                    section_index=section_index,
                    total_sections=total_sections,
                    external_sources=external_sources
                )
                content = res_data.get("content", "")
                used_links = res_data.get("used_links", [])
                brand_link_used_in_sec = res_data.get("brand_link_used", False)

        # Multi-Layer Paragraph Structure and Strict SEO Validation
        if content:
            is_valid, validation_errors = await self._validate_section_output(
                content, 
                section, 
                section_index, 
                total_sections, 
                state.get("area"),
                execution_plan.get("cta_type", "none"),
                blocked_domains=state.get("blocked_external_domains", set())
            )
            
            if not is_valid:
                error_msg = "; ".join(validation_errors)
                logger.warning(f"Validation failed for '{title}': {error_msg}. Attempting strict regeneration...")
                res_data = await self.section_writer.write(
                    title=title,
                    global_keywords=global_keywords,
                    section=section,
                    article_intent=article_intent,
                    seo_intelligence=seo_intelligence,
                    content_type=content_type,
                    link_strategy=link_strategy,
                    brand_url=brand_url,
                    brand_link_used=brand_link_used,
                    brand_link_allowed=can_use_brand_link,
                    allow_external_links=True,
                    execution_plan={
                        **execution_plan, 
                        "writing_mode": "creative rephrasing",
                        "structure_rule": f"CRITICAL ERRORS TO FIX: {error_msg}. EXACTLY 3-5 PARAGRAPHS. EXACTLY 2-3 SENTENCES PER PARAGRAPH."
                    },
                    area=state.get("area"),
                    used_phrases=used_phrases,
                    used_internal_links=state.get("used_internal_links", []),
                    used_external_links=state.get("used_external_links", []),
                    section_index=section_index,
                    total_sections=total_sections,
                    external_sources=external_sources
                )
                content = res_data.get("content", "")
                used_links = res_data.get("used_links", [])
                brand_link_used_in_sec = res_data.get("brand_link_used", False)

        # Repetition Guard (Retry Loop)
        if content:
            repeated = self._detect_repetition(content, used_phrases)
            if repeated and len(repeated) > 0:
                logger.warning(f"High repetition detected in section '{title}'. Retrying...")
                res_data = await self.section_writer.write(
                    title=title,
                    global_keywords=global_keywords,
                    section=section,
                    article_intent=article_intent,
                    seo_intelligence=seo_intelligence,
                    content_type=content_type,
                    link_strategy=link_strategy,
                    brand_url=brand_url,
                    brand_link_used=brand_link_used,
                    brand_link_allowed=can_use_brand_link,
                    allow_external_links=True,
                    execution_plan={**execution_plan, "writing_mode": "creative rephrasing"},
                    area=state.get("area"),
                    used_phrases=used_phrases + repeated,
                    used_internal_links=state.get("used_internal_links", []),
                    used_external_links=state.get("used_external_links", []), 
                    section_index=section_index,
                    total_sections=total_sections,
                    external_sources=external_sources
                )
                content = res_data.get("content", "")
                used_links = res_data.get("used_links", [])
                brand_link_used_in_sec = res_data.get("brand_link_used", False)

        if content:
            new_sentences = self._extract_sentences(content)
            state.setdefault("used_phrases", [])
            state.setdefault("used_claims", [])
            state.setdefault("used_internal_links", [])
            state.setdefault("used_external_links", [])

            substantial_sentences = [s for s in new_sentences if len(s) > 40]
            state["used_phrases"].extend(substantial_sentences)
            if getattr(self, "semantic_model", None):
                state["used_claims"].extend(substantial_sentences)

            content = self._sanitize_section_links(
                content=content,
                state=state,
                brand_url=brand_url or "",
                max_external=2 # Increased to allow 3-4 across article
            )

            logger.info(f"Section '{section.get('heading_text')}' finalized. Current external links in state: {len(state.get('used_external_links', []))}")
            if state.get("workflow_logger"):
                state["workflow_logger"].log_event(f"Section Finalized: {section.get('heading_text')}", {
                    "external_links_count": len(state.get("used_external_links", [])),
                    "internal_links_count": len(state.get("used_internal_links", []))
                })

            # classify links after sanitize
            found_links = re.findall(r'\[.*?\]\((https?://.*?)\)', content)
            for link in found_links:
                cu = self._canon_url(link)
                if cu in state.get("internal_url_set", set()) or self._is_same_site(cu, brand_url or ""):
                    if cu not in state["used_internal_links"]:
                        state["used_internal_links"].append(cu)
                else:
                    if cu not in state["used_external_links"]:
                        state["used_external_links"].append(cu)

            # update brand link flag
            if brand_url:
                bcu = self._canon_url(brand_url)
                if any(self._canon_url(l) == bcu for l in found_links):
                    state["brand_link_used"] = True

            final_content = self._enforce_paragraph_structure(content)

            return {
                **section,
                "section_id": section_id,
                "generated_content": final_content,
                "used_links": found_links,
                "brand_link_used": state.get("brand_link_used", False)
            }
        return None
    
    async def _step_4_generate_image_prompts(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Generates image prompts using the image client."""
        if not self.enable_images:
            logger.info("Image pipeline skipped (disabled).")
            return state

        input_data = state.get("input_data", {})
        title = input_data.get("title", "Untitled")
        keywords = input_data.get("keywords", [])
        outline = state.get("outline", [])
        primary_keyword = state.get("primary_keyword")
        brand_visual_style = state.get("brand_visual_style", "")

        # Zero out previous step tokens to prevent token leakage in metrics log
        state["last_step_tokens"] = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

        image_prompts = await self.image_prompt_planner.generate(
            title=title,
            primary_keyword=primary_keyword,
            keywords=keywords,
            outline=outline,
            brand_visual_style=brand_visual_style
        )
        print("FINAL IMAGE PROMPTS COUNT:", len(image_prompts))

        for p in image_prompts:
            alt = p.get("alt_text", "")
            if primary_keyword and primary_keyword.lower() not in alt.lower():
                p["alt_text"] = f"{primary_keyword} - {alt}"

        state["image_prompts"] = image_prompts
        return state

    async def _step_4_1_generate_master_frame(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generates a unique AI Master Frame based on brand colors and identity.
        """
        logo_path = state.get("input_data", {}).get("logo_image_path") or state.get("logo_path")
        brand_colors = state.get("brand_colors", [])
        
        if not logo_path or not brand_colors:
            logger.info("Skipping Master Frame generation: No logo or brand colors found.")
            return state

        color_str = ", ".join(brand_colors)
        primary_keyword = state.get("primary_keyword", "Professional Business")
        
        # Design a prompt for a functional 'Picture Frame' border
        frame_prompt = f"""Sophisticated 'Picture Frame' border template for a {primary_keyword} article.
        Design a premium horizontal frame with thick elegant borders on all four sides.
        Borders should feature modern architectural textures, glassmorphism, or abstract geometric patterns.
        Primary Brand Colors to incorporate into the border: {color_str}.
        The center of the image MUST be a flat, solid white rectangular area (the content zone).
        Style: Luxury, 3D depth, professional lighting, soft inner-shadow on the frame edges.
        NO PEOPLE, NO REAL PHOTOS, NO TEXT. Just a reusable branded frame border."""

        logger.info(f"Generating Master Frame with colors: {color_str}")
        
        # We use a single generation for the Master Frame
        try:
            # Create a temporary 'prompt' object for the image client
            frame_prompt_obj = {
                "prompt": frame_prompt,
                "alt_text": "Master Brand Frame",
                "image_type": "MasterFrame"
            }
            
            output_dir = state.get("output_dir", self.work_dir)
            frames_dir = os.path.join(output_dir, "images")
            os.makedirs(frames_dir, exist_ok=True)
            
            self.image_client.save_dir = frames_dir
            master_frame_res = await self.image_client.generate_images(
                [frame_prompt_obj],
                primary_keyword=primary_keyword,
                workflow_logger=state.get("workflow_logger")
            )
            
            if master_frame_res and "local_path" in master_frame_res[0]:
                raw_frame_path = os.path.join(output_dir, master_frame_res[0]["local_path"])
                
                # Now, use ImageGenerator to add the LOGO to this new Master Frame permanently
                final_master_frame_path = self.image_client.create_branded_template(
                    base_frame_path=raw_frame_path,
                    logo_path=logo_path,
                    output_path=os.path.join(frames_dir, "master_brand_template.png")
                )
                
                if final_master_frame_path:
                    state["master_frame_path"] = final_master_frame_path
                    logger.info(f"Master Frame created successfully: {final_master_frame_path}")
                
        except Exception as e:
            logger.error(f"Failed to generate Master Frame: {e}")
            
        return state
    
    async def _step_4_5_download_images(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Downloads images (now parallel in the client)."""
        prompts = state.get("image_prompts", [])
        keywords = state.get("input_data", {}).get("keywords", [])
        # primary_keyword = (keywords[0] if keywords else "") or ""
        primary_keyword = state.get("primary_keyword")
        # logo_path = state.get("input_data", {}).get("logo_path")
        brand_visual_style = state.get("brand_visual_style", "")
        
        # Prioritize USER OVERRIDES if available, else use auto-discovered
        image_frame_path = state.get("input_data", {}).get("image_frame_path") or state.get("master_frame_path")
        logo_path = state.get("input_data", {}).get("logo_image_path") or state.get("logo_path")
        
        # Zero out previous step tokens
        state["last_step_tokens"] = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

        output_dir = state.get("output_dir", self.work_dir)
        images_dir = os.path.join(output_dir, "images")
        os.makedirs(images_dir, exist_ok=True)
        self.image_client.save_dir = images_dir

        images = await self.image_client.generate_images(
            prompts,
            primary_keyword=primary_keyword,
            image_frame_path=image_frame_path,
            logo_path=logo_path,
            brand_visual_style=brand_visual_style,
            workflow_logger=state.get("workflow_logger")
        )

        for img in images:
            if "local_path" in img:
                img["local_path"] = f"images/{os.path.basename(img['local_path'])}"

        state["images"] = images
        return state
 
    async def _step_5_assembly(self, state):
        title = state.get("input_data", {}).get("title", "Untitled")
        outline = state.get("outline", [])
        # sections_list = list(state["sections"].values())
        sections_dict = state.get("sections", {})
        # article_language = state.get("input_data", {}).get("article_language", "ar")
        article_language = state.get("article_language") or state.get("input_data", {}).get("article_language", "en")
        ordered_sections = [
            sections_dict[s["section_id"]]
            for s in outline
            if s.get("section_id") in sections_dict
        ]

        # Redundancy Guard & Similarity Check
        final_sections = []
        for i, section in enumerate(ordered_sections):
            content = section.get("generated_content", "")
            if not content:
                continue

            # Similarity Check against previous sections
            is_redundant = False
            for prev in final_sections:
                prev_content = prev.get("generated_content", "")
                similarity = self._calculate_similarity(content, prev_content)
                if similarity > 0.7:
                    logger.warning(f"High similarity ({similarity:.2f}) detected between section '{section.get('heading_text')}' and a previous section. Flagging for pruning.")
                    is_redundant = True
                    break
            
            # Prune redundant intros anyway for consistent quality
            section["generated_content"] = self._prune_redundant_intros(content)
            final_sections.append(section)

        assembled = await self.assembler.assemble(title=title, sections=final_sections, article_language=article_language)
        
        # Final pass redundancy pruning on the whole assembled markdown
        if "final_markdown" in assembled:
            md = assembled["final_markdown"]
            md = self._prune_redundant_intros(md)
            brand_url = state.get("brand_url", "")
            brand_domain = self._domain(brand_url) if brand_url else ""
            md = self._deduplicate_links_in_markdown(md, brand_domain=brand_domain, max_internal=6)

            assembled["final_markdown"] = md

        state["final_output"] = assembled
        return state

    async def _step_6_image_inserter(self, state):
        final_md = state.get("final_output", {}).get("final_markdown", "")
        images = state.get("images", [])

        if not final_md or not images:
            return state

        new_md = await self.image_inserter.insert(final_md, images)
        # Run a second dedup pass after image insertion to catch any links added by images
        brand_url = state.get("brand_url", "")
        brand_domain = self._domain(brand_url) if brand_url else ""
        new_md = self._deduplicate_links_in_markdown(new_md, brand_domain=brand_domain, max_internal=6)
        state["final_output"]["final_markdown"] = new_md
        return state

    async def _step_7_meta_schema(self, state):
        final_md = state.get("final_output", {}).get("final_markdown", "")
        if not final_md:
            return state

        meta_raw = await self.meta_schema.generate(
            final_markdown=final_md,
            primary_keyword=state.get("primary_keyword"),
            intent=state.get("intent"),
            article_language = state.get("article_language") or state.get("input_data", {}).get("article_language", "en"),
            secondary_keywords=state.get("input_data", {}).get("keywords", []),
            include_meta_keywords=state.get("include_meta_keywords", False),
            article_url=state.get("final_url")
        )

        meta_json = recover_json(meta_raw)

        if not meta_json:
            logger.error("Meta schema returned invalid JSON")
            return state

        meta_json = enforce_meta_lengths(meta_json)

        # Enforce H1 Length (Strict)
        h1 = meta_json.get("h1", "")
        if h1 and not self.validate_h1_length(h1):
            logger.error(f"H1 length invalid ({len(h1)} chars).")
            raise ValueError("H1 length invalid")
            
        state["seo_meta"] = meta_json
        return state

    async def _step_8_article_validation(self, state):

        final_md = state.get("final_output", {}).get("final_markdown", "")
        meta = state.get("seo_meta", {})
        images = state.get("images", [])
        input_data = state.get("input_data", {})

        title = input_data.get("title", "")
        # article_language = input_data.get("article_language", "en")
        # article_language = state.get("article_language", "en")
        article_language = state.get("article_language") or state.get("input_data", {}).get("article_language", "en")
        keywords = input_data.get("keywords", [])
        # primary_keyword = keywords[0] if keywords else ""
        primary_keyword = state.get("primary_keyword")

        if not final_md:
            state["seo_report"] = {
                "status": "FAIL",
                "issues": ["Final markdown missing"]
            }
            return state
        
        
        # final_md = self.sanitize_links(
        #     final_md,
        #     max_external=3,
        #     max_brand=1,
        #     brand_url=state.get("brand_url")
        # )
        final_md = self.sanitize_links(
            final_md,
            max_external=3,
            max_brand=6,
            brand_url=state.get("brand_url"),
            internal_url_set=state.get("internal_url_set", set()),
            blocked_domains=state.get("blocked_external_domains", set()),
            allowed_domains=state.get("allowed_external_domains", set())
        )

        state["final_output"]["final_markdown"] = final_md

        word_count, keyword_count, keyword_density = self.calculate_keyword_stats(
            final_md,
            primary_keyword
        )
        critical_issues = []
        warnings = []

        # Heuristic checks
        ok, issue = self.validate_sales_intro(final_md, state.get("intent"))
        if not ok:
            critical_issues.append(issue)

        if state.get("content_type") == "brand_commercial":
            structural_intel = state.get("seo_intelligence", {}).get("strategic_analysis", {}).get("structural_intelligence", {})
            # article_language = state.get("article_language", "en")
            article_language = state.get("article_language") or state.get("input_data", {}).get("article_language", "en")
            
            is_dense_enough = self.calculate_sales_density(
                final_md, 
                state.get("intent"), 
                article_language, 
                structural_intel
            )
            
            if not is_dense_enough:
                intensity = structural_intel.get("cta_intensity_pattern", "soft commercial")
                critical_issues.append(f"Sales density too low for {intensity} mode")

        ok, local_issues = self.validate_local_seo(
            final_md,
            meta,
            state.get("area")
        )
        critical_issues.extend(local_issues)

        # Enforce Contextual Local SEO (Strict)
        area = state.get("area")
        if area:
            if not self.validate_local_context(final_md, area, article_language):
                logger.error(f"Weak local contextualization for area '{area}'")
                raise ValueError("Weak local contextualization")

        ok, angle_issue = self.validate_content_angle(
            final_md,
            state.get("content_strategy", {})
        )
        if not ok:
            critical_issues.append(angle_issue)

        # Enforce Final CTA in Conclusion (Commercial Articles)
        # if state.get("intent") == "Commercial":
        if state.get("intent", "").lower() == "commercial":
            if not self.validate_final_cta(final_md, article_language):
                logger.error("Missing final CTA in conclusion for Commercial article.")
                raise ValueError("Missing final CTA")

        final_md = self.auto_split_long_paragraphs(final_md)
        state["final_output"]["final_markdown"] = final_md

        # Enforce Paragraph Length Rules
        if not self.validate_paragraph_structure(final_md):
            logger.error("Paragraph structure violation detected.")
            raise ValueError("Paragraph structure violation")

        report_raw = await self.article_validator.validate(
            final_markdown=final_md, 
            meta=meta, 
            images=images,
            title=title,
            article_language=article_language,
            primary_keyword=primary_keyword,
            word_count=word_count,
            keyword_count=keyword_count,
            keyword_density=keyword_density,
            content_strategy=state.get("content_strategy", {})
        )

        report_json = recover_json(report_raw)

        if not isinstance(report_json, dict):
            state["seo_report"] = {
                "status": "FAIL",
                "critical_issues": ["Validator returned malformed JSON"],
                "warnings": []
            }
            return state

        # Merge AI issues
        ai_critical = report_json.get("critical_issues", [])
        if isinstance(ai_critical, list):
            critical_issues.extend(ai_critical)
            
        ai_warnings = report_json.get("warnings", [])
        if isinstance(ai_warnings, list):
            warnings.extend(ai_warnings)
        
        # Backward compatibility for "issues" field if it exists
        if "issues" in report_json and isinstance(report_json["issues"], list):
            critical_issues.extend(report_json["issues"])

        # Final Report Building
        final_report = {
            "critical_issues": critical_issues,
            "warnings": warnings,
            "status": "FAIL" if len(critical_issues) > 3 else "PASS"
        }

        state["seo_report"] = final_report
        return state

    async def _step_render_html(self, state):
        """Step 9: Render HTML page"""
        final_output = self._assemble_final_output(state)
        output_dir = state.get("output_dir", "")
        
        # Prepare data for renderer
        meta = state.get("seo_meta", {})
        title = state.get("input_data", {}).get("title", "")
        render_data = {
            "title": title,
            "meta_title": meta.get("meta_title", title),
            "meta_description": meta.get("meta_description", ""),
            "meta_keywords": meta.get("meta_keywords", ""),
            "final_markdown": final_output.get("final_markdown"),
            "output_dir": output_dir,
            "article_language": final_output.get("article_language", state.get("article_language", "en")),
        }
        
        try:
            html_path = render_html_page(render_data)
            logger.info(f"HTML Page rendered successfully at: {html_path}")
            state["html_path"] = html_path
        except Exception as e:
            logger.error(f"Failed to render HTML page: {e}")

        # Save Markdown to output directory
        final_markdown = final_output.get("final_markdown")
        if output_dir and final_markdown:
            md_path = os.path.join(output_dir, "article_final.md")
            try:
                with open(md_path, "w", encoding="utf-8") as f:
                    f.write(final_markdown)
                logger.info(f"Markdown saved to: {md_path}")
            except Exception as e:
                logger.error(f"Failed to save Markdown file: {e}")

        return state
    
    # ---------------- UTILITIES ----------------
    def _enforce_paragraph_structure(self, text: str) -> str:
        """
        Enforce max 3 sentences per paragraph WITHOUT breaking markdown tables/lists.
        """
        if not text:
            return text

        # 1) Protect table blocks first (lines with 2+ pipes or starting with |)
        table_pattern = re.compile(r'((?:^\s*\|?.*\|.*\|?.*$\n?){2,})', re.MULTILINE)
        table_blocks = []

        def _stash_table(m):
            table_blocks.append("\n".join([ln.rstrip() for ln in m.group(1).strip("\n").splitlines()]))
            return f"@@TABLE_BLOCK_{len(table_blocks)-1}@@"

        protected = table_pattern.sub(_stash_table, text)

        # 2) Process normal paragraphs only
        paragraphs = [p.strip() for p in protected.split("\n\n") if p.strip()]
        fixed = []

        for p in paragraphs:
            # keep protected table placeholder as-is
            if p.startswith("@@TABLE_BLOCK_") and p.endswith("@@"):
                fixed.append(p)
                continue

            # keep headings/lists/code markers as-is
            if p.startswith("#") or p.startswith("- ") or p.startswith("* ") or re.match(r"^\d+\.\s", p) or p.startswith("```"):
                fixed.append(p)
                continue

            # split long paragraph by sentences into chunks of max 3
            sentences = re.split(r'(?<=[.!؟])\s+', p)
            chunks = []
            for i in range(0, len(sentences), 3):
                chunk = " ".join(s for s in sentences[i:i+3] if s.strip()).strip()
                if chunk:
                    chunks.append(chunk)
            fixed.extend(chunks if chunks else [p])

        out = "\n\n".join(fixed)

        # 3) Restore tables exactly
        for i, t in enumerate(table_blocks):
            out = out.replace(f"@@TABLE_BLOCK_{i}@@", t)

        return out

    SUPPORTED_LANGS = {"ar", "en", "de", "fr", "es", "it", "tr", "pt"}
    LANG_ALIASES = {
        "arabic": "ar", "english": "en", "german": "de",
        "zh-cn": "zh", "zh-tw": "zh", "pt-br": "pt",
        "en-us": "en", "en-gb": "en"
    }

    def _normalize_lang(self, lang: Optional[str]) -> Optional[str]:
        if not lang:
            return None
        code = str(lang).strip().lower().replace("_", "-")
        code = self.LANG_ALIASES.get(code, code)
        # keep only primary subtag
        code = code.split("-")[0]
        return code if code in self.SUPPORTED_LANGS else None

    def _detect_title_language(self, raw_title: str) -> Optional[str]:
        title = (raw_title or "").strip()
        if not title:
            return None

        # Heuristic for Arabic script (faster + safer)
        if re.search(r"[\u0600-\u06FF]", title):
            return "ar"

        # avoid noisy detection on very short titles
        if len(re.findall(r"\w+", title)) < 2:
            return None

        try:
            candidates = detect_langs(title)  # e.g. [de:0.92, nl:0.06]
            if not candidates:
                return None
            top = candidates[0]
            if float(top.prob) < 0.70:
                return None
            return self._normalize_lang(top.lang)
        except Exception as e:
            logger.warning(f"Language detection failed: {e}")
            return None

    def _resolve_article_language(self, raw_title: str, user_lang: Optional[str]) -> str:
        normalized_user = self._normalize_lang(user_lang)
        if normalized_user:
            return normalized_user

        detected = self._detect_title_language(raw_title)
        if detected:
            return detected

        return "en"

    def _sluggify(self, text: str) -> str:
        """Generates a clean slug from English or Arabic text."""
        clean = re.sub(r'[^\w\s-]', '', text).strip().lower()
        return re.sub(r'[-\s_]+', '-', clean)

    def calculate_keyword_stats(self, markdown: str, keyword: str):
        if not markdown or not keyword:
            return 0, 0, 0.0

        # Remove markdown syntax
        clean_text = re.sub(r'[#>*`\-\[\]\(\)!]', '', markdown)

        words = re.findall(r'\b\w+\b', clean_text.lower())
        word_count = len(words)

        # keyword_count = clean_text.lower().count(keyword.lower())

        pattern = r'\b{}\b'.format(re.escape(keyword.lower()))
        keyword_count = len(re.findall(pattern, clean_text.lower()))


        density = 0.0
        if word_count > 0:
            density = (keyword_count / word_count) * 1000  # per 1000 words

        return word_count, keyword_count, round(density, 2)

    def _extract_first_json_object(self, text: str) -> str:
        if not text:
            return ""
        cleaned = re.sub(r"```json|```", "", text, flags=re.IGNORECASE).strip()
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return cleaned
        return cleaned[start:end+1]

    def _normalize_content_strategy(self, data: Dict[str, Any], primary_keyword: str, content_type: str, area: str) -> Dict[str, Any]:
        defaults = {
            "primary_angle": f"{primary_keyword} with performance-first execution",
            "strategic_positioning": "Practical, conversion-focused, locally adapted",
            "target_reader_state": "Comparing providers and ready to shortlist",
            "pain_point_focus": [],
            "emotional_trigger": "Fear of losing leads due to weak digital presence",
            "depth_level": "comprehensive",
            "authority_strategy": [],
            "eeat_signals_to_include": [],
            "differentiation_focus": [],
            "conversion_strategy": "Intro CTA bridge -> proof -> close CTA",
            "cta_philosophy": "One clear CTA early, one decisive CTA in conclusion",
            "local_strategy": f"Reflect market behavior, trust factors, and payment context in {area}" if area else "No local constraint",
            "tone_direction": "Confident, direct, benefit-led",
            "section_role_map": {
                "introduction": "Hook with local market urgency + primary CTA",
                "core_or_benefits": "Show service value and business outcomes",
                "proof": "Use metrics, case-style evidence, trust signals",
                "process_or_how": "Clear implementation path and delivery model",
                "faq": "Handle objections and clarify buying concerns",
                "conclusion": "Reinforce value + final strong CTA"
            }
        }

        out = defaults.copy()
        if isinstance(data, dict):
            out.update(data)

        if not isinstance(out.get("pain_point_focus"), list):
            out["pain_point_focus"] = []
        if not isinstance(out.get("authority_strategy"), list):
            out["authority_strategy"] = []
        if not isinstance(out.get("eeat_signals_to_include"), list):
            out["eeat_signals_to_include"] = []
        if not isinstance(out.get("differentiation_focus"), list):
            out["differentiation_focus"] = []

        role_defaults = defaults["section_role_map"]
        role_map = out.get("section_role_map", {})
        if not isinstance(role_map, dict):
            role_map = {}
        out["section_role_map"] = {**role_defaults, **role_map}

        allowed_depth = {"intermediate", "advanced", "comprehensive"}
        if out.get("depth_level") not in allowed_depth:
            out["depth_level"] = "comprehensive"

        return out

    def _is_valid_content_strategy(self, data: Dict[str, Any]) -> bool:
        required = [
            "primary_angle", "strategic_positioning", "target_reader_state",
            "pain_point_focus", "emotional_trigger", "depth_level",
            "authority_strategy", "eeat_signals_to_include", "differentiation_focus",
            "conversion_strategy", "cta_philosophy", "local_strategy",
            "tone_direction", "section_role_map"
        ]
        if not isinstance(data, dict) or not data:
            return False
        return all(k in data for k in required)

    def _detect_repetition(self, text: str, global_used_phrases: List[str], threshold: int = 1) -> List[str]:
        """Detects repeated sentences within the text or against global memory."""
        if not text:
            return []
            
        sentences = self._extract_sentences(text)
        repeated = []
        
        # 1. Internal Repetition
        counts = Counter(sentences)
        internal_repeated = [s for s, c in counts.items() if c > threshold and len(s) > 30]
        repeated.extend(internal_repeated)
        
        # 2. Global Repetition
        for s in sentences:
            if len(s) > 40: # Only check meaningful sentences
                if s in global_used_phrases:
                    repeated.append(s)
                    
        return list(set(repeated))

    def _check_semantic_overlap(self, text: str, used_claims: List[str], threshold: float = 0.85) -> tuple[bool, float, str]:
        """Checks if the new text has high semantic overlap with any previously used claims."""
        if not getattr(self, "semantic_model", None) or not text or not used_claims:
            return False, 0.0, ""
            
        sentences = self._extract_sentences(text)
        # Only check substantial sentences for semantic meaning
        sentences = [s for s in sentences if len(s) > 40]
        
        if not sentences:
            return False, 0.0, ""
            
        try:
            from sentence_transformers import util
            import torch
            # Encode sentences and claims
            new_embeddings = self.semantic_model.encode(sentences, convert_to_tensor=True)
            claim_embeddings = self.semantic_model.encode(used_claims, convert_to_tensor=True)
            
            # Calculate cosine similarity matrix
            cosine_scores = util.cos_sim(new_embeddings, claim_embeddings)
            
            # Find maximum similarity
            max_score = float(torch.max(cosine_scores))
            
            # Find the specific overlapping sentence if needed for logging
            if max_score > threshold:
                max_idx = int(torch.argmax(cosine_scores).item())
                row_idx = max_idx // cosine_scores.shape[1]
                overlapping_sentence = sentences[row_idx]
                return True, max_score, overlapping_sentence
                
            return False, max_score, ""
        except Exception as e:
            logger.error(f"Semantic overlap check failed: {e}")
            return False, 0.0, ""

    async def _validate_section_output(self, content: str, section: Dict[str, Any], section_index: int, total_sections: int, area: str, cta_type: str, blocked_domains: set = None) -> tuple[bool, List[str]]:
        """Strictly validates a section's output against counting and structural rules."""
        errors = []
        if not content:
            return False, ["Content is empty"]

        # 1. Paragraph Count Validation (Except FAQ/Pricing which might have lists/tables)
        is_faq_or_pricing = section.get("section_type") in ["faq", "pricing"]
        paragraphs = [p for p in content.split("\n\n") if p.strip()]
        
        # Don't strictly check paragraph boundaries if it has markdown lists or tables
        has_complex_structure = "|" in content or "- " in content or "* " in content
        
        if not is_faq_or_pricing and not has_complex_structure:
            num_paragraphs = len(paragraphs)
            if num_paragraphs < 2 or num_paragraphs > 6:
                errors.append(f"Paragraph count is {num_paragraphs}, must be 3-5")
                
        # 2. Sentence Count Validation per Paragraph (loose check)
        for p in paragraphs:
            if not p.startswith("#") and not p.startswith("-") and not p.startswith("*") and not "|" in p:
                sentences = re.split(r'(?<=[.!؟])\s+', p.strip())
                num_sentences = len([s for s in sentences if len(s.strip()) > 5])
                if num_sentences > 5:
                    errors.append("Paragraphs are too dense (> 4 sentences)")
                    break

        # 3. Local Mention Check
        if area and section_index == 0:
            if area.lower() not in content.lower():
                errors.append(f"Missing mandatory local area mention: {area}")

        # 4. CTA Architecture Check (Basic heuristic)
        has_link_or_button = "]" in content and "(" in content
        has_cta_verb = any(verb in content for verb in ["احصل", "اطلب", "تواصل", "ابدأ", "Get", "Request", "Start"])
        looks_like_cta = has_link_or_button or has_cta_verb

        is_first = (section_index == 0)
        is_last = (section_index == total_sections - 1)

        if is_first and cta_type in ["primary", "strong"] and not looks_like_cta:
            errors.append("Missing required Primary CTA in Introduction")
        elif is_last and cta_type in ["primary", "strong"] and not looks_like_cta:
            errors.append("Missing required Decisive CTA in Conclusion")
        elif not is_first and not is_last and cta_type == "none" and has_link_or_button and has_cta_verb:
            # It's a middle section, no CTA allowed, but it looks like it has one
            # Note: We give some leniency, this might be an informational link.
            pass

        # 5. Primary Keyword Density Check
        primary_kw = section.get("assigned_keywords", [""])[0] if section.get("assigned_keywords") else ""
        if primary_kw and not is_faq_or_pricing:
            kw_lower = primary_kw.lower()
            content_lower = content.lower()
            # count occurrences (whole word or phrase match)
            kw_count = len(re.findall(re.escape(kw_lower), content_lower))
            if kw_count < 1:
                errors.append(f"Primary keyword '{primary_kw}' missing from core content")

        # 6. Flexible External Link Validation
        found_links = re.findall(r'\[.*?\]\((https?://.*?)\)', content)
        internal_domain = self._domain(section.get("brand_url", "")) if section.get("brand_url") else ""
        blocked_domains = blocked_domains or set()

        for link in found_links:
            link_domain = self._domain(link)
            # Skip internal links and brand links
            if link_domain == internal_domain:
                continue
            
            # Check for Competitors
            if link_domain in blocked_domains or any(comp in link_domain for comp in blocked_domains):
                 errors.append(f"External link to a potential competitor detected: {link}. Links must be to non-competing authority/credible sources.")
                 continue

            # Verify Reachability (Ensure link works)
            if not await self._verify_external_link(link):
                errors.append(f"External link appears to be broken or unreachable (404/Timeout): {link}")

        return len(errors) == 0, errors

    def _extract_sentences(self, text: str) -> List[str]:
        """Extracts sentences using regex that supports Arabic and English."""
        # Remove markdown chars first for better sentence matching
        clean_text = re.sub(r'[#*`\-]', '', text)
        sentences = re.split(r'(?<=[.!؟])\s+', clean_text)
        return [s.strip() for s in sentences if s.strip()]

    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculates Jaccard Similarity between two texts."""
        if not text1 or not text2:
            return 0.0
            
        def get_words(text):
            return set(re.findall(r'\b\w{5,}\b', text.lower())) # Only check words > 5 chars for meaningful similarity
            
        words1 = get_words(text1)
        words2 = get_words(text2)
        
        if not words1 or not words2:
            return 0.0
            
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return intersection / union

    def _inject_commercial_ctas(self, markdown: str, article_language: str) -> str:
        """
        Post-processing CTA injector for brand_commercial articles.
        Scans the assembled markdown and deterministically injects rotating
        CTA phrases at strategic positions:
          - After first paragraph of each H2 body (except skip sections)
          - After any table within a section
          - Before the first FAQ/conclusion-type section
        CTAs rotate through a bank to avoid repetition.
        Total CTAs capped at MAX_CTAS.
        """
        import re

        # ---- CTA Bank (rotating, no links) ----
        CTA_BANK = {
            "ar": [
                "تواصل مع فريقنا اليوم وابدأ مشروعك في أقرب وقت.",
                "احصل على استشارة مجانية من خبرائنا المتخصصين — بدون أي التزام.",
                "لا تدع منافسيك يسبقونك — ابدأ مشروعك الرقمي الآن.",
                "اكتشف كيف يمكننا تحويل رؤيتك إلى نتائج رقمية حقيقية.",
                "خبراؤنا جاهزون للإجابة على كل تساؤلاتك — تواصل معنا الآن.",
                "خطوتك الأولى نحو النجاح تبدأ بمحادثة واحدة — دعنا نبدأ.",
            ],
            "en": [
                "Contact our team today and get your project moving within days.",
                "Get a free consultation with our specialists — zero commitment required.",
                "Don't let your competitors launch first — start your digital project now.",
                "See how our team can turn your vision into measurable digital results.",
                "Our experts are standing by — reach out and get direct answers today.",
                "Your path to digital success starts with one conversation — let's begin.",
            ],
        }

        MAX_CTAS = 6
        cta_idx = [0]
        ctas_injected = [0]

        lang = "ar" if article_language and "ar" in article_language.lower() else "en"
        ctas = CTA_BANK.get(lang, CTA_BANK["en"])

        def next_cta() -> str:
            if ctas_injected[0] >= MAX_CTAS:
                return ""
            cta = ctas[cta_idx[0] % len(ctas)]
            cta_idx[0] += 1
            ctas_injected[0] += 1
            return f"\n\n**{cta}**\n"

        # Keywords that identify FAQ / skip sections
        SKIP_KEYWORDS = [
            "faq", "أسئلة", "frequently asked", "questions", "خاتمة",
            "conclusion", "في ختام", "summary", "في النهاية",
        ]
        INTRO_KEYWORDS = ["introduction", "مقدمة", "overview", "نظرة عامة"]

        def is_skip(heading: str) -> bool:
            h = heading.lower()
            return any(kw in h for kw in SKIP_KEYWORDS)

        def is_intro(heading: str) -> bool:
            h = heading.lower()
            return any(kw in h for kw in INTRO_KEYWORDS)

        def is_conclusion(heading: str) -> bool:
            h = heading.lower()
            return any(kw in h for kw in ["conclusion", "خاتمة", "في ختام", "ready to", "start your"])

        # ---- Split markdown into (heading, body) pairs ----
        parts = re.split(r'(^## .+$)', markdown, flags=re.MULTILINE)

        # parts[0] = content before first H2 (H1 title, etc.)
        # parts[1::2] = H2 headings
        # parts[2::2] = section bodies

        if len(parts) < 3:
            return markdown  # No H2 sections found

        pre_content = parts[0]
        headings = parts[1::2]
        bodies = parts[2::2]

        result_parts = [pre_content]

        # Peek ahead helper
        def next_heading_is_skip(i):
            if i + 1 < len(headings):
                return is_skip(headings[i + 1])
            return False

        for i, (heading, body) in enumerate(zip(headings, bodies)):
            skip = is_skip(heading)
            intro = is_intro(heading)
            concl = is_conclusion(heading)

            if skip or intro or concl:
                # Inject pre-FAQ CTA before FAQ section
                if skip and ctas_injected[0] < MAX_CTAS:
                    # Add to end of previous section (already appended), add before this heading
                    cta = next_cta()
                    if cta and result_parts:
                        result_parts[-1] = result_parts[-1].rstrip() + cta
                result_parts.append(heading)
                result_parts.append(body)
                continue

            new_body = body

            # 1. Inject CTA after first real paragraph in this section
            # AVOID injecting if the section starts with a table or list
            paragraphs = re.split(r'\n{2,}', new_body.strip())
            if paragraphs:
                first_para = paragraphs[0]
                is_table_or_list = first_para.strip().startswith("|") or first_para.strip().count("|") >= 2 or first_para.strip().startswith("-") or first_para.strip().startswith("*")
                
                if not is_table_or_list:
                    rest = paragraphs[1:]
                    cta = next_cta()
                    if cta:
                        new_body = first_para + cta + "\n\n" + "\n\n".join(rest) if rest else first_para + cta

            # 2. Inject CTA after any markdown table in this section
            # More robust table pattern: matches blocks where multiple lines have pipes
            table_pattern = re.compile(
                r'((?:^\s*\|?.*\|.*\|?.*$\n?){2,})',
                re.MULTILINE
            )
            def inject_after_table(m):
                cta = next_cta()
                return m.group(0) + (cta if cta else "")
            new_body = table_pattern.sub(inject_after_table, new_body)

            # 3. If next section is FAQ or skip, inject a strong CTA at end of this section
            if next_heading_is_skip(i):
                cta = next_cta()
                if cta:
                    new_body = new_body.rstrip() + cta

            result_parts.append(heading)
            result_parts.append(new_body)

        return "\n".join(result_parts)

    def _prune_redundant_intros(self, text: str) -> str:
        """
        Removes repetitive 'Vision 2030' or 'Digital Transformation' style filler intros
        if they appear too close to each other or are redundant.
        """
        if not text:
            return text
            
        # 1. Clean up repetitive Vision 2030 / Transformation clusters
        # Regex to find patterns like (Sentence about vision 2030). (Another sentence about vision 2030).
        # We simplify it to catch repeated core keyword phrases at the start of paragraphs
        patterns = [
            r'(رؤية المملكة 2030.*?\.){2,}',
            r'(Vision 2030.*?\.){2,}',
            r'(التحول الرقمي.*?\.){2,}',
            r'(Digital Transformation.*?\.){2,}'
        ]
        
        cleaned = text
        for p in patterns:
            cleaned = re.sub(p, r'\1', cleaned, flags=re.IGNORECASE | re.DOTALL)
            
        # 2. Prevent consecutive paragraphs starting with the same 5 words
        lines = cleaned.split("\n\n")
        if len(lines) < 2:
            return cleaned
            
        pruned_lines = [lines[0]]
        for i in range(1, len(lines)):
            current = lines[i].strip()
            prev = pruned_lines[-1].strip()
            
            if not current or not prev:
                pruned_lines.append(current)
                continue
                
            cur_words = current.split()[:5]
            prev_words = prev.split()[:5]
            
            if cur_words == prev_words and len(cur_words) >= 3:
                # Similarity too high at start, skip or prune
                logger.info(f"Pruning repetitive paragraph start: {' '.join(cur_words)}")
                # Keep only the unique part if possible, or just keep it as is for now but log
                pruned_lines.append(current)
            else:
                pruned_lines.append(current)
                
        return "\n\n".join(pruned_lines)

    def sanitize_links(
        self,
        markdown: str,
        max_external: int = 3,
        max_brand: int = 1,
        brand_url: str = None,
        internal_url_set: set = None,
        blocked_domains: set = None,
        allowed_domains: set = None
    ):
        if not markdown:
            return markdown

        internal_url_set = internal_url_set or set()
        blocked_domains = blocked_domains or set()
        allowed_domains = allowed_domains or set()

        used_external = set()
        brand_count = 0
        external_count = 0

        pattern = r'\[([^\]]+)\]\(([^)]+)\)'

        def repl(m):
            nonlocal brand_count, external_count
            text, raw_url = m.group(1), m.group(2).strip()

            # Remove invalid links like (None)
            if raw_url.lower() in {"none", "null", ""}:
                return text
            if not raw_url.startswith("http"):
                return text

            cu = self._canon_url(raw_url)
            dom = self._domain(cu)

            # Internal URLs: always allowed
            if cu in internal_url_set or self._is_same_site(cu, brand_url or ""):
                return f"[{text}]({raw_url})"

            # Brand URL rule FIRST
            if brand_url and self._canon_url(raw_url) == self._canon_url(brand_url):
                if brand_count >= max_brand:
                    return text
                brand_count += 1
                return f"[{text}]({raw_url})"

            # Internal URLs
            if cu in internal_url_set or self._is_same_site(cu, brand_url or ""):
                return f"[{text}]({raw_url})"

            # External rules
            if dom in blocked_domains:
                return text
            if not self._is_authority_domain(dom, allowed_domains):
                return text
            if cu in used_external:
                return text
            # if external_count >= max_external:
            if len(used_external) >= max_external:
                return text

            external_count += 1
            used_external.add(cu)
            return f"[{text}]({raw_url})"

        return re.sub(pattern, repl, markdown)

    def validate_intent_from_serp(self, serp_analysis: dict) -> str:
        """Strengthened intent detection based on SERP structural intelligence."""
        structural = serp_analysis.get("structural_intelligence", {})

        page_type = structural.get("dominant_page_type", "")
        cta_pattern = structural.get("cta_intensity_pattern", "")
        pricing_ratio = structural.get("pricing_presence_ratio", 0)
        faq_ratio = structural.get("faq_presence_ratio", 0)

        commercial_score = 0
        informational_score = 0

        # Page type weight (strongest signal)
        if page_type in ["service", "homepage"]:
            commercial_score += 3
        elif page_type in ["guide", "comparison"]:
            informational_score += 3

        # Pricing presence
        if pricing_ratio > 0.4:
            commercial_score += 2

        # CTA intensity
        if cta_pattern in ["soft commercial", "aggressive"]:
            commercial_score += 2
        else:
            informational_score += 1

        # FAQ presence
        if faq_ratio > 0.4:
            informational_score += 1

        return "Commercial" if commercial_score >= informational_score else "Informational"

    def validate_h1_length(self, h1: str) -> bool:
        """Enforces H1 length rules (50-75 chars) as per the framework."""
        return 55 <= len(h1) <= 75

    def validate_strategy_alignment(self, strategy, primary_keyword, area):
        angle = strategy.get("primary_angle", "").lower()
        if primary_keyword.lower() not in angle:
            return False, "Primary keyword not reflected in strategy angle"

        if area and area.lower() not in strategy.get("strategic_positioning","").lower():
            return False, "Local positioning missing"

        return True, None

    def _assemble_final_output(self, state: Dict[str, Any]) -> Dict[str, Any]:
        import re
        input_data = state.get("input_data", {})
        final_out = state.get("final_output", {})
        seo_meta = state.get("seo_meta", {})
        images = state.get("images", [])
        seo_report = state.get("seo_report", {})
        performance = self.ai_client.observer.summarize_model_calls()
        content_type = state.get("content_type", "informational")

        raw_title = input_data.get("title", "Untitled")
        meta_title = seo_meta.get("meta_title", "")

        # For commercial articles, inject brand name into title & meta_title
        if content_type == "brand_commercial":
            brand_url = state.get("brand_url", "")
            if brand_url:
                # Extract a clean brand name from the domain
                domain = self._domain(brand_url)  # e.g., "cems-it.com"
                brand_name = domain.split(".")[0]  # e.g., "cems-it"
                brand_name = brand_name.replace("-", " ").replace("_", " ").title()  # e.g., "Cems It"

                # Append to article title if not already included
                if brand_name.lower() not in raw_title.lower():
                    raw_title = f"{raw_title} | {brand_name}"

                # Append to meta_title if not already included (meta titles are character-limited)
                if meta_title and brand_name.lower() not in meta_title.lower():
                    # Keep meta_title under 60 chars
                    candidate = f"{meta_title} | {brand_name}"
                    if len(candidate) <= 65:
                        meta_title = candidate
                    # If too long, just use the original meta_title unchanged

        return {
            "title": raw_title,
            "slug": state.get("slug", "unknown"),
            "primary_keyword": state.get("primary_keyword", ""),
            "final_markdown": final_out.get("final_markdown", ""),
            "article_language": state.get("article_language", "en"),

            # SEO
            "meta_title": meta_title,
            "meta_description": seo_meta.get("meta_description", ""),
            "meta_keywords": seo_meta.get("meta_keywords", ""),
            "article_schema": seo_meta.get("article_schema", {}),
            "faq_schema": seo_meta.get("faq_schema", {}),

            # Media
            "images": images,

            # Validation
            "seo_report": seo_report,

            # Performance
            "performance": performance,

            # Debug / Storage
            "output_dir": state.get("output_dir", ""),
        }


    _MANDATORY_ROLES: ClassVar[set] = {"introduction", "conclusion"}
    _EDITORIAL_ROLES: ClassVar[set] = {"pros", "cons", "who_for", "who_avoid"}
    # _BRAND_ROLES:     ClassVar[set] = {"benefits"}


    REQUIRED_STRUCTURE_BY_TYPE = {
        "brand_commercial": {
            "mandatory": {
                "introduction",
                "benefits",
                "why_choose_us",
                "proof",
                "process",
                "faq",
                "conclusion"
            },
            "conditional": {
                "pricing": "if_serp_pricing"
            }
        },

        "informational": {
            "mandatory": {
                "introduction",
                "core",
                "examples_or_use_cases",
                "pros_cons",
                "faq",
                "conclusion"
            }
        },

        "comparison": {
            "mandatory": {
                "introduction",
                "comparison",
                "criteria",
                "pros_cons_each",
                "who_should_choose_what",
                "faq",
                "conclusion"
            }
        }
    }

    def _enforce_outline_structure(self, outline: List[Dict[str, Any]], intent: str, area: Optional[str], content_type: str,) -> List[Dict[str, Any]]:
        """
        VALIDATES that the LLM-generated outline contains the required semantic
        section_type roles.
        """
        present_types = {
            (s.get("section_type") or "").lower().strip()
            for s in outline
        }

        # --- content-type specific strict mandatory roles ---
        structure_rules = self.REQUIRED_STRUCTURE_BY_TYPE.get(content_type)
        if structure_rules:
            required = structure_rules.get("mandatory", set())
            missing = required - present_types
            if missing:
                logger.error(f"[outline_validate] Missing mandatory sections for {content_type}: {missing}")
                raise ValueError(f"Outline missing mandatory sections: {missing}")

        # --- assign section_ids for any section that is missing one ---
        for i, sec in enumerate(outline):
            if not sec.get("section_id"):
                sec["section_id"] = f"sec_{i+1:02d}"

        return outline

    def _enforce_intent_distribution(self, outline, intent, content_type):
        errors = []
        h2_sections = [s for s in outline if (s.get("heading_level") or "").upper() == "H2"]

        if content_type == "brand_commercial":
            TARGET_COMMERCIAL_RATIO = 0.70
            # Protected section types that should NOT be converted to commercial
            PROTECTED_TYPES = {"faq", "conclusion", "introduction"}

            commercial_sections = [
                s for s in h2_sections
                if s.get("section_intent") in ["Commercial", "Transactional"]
            ]
            ratio = len(commercial_sections) / max(len(h2_sections), 1)

            if ratio < TARGET_COMMERCIAL_RATIO:
                # Actively fix: convert eligible informational sections to Commercial
                needed = round(TARGET_COMMERCIAL_RATIO * len(h2_sections)) - len(commercial_sections)
                converted = 0
                for s in h2_sections:
                    if converted >= needed:
                        break
                    s_type = (s.get("section_type") or "").lower()
                    s_intent = s.get("section_intent", "")
                    if s_type in PROTECTED_TYPES:
                        continue
                    if s_intent not in ["Commercial", "Transactional"]:
                        # Convert to Commercial
                        s["section_intent"] = "Commercial"
                        s["sales_intensity"] = s.get("sales_intensity", "medium")
                        if s.get("cta_type") in [None, "none", ""]:
                            s["cta_type"] = "moderate"
                            s["cta_position"] = "last_sentence"
                        converted += 1

                # Recalculate after correction
                commercial_now = [
                    s for s in h2_sections
                    if s.get("section_intent") in ["Commercial", "Transactional"]
                ]
                new_ratio = len(commercial_now) / max(len(h2_sections), 1)
                logger.info(f"[intent_distribution] Corrected commercial ratio: {ratio:.0%} → {new_ratio:.0%} (converted {converted} sections)")

                if new_ratio < 0.60:
                    errors.append(
                        f"Commercial intent distribution still too weak ({new_ratio:.0%}) after correction. "
                        f"Brand articles require at least 70% commercial/transactional H2 sections."
                    )

        if intent.lower() == "informational":
            for s in outline:
                if s.get("cta_allowed"):
                    errors.append(f"Section '{s.get('heading_text')}' allows CTA but article intent is Informational.")
                s["cta_allowed"] = False

        return outline, errors

    def enforce_paa_sections( self, outline: List[Dict], paa_questions: List[str], min_percent: float = 0.15,) -> Dict[str, Any]:
        """
        VALIDATES PAA coverage in the LLM-generated outline.
        Does NOT inject sections — the LLM is responsible for covering PAA
        questions (per the prompt: "At least 30% of H2 headings inspired by PAA").

        Returns a dict so the call site can decide whether to regenerate.
        """
        h2_sections = [s for s in outline if (s.get("heading_level") or "").upper() == "H2"]
        total_h2 = max(len(h2_sections), 1)

        if not paa_questions:
            return {"paa_ok": True, "paa_ratio": 1.0, "missing_count": 0}

        safe_paa = []
        for q in paa_questions:
            if isinstance(q, dict):
                safe_paa.append(str(q.get("question") or q.get("text", str(q))).lower())
            else:
                safe_paa.append(str(q).lower())

        covered = sum(
            1
            for sec in h2_sections
            if any(
                q_text in sec.get("heading_text", "").lower()
                for q_text in safe_paa
            )
        )

        ratio = covered / total_h2
        required = max(1, int(total_h2 * min_percent))
        missing = max(0, required - covered)

        return {
            "paa_ok": ratio >= min_percent,
            "paa_ratio": round(ratio, 2),
            "missing_count": missing,
        }

    async def _inject_local_seo(self, outline, area):
        """
        VALIDATES that the local area is reflected in the first H2.
        Does NOT mutate heading_text.
        """
        if not area:
            return outline, []

        errors = []
        # Only mark FIRST core H2 as local-context required to avoid over-optimization
        applied = False
        for s in outline:
            if s.get("section_type") == "core" and s.get("heading_level") == "H2" and not applied:
                s["local_context_required"] = True
                applied = True
            else:
                s.pop("local_context_required", None)

        # Soft validation.
        first_h2 = next((s for s in outline if (s.get("heading_level") or "").upper() == "H2"), None)
        if first_h2 and area.lower() not in first_h2.get("heading_text", "").lower():
            msg = f"Local area '{area}' not reflected in the first H2 heading: '{first_h2.get('heading_text')}'."
            logger.warning(f"[local_seo_validate] {msg}")
            errors.append(msg)

        return outline, errors

    async def _enforce_content_angle(self, outline, strategy):
        if not strategy:
            return outline

        angle = strategy.get("primary_angle")
        if not angle:
            return outline

        # Only assign the angle to the first core H2 section to avoid "robotic" repetition
        applied = False
        for s in outline:
            if s.get("section_type") == "core" and s.get("heading_level") == "H2" and not applied:
                s["content_angle"] = angle
                applied = True
            else:
                s.pop("content_angle", None)

        return outline

    def _consolidate_faq(self, outline: List[Dict]) -> List[Dict]:
        """
        Groups all sections with section_type='faq' or parent_section='sec_faq' 
        into a single FAQ section.
        """
        faq_sections = [s for s in outline if s.get("section_type") == "faq" or s.get("parent_section") == "sec_faq"]
        if not faq_sections:
            return outline

        # Keep the first FAQ section as the anchor
        first_faq = faq_sections[0]
        
        # Consolidate all questions
        all_questions = []
        for s in faq_sections:
            # If it has a 'questions' list (new format)
            if s.get("questions") and isinstance(s["questions"], list):
                all_questions.extend(s["questions"])
            # If it's a separate question (old format or PAA)
            elif s.get("heading_level") in ["H2", "H3"]:
                all_questions.append(s["heading_text"])

        # Ensure all questions are strings before deduplicating
        safe_questions = []
        for q in all_questions:
            if isinstance(q, dict):
                safe_q = q.get("question") or q.get("text", str(q))
                safe_questions.append(str(safe_q))
            else:
                safe_questions.append(str(q))

        # Update the first FAQ section
        first_faq["questions"] = list(dict.fromkeys(safe_questions)) # Deduplicate
        first_faq["section_type"] = "faq"
        first_faq["heading_level"] = "H2"
        if "parent_section" in first_faq:
            del first_faq["parent_section"]

        # Filter out other FAQ sections
        new_outline = []
        faq_anchored = False
        for s in outline:
            is_faq = s.get("section_type") == "faq" or s.get("parent_section") == "sec_faq"
            if is_faq:
                if not faq_anchored:
                    new_outline.append(first_faq)
                    faq_anchored = True
            else:
                new_outline.append(s)

        return new_outline

    def _adjust_paa_by_intent(self, outline, intent):
        if intent in ["transactional", "commercial"]:
            # Move all PAA to FAQ section only
            for s in outline:
                if s.get("source") == "paa":
                    s["heading_level"] = "H3"
                    s["parent_section"] = "sec_faq"

        return outline

    def _validate_outline_quality(self, outline, intent):
        errors = []
        h2_sections = [s for s in outline if (s.get("heading_level") or "").upper() == "H2"]

        if len(h2_sections) < 3:
            errors.append(f"Outline too thin: only {len(h2_sections)} H2 sections found. Need at least 3-5.")

        # Prevent duplicate H2 text
        texts = [s["heading_text"].lower() for s in h2_sections]
        if len(texts) != len(set(texts)):
            errors.append("Duplicate H2 headings detected. Each heading must be unique.")

        # FAQ Question Count Validation
        faq_section = next((s for s in outline if s.get("section_type") == "faq"), None)
        faq_count = len(faq_section.get("questions", [])) if faq_section else 0
        
        if faq_count > 0:
            if faq_count > 6:
                errors.append(f"Too many FAQ questions detected ({faq_count}). Maximum allowed is 6.")
            if faq_count < 4:
                errors.append(f"Too few FAQ questions detected ({faq_count}). Minimum required is 4.")

        return errors

    def _build_execution_plan(self, section, state):
        content_type = state.get("content_type")
        intent = state.get("intent")
        area = state.get("area")

        plan = {}
        plan["structure_rule"] = "standard structured paragraphs"
        plan["force_external_link_sections"] = ["proof", "core"]
        
        # Writing Mode
        if content_type == "brand_commercial":
            plan["writing_mode"] = "persuasive"
            plan["tone"] = "authoritative, confident"
            plan["conversion_weight"] = 0.8
        else:
            plan["writing_mode"] = "educational"
            plan["tone"] = "expert, clear"
            plan["conversion_weight"] = 0.3

        if intent == "Comparative":
            plan["writing_mode"] = "analytical"
            plan["structure_rule"] = "criteria-based comparison"
            plan["comparison_required"] = True
        else:
            plan["comparison_required"] = False

        # Local SEO
        plan["local_context_required"] = bool(area)

        # CTA Rules
        if content_type == "brand_commercial" and section.get("section_type") in ["core", "introduction"]:
            # Override with structural insights if available
            structural = state.get("seo_intelligence", {}).get("strategic_analysis", {}).get("structural_intelligence", {})
            plan["cta_position"] = structural.get("cta_position_pattern") or "first_paragraph"
            plan["cta_strength"] = structural.get("cta_intensity_pattern") or "strong"
        else:
            plan["cta_position"] = "none"
            plan["cta_strength"] = "none"

        # Content Angle
        plan["angle"] = section.get("content_angle")

        section_type = section.get("section_type")

        if section_type == "introduction":
            plan["structure_rule"] = "hook + positioning"

        elif section_type == "benefits":
            plan["structure_rule"] = "benefit-first bullets"

        elif section_type == "faq":
            plan["structure_rule"] = "question-driven concise answers"

        elif section_type == "conclusion":
            plan["structure_rule"] = "recap + high-urgency conversion CTA block (MUST use descriptive anchor text, no generic 'contact us')"

        # Apply Structural Intelligence Words & Patterns
        serp_strat = state.get("seo_intelligence", {}).get("strategic_analysis", {})
        structural = serp_strat.get("structural_intelligence", {})
        
        avg_wc = structural.get("avg_word_count")
        if avg_wc and isinstance(avg_wc, (int, float)):
            plan["target_word_count"] = int(avg_wc)
        else:
            plan["target_word_count"] = 400 # Sensible default

        plan["cta_pattern"] = structural.get("cta_position_pattern") or None
        plan["cta_intensity"] = structural.get("cta_intensity_pattern") or None

        plan["cta_enabled"] = plan["cta_position"] != "none"

        if plan["cta_enabled"]:
            plan["cta_type"] = "soft" if plan["cta_strength"] == "medium" else "strong"
        else:
            plan["cta_type"] = "none"

        ROLE_EXECUTION_RULES = {
            "proof": {
                "structure_rule": "evidence-driven credibility",
                "writing_mode": "persuasive",
                "conversion_weight": 0.8
            },
            "why_choose_us": {
                "structure_rule": "differentiation positioning",
                "writing_mode": "persuasive",
                "conversion_weight": 0.9
            },
            "pricing": {
                "structure_rule": "roi transparency framing",
                "writing_mode": "analytical",
                "conversion_weight": 0.85
            }
        }
        role_rules = ROLE_EXECUTION_RULES.get(section_type, {})
        plan.update(role_rules)

        return plan

    def validate_sales_intro(self, markdown: str, intent: str):
        if intent not in ["Transactional", "Commercial"]:
            return True, None

        first_200_words = " ".join(markdown.split()[:200]).lower()

        cta_keywords = [
            "تواصل", "احصل على", "اطلب", "استشارة", "عرض سعر",
            "contact", "get a quote", "book", "call us"
        ]

        if any(k in first_200_words for k in cta_keywords):
            return True, None

        return False, "Missing CTA in first 200 words for sales article"

    sales_terms_by_language = {
        "ar": ["اتصل", "تواصل", "احجز", "اطلب", "سعر", "خدمة", "شركة"],
        "en": ["contact", "call", "book", "order", "price", "service", "agency"]
    }

    local_context_terms = {
        "ar": ["السوق", "العملاء في", "شركات في", "المنافسة في"],
        "en": ["market in", "businesses in", "companies in", "competition in"]
    }

    def calculate_sales_density(self, text: str, intent: str, language: str, structural_intel: dict) -> bool:
        if intent.lower() != "commercial":
           return True

        terms = self.sales_terms_by_language.get(language, [])
        paragraphs = [p for p in text.split("\n") if len(p.strip()) > 30]

        if not paragraphs:
            return False

        sales_count = sum(
            any(term.lower() in p.lower() for term in terms)
            for p in paragraphs
        )

        ratio = sales_count / len(paragraphs)

        intensity = structural_intel.get("cta_intensity_pattern", "soft commercial")

        required_ratio = {
            "aggressive": 0.5,
            "soft commercial": 0.3
        }.get(intensity, 0.3)

        return ratio >= required_ratio

    def validate_final_cta(self, text: str, language: str) -> bool:
        """Enforces a final CTA presence, with a strict terminal check for Arabic."""
        if not text:
            return False
            
        clean_text = text.strip()
        
        # Hard terminal check for Arabic as requested by user
        if language == "ar":
            if clean_text.endswith(("الآن.", "اليوم.", "الآن!", "اليوم!")):
                return True
        
        # Fallback to general terms check for other languages or as secondary AR check
        terms = self.sales_terms_by_language.get(language, [])
        last_300 = clean_text[-300:].lower()
        return any(term.lower() in last_300 for term in terms)

    def validate_local_context(self, text: str, area: str, language: str) -> bool:
        """Enforces a contextual mention of the local area beyond simple keyword presence."""
        context_terms = self.local_context_terms.get(language, [])
        text_lower = text.lower()

        if area.lower() not in text_lower:
            return False

        return any(term.lower() in text_lower for term in context_terms)

    def validate_paragraph_structure(self, text: str) -> bool:
        """
        Validates that each paragraph contains between 1 and 4 sentences.
        Returns False if any paragraph (non-list/table) violates this upper limit.
        """
        if not text:
            return True

        paragraphs = [p.strip() for p in text.split("\n\n") if len(p.strip()) > 30]

        for p in paragraphs:
            # Skip architectural elements
            if p.startswith("|") or p.startswith("- ") or p.startswith("* ") or p.startswith("#"):
                continue

            # Split sentences using custom regex
            sentences = self._extract_sentences(p)
            
            # User requirement: no huge wall-of-text paragraphs (max 4 sentences)
            if len(sentences) > 4:
                return False

        return True

    async def _verify_external_link(self, url: str) -> bool:
        """Asynchronously checks if a URL is reachable and functional."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                response = await client.head(url)
                # If HEAD fails, try GET (some servers block HEAD)
                if response.status_code >= 400:
                    response = await client.get(url)
                return 200 <= response.status_code < 400
        except Exception as e:
            logger.warning(f"Failed to verify external link {url}: {e}")
            return False

    def validate_local_seo(self, markdown: str, meta: dict, area: str):
        if not area:
            return True, []

        issues = []
        lower_md = markdown.lower()
        area_lower = area.lower()

        first_100 = " ".join(markdown.split()[:100]).lower()

        if area_lower not in first_100:
            issues.append("Local area missing in first 100 words")

        if area_lower not in lower_md.split("\n")[0]:
            issues.append("Local area missing in H1")

        if area_lower not in meta.get("meta_title", "").lower():
            issues.append("Local area missing in Meta Title")

        if area_lower not in meta.get("meta_description", "").lower():
            issues.append("Local area missing in Meta Description")

        return len(issues) == 0, issues

    def validate_content_angle(self, markdown: str, strategy: dict):
        angle = strategy.get("primary_angle")
        if not angle:
            return True, None

        h2s = re.findall(r'^##\s+(.*)', markdown, re.MULTILINE)

        if not h2s:
            return False, "No H2 found"

        if angle.lower() not in h2s[0].lower():
            return False, "Content angle not reflected in first H2"

        return True, None

    def validate_outline(outline, article_type):
        required = REQUIRED_STRUCTURE_BY_TYPE[article_type]["mandatory"]
        existing = {section["section_type"] for section in outline}

        missing = required - existing

        if missing:
            raise StructureError(f"Missing mandatory sections: {missing}")

    def _canon_url(self, url: str) -> str:
        if not url:
            return ""
        u = str(url).strip()
        u = re.sub(r"#.*$", "", u)
        u = re.sub(r"\?.*$", "", u)
        return u.rstrip("/").lower()

    def _domain(self, url: str) -> str:
        try:
            return urlparse(url).netloc.lower().replace("www.", "")
        except Exception:
            return ""

    def _is_same_site(self, url: str, brand_url: str) -> bool:
        if not url or not brand_url:
            return False
        d1 = self._domain(url)
        d2 = self._domain(brand_url)
        return d1 == d2 or d1.endswith("." + d2) or d2.endswith("." + d1)

    def _extract_competitor_domains(self, serp_data: Dict[str, Any], brand_url: str = "") -> set:
        blocked = set()
        brand_domain = self._domain(brand_url)
        for r in serp_data.get("top_results", []):
            if isinstance(r, dict):
                d = self._domain(r.get("url", ""))
                if d and d != brand_domain:
                    blocked.add(d)
        return blocked

    def _is_authority_domain(self, domain: str, allowed_domains: set) -> bool:
        if not domain:
            return False
        if domain in allowed_domains:
            return True
        return domain.endswith(".gov") or domain.endswith(".gov.sa") or domain.endswith(".edu") or domain.endswith(".org")
        
    def _sanitize_section_links(self, content: str, state: Dict[str, Any], brand_url: str, max_external: int = None) -> str:
        if not content:
            return content

        if brand_url in {"None", "", None}:
            brand_url = ""

        if "used_all_urls" not in state:
            state["used_all_urls"] = set()
            for u in state.get("used_internal_links", []):
                state["used_all_urls"].add(self._canon_url(u))
            for u in state.get("used_external_links", []):
                state["used_all_urls"].add(self._canon_url(u))

        internal_set = state.get("internal_url_set", set()) or set()
        blocked_domains = state.get("blocked_external_domains", set()) or set()

        # Track external links specifically for this section to avoid cramming too many at once
        section_external_count = 0 
        
        # Max global external limits
        global_used_external_count = len(state.get("used_external_links", []))
        if max_external is None:
            max_external = state.get("max_external_links", 6)

        pattern = r'\[([^\]]+)\]\(([^)]+)\)'

        def repl(m):
            nonlocal section_external_count, global_used_external_count
            text, raw_url = m.group(1), m.group(2).strip()

            if raw_url.lower() in {"none", "null", ""}:
                return text

            if not raw_url.startswith("http"):
                return text

            cu = self._canon_url(raw_url)
            dom = self._domain(cu)

            # 1. Global Uniqueness Check (CRITICAL)
            if cu in state["used_all_urls"]:
                return text

            # 2. Category logic
            is_internal = cu in internal_set or (brand_url and self._is_same_site(cu, brand_url))

            if is_internal:
                state["used_all_urls"].add(cu)
                return f"[{text}]({raw_url})"
            else:
                # External checks
                if dom in blocked_domains:
                    return text
                    
                # Limit external links per section to 2 max, and global to max_external
                if section_external_count >= 2:
                    return text
                
                if global_used_external_count >= max_external:
                    return text
                
                state["used_all_urls"].add(cu)
                section_external_count += 1
                global_used_external_count += 1
                return f"[{text}]({raw_url})"

        cleaned = re.sub(pattern, repl, content)
        return cleaned
    
    def _normalize_url_for_dedup(self, url: Any) -> str:
        """Normalize URL for deduplication by removing trailing slashes, fragments, and queries."""
        import urllib.parse
        if not url:
            return ""
        
        if isinstance(url, dict):
            url = url.get("url") or url.get("link", "")
            if not url:
                return ""
        
        try:
            url = str(url).strip()
            parsed = urllib.parse.urlparse(url)
            # Remove www., force lowercase for netloc
            netloc = parsed.netloc.lower()
            if netloc.startswith("www."):
                netloc = netloc[4:]
            
            path = parsed.path
            # Remove trailing slash from path unless it's just "/"
            if len(path) > 1 and path.endswith("/"):
                path = path[:-1]
                
            # Ignore schema, query, and fragment for deduplication purposes
            normalized = f"{netloc}{path}"
            return normalized
        except Exception:
            return url.strip().lower()

    def _deduplicate_links_in_markdown(self, markdown_text: str, brand_domain: str = "", max_internal: int = 6) -> str:
        """
        Final safety gate for link quality:
        1. Deduplicates by URL (same URL -> keep first only).
        2. Deduplicates by anchor text (same anchor -> second occurrence becomes plain text).
        3. Strips dates and numbers from AI-generated anchor text.
        4. Removes generic/useless anchors.
        5. Enforces a hard cap of max_internal internal links.
        """
        import re
        if not markdown_text:
            return markdown_text

        # Patterns for bad anchor content
        date_pattern = re.compile(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b|\b(19|20)\d{2}\b')
        # Anchors that are ONLY numbers/dates after cleaning
        numeric_only = re.compile(r'^[\d\s/.,:-]+$')
        generic_anchors = {"click here", "read more", "learn more", "lets talk",
                           "let's talk", "contact us", "see all", "here",
                           "this page", "this article", "اقرأ أكثر", "انقر هنا",
                           "websites", "مواقع", "projects", "portfolio"}

        def clean_anchor_text(text: str) -> str:
            cleaned = date_pattern.sub('', text).strip()
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()
            return cleaned

        link_pattern = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')
        seen_urls = set()
        seen_anchors = set()
        internal_count = 0
        
        # We'll track links per H2 section to enforce the "max 1 per section" rule
        links_in_current_h2 = 0

        def is_internal(url):
            return brand_domain and brand_domain.lower() in url.lower()

        def is_seo_valuable(url):
            # Same filter as discovery
            junk_slugs = {'contact', 'about', 'login', 'signup', 'account', 'cart', 'checkout', 'privacy', 'terms', 'help', 'faq'}
            path = urlparse(url).path.lower().rstrip('/')
            last_segment = path.split('/')[-1]
            return last_segment not in junk_slugs

        def replace_func(match):
            nonlocal internal_count, links_in_current_h2
            anchor_raw = match.group(1)
            url = match.group(2).strip()

            anchor = anchor_raw.strip()
            if not anchor:
                return ""

            # 1. Global Physical Duplicate Check with language normalization
            core_url = self._normalize_url_for_dedup(url)
            if core_url in seen_urls:
                return anchor

            # 2. Section Limit Check: Max 2 internal links per H2 block
            if is_internal(url):
                if links_in_current_h2 >= 2:
                    return anchor # Show as text if section already has a link
            
            # 3. SEO Value Check
            if is_internal(url) and not is_seo_valuable(url):
                return anchor

            # 4. Global Cap Check
            if is_internal(url) and internal_count >= max_internal:
                return anchor

            # 5. Anchor Uniqueness Check
            anchor_key = anchor.lower().strip()
            if anchor_key in seen_anchors:
                return anchor
            
            # Success: Mark as seen and increment counters
            seen_urls.add(core_url)
            seen_anchors.add(anchor_key)
            if is_internal(url):
                internal_count += 1
                links_in_current_h2 += 1
            
            return f"[{anchor_raw}]({url})"

        # To enforce "per section", we split by H2 and process each chunk separately
        parts = re.split(r'(^##\s+.*)', markdown_text, flags=re.MULTILINE)
        processed_parts = []
        for part in parts:
            if part.startswith('##'):
                links_in_current_h2 = 0 # Reset counter for the new H2 block
                processed_parts.append(part)
            else:
                processed_parts.append(link_pattern.sub(replace_func, part))

        return "".join(processed_parts)

    # async def _step_4_validate_sections(self, state):
    #     input_data = state.get("input_data", {})
    #     title = input_data.get("title", "Untitled")
    #     article_language = input_data.get("article_language", "ar")

    #     sections = state.get("sections", {})
    #     outline = state.get("outline", [])

    #     failed_sections = []

    #     for sec in outline:
    #         sid = sec.get("section_id")
    #         content = sections.get(sid, {}).get("generated_content", "")

    #         if not content:
    #             continue

    #         word_count = len(content.split())
    #         min_words = sec.get("estimated_word_count_min", 0)
    #         max_words = sec.get("estimated_word_count_max", 99999)

    #         if not (min_words <= word_count <= max_words):
    #             sections[sid]["validation_report"] = {
    #                 "status": "FAIL",
    #                 "issues": [
    #                     f"Word count {word_count} خارج النطاق ({min_words}-{max_words})"
    #                 ]
    #             }

    #             failed_sections.append({
    #                 "section_id": sid,
    #                 "issues": sections[sid]["validation_report"]["issues"]
    #             })
    #             continue  

    #         result = await self.section_validator.validate(
    #             title,
    #             article_language,
    #             sec,
    #             content
    #         )

    #         sections[sid]["validation_report"] = result

    #         if result["status"].upper() == "FAIL":
    #             failed_sections.append({
    #                 "section_id": sid,
    #                 "issues": result.get("issues", [])
    #             })

    #     state["sections"] = sections
    #     state["failed_sections"] = failed_sections
    #     state["validation_passed"] = len(failed_sections) == 0

    #     return state


    # async def _step_semantic_layer(self, state):

    #     primary_keyword = state["primary_keyword"]
    #     with open("prompts/templates/seo_semantic_layer.txt") as f:
    #         template = Template(f.read())

    #     prompt = template.render(
    #         primary_keyword=primary_keyword
    #     )

    #     raw = await self.ai_client.send_with_web(
    #         prompt,
    #         max_results= 5
    #     )

    #     clean = re.sub(r"```json|```", "", raw).strip()
    #     semantic_data = recover_json(clean) or {}

    #     # merge into existing serp_data
    #     serp_data = state.get("serp_data", {})

    #     if semantic_data.get("paa_questions"):
    #         serp_data["paa_questions"] = semantic_data["paa_questions"]

    #     if semantic_data.get("related_searches"):
    #         serp_data["related_searches"] = semantic_data["related_searches"]

    #     if semantic_data.get("lsi_keywords"):
    #         serp_data["lsi_keywords"] = semantic_data["lsi_keywords"]

    #     if semantic_data.get("autocomplete_suggestions"):
    #         serp_data["autocomplete_suggestions"] = semantic_data["autocomplete_suggestions"]

    #     state["serp_data"] = serp_data

    #     return state