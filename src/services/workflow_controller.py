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
import traceback
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from langdetect import detect  
from jinja2 import Environment, FileSystemLoader, Template, StrictUndefined
import hashlib
import requests
from typing import Dict, Any, List, Optional, Callable, ClassVar
from collections import Counter
from langdetect import detect_langs, DetectorFactory
from src.services.image_generator import ImageGenerator, ImagePromptPlanner
from src.services.openrouter_client import OpenRouterClient
from src.schemas.input_validator import normalize_urls
from src.utils.injector import DataInjector
# from services.groq_client import GroqClient
# from services.gemini_client import GeminiClient
# from services.huggingface_client import HuggingFaceClient
from src.services.title_generator import TitleGenerator
from src.services.content_generator import OutlineGenerator, SectionWriter, Assembler, ContentGeneratorError, FinalHumanizer
# from services.section_validator import SectionValidator
from src.services.image_inserter import ImageInserter
from src.services.meta_schema_generator import MetaSchemaGenerator
from src.services.article_validator import ArticleValidator
from src.utils.json_utils import recover_json
# from src.utils.json_repair import recover_json # Prefer json_utils unless repair is needed
from src.utils.observability import ObservabilityTracker
from src.utils.seo_utils import enforce_meta_lengths
from src.utils.html_renderer import render_html_page
from src.utils.workflow_logger import WorkflowLogger
from src.utils.link_manager import LinkManager
from src.services.research_service import ResearchService
from src.services.strategy_service import StrategyService
from src.services.validation_service import ValidationService
from src.services.semantic_service import SemanticService
BASE_DIR = Path(__file__).resolve().parents[2] 


# Custom errors
class StructureError(Exception):
    pass

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
                    model = new_state.get("last_step_model", "unknown")
                    workflow_logger.end_step(
                        step_name=f"STEP_TOTAL: {step_name}",
                        start_time=start_time,
                        prompt=new_state.get("last_step_prompt"),
                        response=new_state.get("last_step_response"),
                        tokens=tokens,
                        model=model
                    )
                
                if self.observer:
                    self.observer.log_workflow_step(step_name, duration)
                logger.info(f"--- Finished Step: {step_name} (Duration: {duration:.2f}s) ---")
                return {"status": "success", "step": step_name, "duration": duration, "data": new_state}
            
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"Error in step '{step_name}' attempt {attempt + 1}: {e}")
                
                if workflow_logger:
                    # Log to the technical errors.txt file
                    tb_str = traceback.format_exc()
                    workflow_logger.log_technical_error(
                        step_name=step_name,
                        error_msg=str(e),
                        traceback_str=tb_str
                    )
                    
                    workflow_logger.log_step_details(
                        step_name=step_name,
                        duration=duration,
                        input_data=state,
                        error=str(e)
                    )
                
                attempt += 1
                if attempt <= retries:
                    await asyncio.sleep(0.1) # Reduced from 1s for better responsiveness
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
            template_path=BASE_DIR / "assets/prompts/templates/06_image_planner.txt"
            
        )
        self.env = Environment(
            loader=FileSystemLoader("assets/prompts/templates"),
            undefined=StrictUndefined
        )

        with open("assets/prompts/templates/00_intent_classifier.txt", "r", encoding="utf-8") as f:
            self.intent_template = Template(f.read(), undefined=StrictUndefined)
        
        # Semantic Intelligence Layer
        self.semantic_service = SemanticService()
        self.semantic_model = self.semantic_service.model
        
        # Content generation services
        self.title_generator = TitleGenerator(self.ai_client)
        self.outline_gen = OutlineGenerator(self.ai_client)
        self.section_writer = SectionWriter(self.ai_client)
        self.assembler = Assembler(self.ai_client)
        self.final_humanizer = FinalHumanizer(self.ai_client)
        self.image_inserter = ImageInserter()
        self.meta_schema = MetaSchemaGenerator(self.ai_client)
        self.article_validator = ArticleValidator(self.ai_client)
        self.research_service = ResearchService(self.ai_client, self.work_dir)
        self.strategy_service = StrategyService(
            ai_client=self.ai_client,
            title_generator=self.title_generator,
            jinja_env=self.env,
            intent_template=self.intent_template
        )
        self.validator = ValidationService(ai_client=self.ai_client, semantic_model=self.semantic_service)
        
        # Hardened Error Management: Essential steps that MUST succeed
        self.CRITICAL_STEPS = {
            "analysis_init",
            "brand_discovery",
            "web_research",
            "content_strategy",
            "outline_generation",
            "content_writing",
            "assembly"
        }

        # Image generator
        self.image_client = ImageGenerator(
            ai_client=self.ai_client,
            save_dir=os.path.join(work_dir, "assets/images"),
        )

    async def run_workflow(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Main entry point for the async pipeline."""
        self.observer.reset()
        # Initialize state keys
        state.setdefault("input_data", {})
        state.setdefault("seo_meta", {})
        state.setdefault("outline", [])
        state.setdefault("sections", {})
        state.setdefault("assets/images", [])
        state.setdefault("final_output", {})
        state.setdefault("content_type", "informational")
        state.setdefault("brand_link_used", False)
        state.setdefault("used_internal_links", [])
        state.setdefault("used_external_links", []) 
        state.setdefault("prohibited_competitors", [])
        state.setdefault("blocked_external_domains", set())
        state.setdefault("brand_name", "")
        state["max_external_links"] = 3
        state.setdefault("global_keyword_count", 0)
        state.setdefault("used_topics", [])
        state.setdefault("full_content_so_far", "")
        state.setdefault("brand_mentions_count", 0)

        steps = [
            # ("semantic_layer", self._step_semantic_layer, 1),
            ("analysis_init", self._step_0_init, 0),
            ("brand_discovery", self._step_brand_discovery_router, 1),
            ("web_research", self._step_web_research_router, 1),
            ("serp_analysis", self._step_serp_analysis_router, 1),
            ("intent_title", self.strategy_service.run_intent_title, 0),
            ("style_analysis", self.strategy_service.run_style_analysis, 1),
            ("content_strategy", self.strategy_service.run_content_strategy, 3),
            ("outline_generation", self._step_1_outline, 1),
            ("content_writing", self._step_2_write_sections, 1),
            ("humanizer", self._step_3_humanizer, 1),
        ]

        # Dynamic Image Skipping
        generate_images = state.get("generate_images", True)
        num_images = state.get("num_images", 7)
        
        if generate_images and num_images > 0:
            steps.extend([
                ("image_prompting", self._step_4_generate_image_prompts, 0),
                ("master_frame", self._step_4_1_generate_master_frame, 1),
                ("image_generation", self._step_4_5_download_images, 2),
            ])
        else:
            logger.info(f"Skipping image generation: generate_images={generate_images}, num_images={num_images}")

        steps.extend([
            # ("section_validation", self._step_4_validate_sections, 0),
            ("assembly", self._step_5_assembly, 0),
            ("final_humanizer", self._step_5_1_final_humanizer, 1),
        ])

        if generate_images and num_images > 0:
            steps.append(("image_inserter", self._step_6_image_inserter, 0))

        steps.extend([
            ("meta_schema", self._step_7_meta_schema, 0),
            ("article_validation", self._step_8_article_validation, 0),
            ("render_html", self._step_render_html, 0)
        ])
        for name, func, retries in steps:
            result = await self.executor.run_step(name, func, state, retries=retries)
            state = result.get("data", state)
            
            if result["status"] == "error":
                if name in self.CRITICAL_STEPS:
                    logger.error(f"Workflow stopped at critical step: {name}")
                    # Errors are already logged to errors.txt by AsyncExecutor
                    break
                else:
                    logger.warning(f"Non-critical step '{name}' failed technically. Continuing workflow to preserve output.")
                    # We continue to the next step
                    continue

        # Final Export
        if state.get("workflow_logger"):
            state["workflow_logger"].export_csv()
            state["workflow_logger"].export_diagnostic_report(state)

        return self._assemble_final_output(state)

    # ---------------- COORDINATION STEPS (ASYNC) ----------------
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
        article_language = self.strategy_service.resolve_article_language(raw_title, user_lang)
        area = input_data.get("area")
        state["area"] = area
        state["include_meta_keywords"] = input_data.get("include_meta_keywords", True)
        state["generate_images"] = input_data.get("generate_images", True)
        self.enable_images = state["generate_images"]
        # area_neighborhoods will be populated by AI in _step_0_brand_discovery
        state["area_neighborhoods"] = []
        state["article_language"] = article_language
        state["primary_keyword"] = primary_keyword
        state["raw_title"] = raw_title
        state["keywords"] = keywords
        
        # Dual-Mode / Advanced Customization
        state["workflow_mode"] = input_data.get("workflow_mode", "core")
        state["tone"] = input_data.get("tone")
        state["article_type"] = input_data.get("article_type")
        state["pov"] = input_data.get("pov")
        state["article_size"] = input_data.get("article_size") or "core_dynamic_expansion"
        state["brand_voice_description"] = input_data.get("brand_voice_description")
        
        state["include_conclusion"] = input_data.get("include_conclusion", True)
        state["include_faq"] = input_data.get("include_faq", True)
        state["include_tables"] = input_data.get("include_tables", True)
        state["include_bullet_lists"] = input_data.get("include_bullet_lists", True)
        state["include_comparison_blocks"] = input_data.get("include_comparison_blocks", True)
        state["bold_key_terms"] = input_data.get("bold_key_terms", True)
        
        state["num_images"] = input_data.get("num_images", 7)
        state["image_style"] = input_data.get("image_style", "illustration")
        state["image_size"] = input_data.get("image_size", "1024x1024")
        
        state["custom_keyword_density"] = input_data.get("custom_keyword_density")
        state["secondary_keywords"] = input_data.get("secondary_keywords", [])
        state["competitor_count"] = input_data.get("competitor_count", 5)
        state["min_external_links"] = max(0, int(input_data.get("min_external_links", 2)))
        
        state["logo_image"] = input_data.get("logo_image")
        state["reference_image"] = input_data.get("reference_image")
        state["brand_voice_guidelines"] = input_data.get("brand_voice_guidelines")
        state["brand_voice_examples"] = input_data.get("brand_voice_examples")

        
        # Derive brand_url from the FIRST URL provided in the UI list
        urls = state.get("input_data", {}).get("urls", [])
        external_urls = state.get("input_data", {}).get("external_urls", [])
        brand_url = urls[0].get("link") if urls else None
        state["brand_url"] = brand_url
        
        # PRE-INITIALIZE internal_resources with user-provided URLs
        state["internal_resources"] = []
        state["external_resources"] = []
        seen_canons = set()
        
        # Prioritize brand_url from internal_links if marked as brand
        brand_url = None
        for u in urls:
            if u.get("is_brand"):
                brand_url = u.get("link")
                break
        
        # If no brand_url found from is_brand, use the first URL as before
        if not brand_url and urls:
            brand_url = urls[0].get("link")
            
        state["brand_url"] = brand_url

        if brand_url:
            state["internal_resources"].append({
                "link": brand_url,
                "text": "Homepage",
                "is_manual": True,
                "is_homepage": True,
                "is_brand": True # Mark the primary brand URL as brand
            })
            seen_canons.add(LinkManager.canon_url(brand_url))
        
        for u in urls:
            link = u.get("link", "")
            if not link or not link.startswith("http"): continue
            
            # Skip if already seen (e.g., if it was the brand_url)
            canon = LinkManager.canon_url(link)
            if canon in seen_canons: continue
            
            state["internal_resources"].append({
                "link": link,
                "text": u.get("text", ""),
                "is_manual": True,
                "is_brand": u.get("is_brand", False)
            })
            seen_canons.add(canon)

        # Handle external URLs
        for u in external_urls:
            link = u.get("link", "")
            if not link or not link.startswith("http"): continue
            state["external_resources"].append({
                "link": link,
                "text": u.get("text", ""),
                "is_manual": True
            })
        
        # Helper for junk slugs (restore manual link protection)
        junk_slugs = {'contact', 'about', 'login', 'signup', 'account', 'cart', 'checkout', 'privacy', 'terms', 'help', 'faq'}
        def is_junk_init(url_str):
            try:
                from urllib.parse import urlparse
                path = urlparse(url_str).path.lower().rstrip('/')
                return path.split('/')[-1] in junk_slugs
            except: return False


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
        slug_base = LinkManager.sluggify(primary_keyword)
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

    # ---------------- ROUTING HELPERS (COST OPTIMIZATION) ----------------
    async def _step_brand_discovery_router(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Routes brand discovery.
        UNIFIED: Now always performs DEEP discovery to ensure maximum quality and internal link variety.
        """
        brand_url = state.get("brand_url")
        if not brand_url:
            logger.info("No brand URL provided. Skipping brand discovery.")
            return state

        logger.info(f"Enforcing DEEP Brand Discovery for quality stabilization (URL: {brand_url}).")
        return await self.research_service.run_brand_discovery(state)

    async def _step_web_research_router(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Consolidates research routing."""
        return await self.research_service.run_web_research(state)

    async def _step_serp_analysis_router(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Runs dedicated SERP analysis to extract intent and gaps."""
        return await self.research_service.run_serp_analysis(state)

    # Strategy methods migrated to StrategyService
    
    async def _step_1_outline(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Generates the article outline with a soft retry loop for validation failures."""
        
        input_data = state.get("input_data", {})
        title = input_data.get("title") or "Untitled"
        keywords = input_data.get("keywords") or []
        urls_raw = input_data.get("urls", [])
        urls_norm = []
        
        # We use state["internal_resources"] which was populated in brand_discovery
        # Junk link filter (avoid Contact, Login, etc.)
        junk_slugs = {'contact', 'about', 'login', 'signup', 'account', 'cart', 'checkout', 'privacy', 'terms', 'help'}
        
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
            canon = LinkManager.canon_url(r.get("link", ""))
            if not canon: continue
            if canon not in temp_map or (r.get("is_manual") and not temp_map[canon].get("is_manual")):
                temp_map[canon] = r
                
        deduplicated_internal_resources = list(temp_map.values())
        
        logger.info(f"Final internal pool: {len(deduplicated_internal_resources)} resources ({sum(1 for r in deduplicated_internal_resources if r.get('is_manual'))} manual, {sum(1 for r in deduplicated_internal_resources if not r.get('is_manual'))} discovered).")

        state["internal_url_set"] = set()
        for res in deduplicated_internal_resources:
            urls_norm.append({
                "text": res.get("text", "Internal Resource"), 
                "link": res.get("link"),
                "is_manual": res.get("is_manual", False)
            })
            canon = LinkManager.canon_url(res.get("link", ""))
            if canon:
                state["internal_url_set"].add(canon)

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

        mandatory = set(self.validator.REQUIRED_STRUCTURE_BY_TYPE[content_type]["mandatory"])

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
                mandatory_section_types=list(mandatory),
                prohibited_competitors=state.get("prohibited_competitors", []),
                # Advanced Customization
                article_size=state.get("article_size", "1000"),
                include_conclusion=state.get("include_conclusion", True),
                include_faq=state.get("include_faq", True),
                include_tables=state.get("include_tables", True),
                include_bullet_lists=state.get("include_bullet_lists", True),
                include_comparison_blocks=state.get("include_comparison_blocks", True),
                bold_key_terms=state.get("bold_key_terms", True),
                secondary_keywords=state.get("secondary_keywords", []),
                competitor_count=state.get("competitor_count", 5),
                external_resources=state.get("external_resources", []),
                style_blueprint=state.get("style_blueprint", {}),
                brand_name=state.get("brand_name", "")
            )


            
            # Store metadata for WorkflowLogger
            if "metadata" in outline_data:
                state["last_step_prompt"] = outline_data["metadata"]["prompt"]
                state["last_step_response"] = outline_data["metadata"]["response"]
                state["last_step_tokens"] = outline_data["metadata"]["tokens"]
                state["last_step_model"] = outline_data["metadata"].get("model", "unknown")

            if not outline_data or not outline_data.get("outline"):
                if attempt < 2:
                    feedback = "Outline generation returned empty result. Please provide a full, structured JSON outline."
                    continue
                raise RuntimeError("Outline generation returned empty result after 3 attempts.")
            
            outline = outline_data.get("outline", [])
            
            # Validation Layer
            errors = []
            
            # 0. FAQ Consolidation (Robustness)
            outline = self.validator.consolidate_faq(outline)
            
            # 1. Intent Distribution
            outline, dist_errors = self.validator.enforce_intent_distribution(
                outline,
                intent,
                content_type
            )
            errors.extend(dist_errors)

            # 2. Local SEO
            outline, local_errors = self.validator.inject_local_seo(outline, area)
            errors.extend(local_errors)

            # 3. Quality (Thin, Duplicates, CTAs)
            quality_errors = self.validator.validate_outline_quality(outline, content_type=content_type)
            errors.extend(quality_errors)

            if not errors:
                logger.info(f"Outline validated successfully on attempt {attempt + 1}.")
                break
            
            feedback = "Validation failed. Please correct the following issues and regenerate the outline:\n- " + "\n- ".join(errors)
            logger.warning(f"Outline validation failed (attempt {attempt + 1}): {feedback}")

        # 4. CTA Policy Enforcement (Budget & Strategic Distribution)
        outline = self.validator.enforce_cta_policy(outline, content_type)

        # Post-validation enhancements (non-critical, so we don't retry)
        outline = self.validator.enforce_outline_structure(
            outline,
            content_type=content_type
        )

        outline = self.validator.enforce_content_angle(
            outline,
            content_strategy
        )

        outline = self.validator.adjust_paa_by_intent(
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
        paa_check = self.validator.enforce_paa_sections(outline, paa_questions, min_percent=0.15)
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
            LinkManager.canon_url(u.get("link", ""))
            for u in urls_norm if u.get("link")
        }

        serp_data = state.get("serp_data", {})
        brand_url = state.get("brand_url", "")
        state["blocked_external_domains"] = LinkManager.extract_competitor_domains(
            serp_data, brand_url
        )
        # Authority domains are used as an allowlist for useful trust links.
        reference_links = serp_data.get("reference_authority_links", []) if isinstance(serp_data, dict) else []
        authority_domains = set()
        for item in reference_links:
            url = item.get("url") if isinstance(item, dict) else item
            dom = LinkManager.domain(url or "")
            if dom:
                authority_domains.add(dom)
        state["authority_domains"] = authority_domains
        
        # Extract brand names for the prohibited list
        prohibited_names = []
        for domain in state["blocked_external_domains"]:
            # Basic cleaning: webook.com -> Webook
            name = domain.split('.')[0].capitalize()
            if name and len(name) > 1:
                prohibited_names.append(name)
        
        state["prohibited_competitors"] = prohibited_names
        logger.info(f"Prohibited competitors identified: {state['prohibited_competitors']}")

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
                
                norm = LinkManager.normalize_url_for_dedup(url)
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

        # Distribute Primary Keyword Requirement (Pacing)
        # Distribute Primary Keyword Requirement (Pacing)
        total_secs = len(outline)
        if total_secs <= 3:
            # Sparse distribution for short articles: Opening for all, plus one footer for 3-section articles
            for i, sec in enumerate(outline):
                if i == 0:
                    sec["requires_primary_keyword"] = True
                elif i == 2 and total_secs == 3:
                    sec["requires_primary_keyword"] = True
                else:
                    sec["requires_primary_keyword"] = False
        else:
            # Phase-aware distribution: Opening, one key H2, and a later section (avoiding adjacency)
            target_indices = {0} # Always the opening
            
            # Find first real H2 after intro (if any)
            first_h2_idx = next((i for i, s in enumerate(outline) if i > 0 and s.get("heading_level") == "H2"), None)
            if first_h2_idx is not None:
                if first_h2_idx > 1: # Avoid adjacency with intro if possible
                    target_indices.add(first_h2_idx)
                else:
                    target_indices.add(min(total_secs - 1, 2)) # Push to next if 1 is adjacent to 0
            
            # Add one later section (Conclusion or last body)
            last_idx = total_secs - 1
            if last_idx not in target_indices and last_idx - 1 not in target_indices:
                target_indices.add(last_idx)
            elif last_idx - 1 > max(target_indices):
                target_indices.add(last_idx - 1)

            for i, sec in enumerate(outline):
                sec["requires_primary_keyword"] = (i in target_indices)

        # Initialize global quality tracking
        state["used_claims"] = []
        state["ctas_placed"] = 0
        state["tables_placed"] = 0
        state["full_content_so_far"] = ""
        state["last_section_content"] = ""

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
                    total_sections=len(outline),
                    global_keyword_count=state.get("global_keyword_count", 0),
                    brand_mentions_count=state.get("brand_mentions_count", 0)
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
                    total_sections=len(outline),
                    global_keyword_count=state.get("global_keyword_count", 0),
                    brand_mentions_count=state.get("brand_mentions_count", 0)
                )
                
                # Update quality state after each section in sequential mode
                if res and res.get("generated_content"):
                     # Update Full Content (Cumulative Memory)
                     state["full_content_so_far"] += "\n\n" + res["generated_content"]
                     # Update Last Section Content (For Logical Flow)
                     state["last_section_content"] = res["generated_content"]
                     
                     # Track CTAs using has_cta helper
                     def has_cta_local(text):
                         return bool(re.search(r'<a\b|<button\b|\[.*?\]\(https?://', text))
                     
                     if has_cta_local(res["generated_content"]):
                          state["ctas_placed"] = state.get("ctas_placed", 0) + 1
                     
                     # Track Tables (Max 2 rule)
                     if "|" in res["generated_content"] and re.search(r"\|\s*---\s*\|", res["generated_content"]):
                          state["tables_placed"] = state.get("tables_placed", 0) + 1

                     # Update global brand mention count
                     state["brand_mentions_count"] = state.get("brand_mentions_count", 0) + res.get("brand_mentions_count", 0)
                          
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
            if res.get("section_index") == 0:
                state["introduction_text"] = res.get("generated_content", "")

            # Update global keyword count
            primary_keyword = global_keywords.get("primary", "")
            if primary_keyword:
                full_text_for_search = (res.get("heading_text") or "") + "\n" + res.get("generated_content", "")
                pattern = r'\b{}\b'.format(re.escape(primary_keyword.lower()))
                matches = re.findall(pattern, full_text_for_search.lower())
                state["global_keyword_count"] = state.get("global_keyword_count", 0) + len(matches)

            # Update full content summary
            state["full_content_so_far"] = state.get("full_content_so_far", "") + "\n\n" + res.get("generated_content", "")
            
            # For parallel results, update the brand_mentions_count if not already updated in serial loop
            if PARALLEL_SECTIONS:
                state["brand_mentions_count"] = state.get("brand_mentions_count", 0) + res.get("brand_mentions_count", 0)


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
        total_sections: int = 1,
        global_keyword_count: int = 0,
        brand_mentions_count: int = 0
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
        allowed_domains = state.get("authority_domains", set())
        brand_domain = LinkManager.domain(state.get("brand_url", ""))
        
        for r in serp_results:
            url = r.get("url")
            if not url: continue
            dom = LinkManager.domain(url)
            if dom == brand_domain or dom in blocked_domains:
                continue
            # Accept only trusted domains: allowlist from SERP authority links,
            # or generally trusted TLDs (.gov/.edu/.org) via LinkManager.
            if not LinkManager.is_authority_domain(dom, allowed_domains):
                continue
            external_sources.append({"url": url, "text": r.get("title", "External Resource")})
            if len(external_sources) >= 8: # Cap to 8 sources
                break
        
        logger.info(f"Extracted {len(external_sources)} external sources for section '{section.get('heading_text')}'")
        
        # --- Runtime CTA Assignment ---
        # The outline generator and ValidationService now determine the strategic cta_eligible flag.
        # SectionWriter respects section.get('cta_eligible') and section.get('section_intent').
        cta_type = section.get("cta_type", "none")
            
        # --- Context Windowing (Token Optimization) ---
        # Instead of sending the entire article, send the Intro + last 2 sections.
        intro_text = state.get("introduction_text", "")
        # Get generated content of all sections written so far in order
        all_content = [s["generated_content"] for s in state.get("sections", {}).values() if "generated_content" in s]
        # Keep only the last 2 sections (excluding the one being written)
        recent_context = "\n\n".join(all_content[-2:]) if all_content else ""
        
        optimized_context = f"ARTICLE INTRODUCTION:\n{intro_text}\n\nRECENT CONTEXT:\n{recent_context}" if intro_text else recent_context

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
            allow_external_links=bool(external_sources),
            workflow_mode=state.get("workflow_mode", "core"),
            execution_plan=execution_plan,
            area=state.get("area"),
            used_phrases=used_phrases,
            used_internal_links=state.get("used_internal_links", []),
            used_external_links=state.get("used_external_links", []), 
            section_index=section_index,
            total_sections=total_sections,
            brand_context=brand_context,
            section_source_text=section_source_text,
            external_sources=external_sources,
            workflow_logger=state.get("workflow_logger"),
            prohibited_competitors=state.get("prohibited_competitors", []),
            cta_type=cta_type, # Pass the tiered strategy
            # Advanced CustomizationCustomization
            tone=state.get("tone"),
            pov=state.get("pov"),
            brand_voice_description=state.get("brand_voice_description"),
            brand_voice_guidelines=state.get("brand_voice_guidelines"),
            brand_voice_examples=state.get("brand_voice_examples"),
            custom_keyword_density=state.get("custom_keyword_density"),
            bold_key_terms=state.get("bold_key_terms", True),
            requires_primary_keyword=section.get("requires_primary_keyword", False),
            used_topics=state.get("used_topics", []),
            used_claims=state.get("used_claims", []),
            previous_section_text=state.get("last_section_content", ""),
            previous_content_summary=optimized_context, # Optimized Context!
            full_outline=state.get("outline", []),
            introduction_text=state.get("introduction_text", ""),
            external_resources=state.get("external_resources", []),
            brand_name=state.get("brand_name", ""),
            style_blueprint=state.get("style_blueprint", {}),
            ctas_placed=state.get("ctas_placed", 0),
            tables_placed=state.get("tables_placed", 0),
            serp_data=state.get("serp_data", {}),
            area_neighborhoods=state.get("area_neighborhoods", []),
            global_keyword_count=global_keyword_count,
            brand_mentions_count=brand_mentions_count
        )
        
        content = res_data.get("content", "")
        used_links = res_data.get("used_links", [])
        brand_link_used_in_sec = res_data.get("brand_link_used", False)
        
        # Store metadata for WorkflowLogger
        if "metadata" in res_data:
            state["last_step_prompt"] = res_data["metadata"]["prompt"]
            state["last_step_response"] = res_data["metadata"]["response"]
            state["last_step_tokens"] = res_data["metadata"]["tokens"]
            state["last_step_model"] = res_data["metadata"].get("model", "unknown")

        # Semantic Overlap Rejection
        if content and getattr(self, "semantic_model", None) and state.get("used_claims"):
            is_rejected, overlap_score, overlap_sentence = await self.validator.check_semantic_overlap(content, state.get("used_claims", []), threshold=0.60)
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
                    allow_external_links=bool(external_sources),
                    workflow_mode=state.get("workflow_mode", "core"),
                    execution_plan={
                        **execution_plan, 
                        "writing_mode": "refinement",
                        "structure_rule": "AVOID SEMANTIC OVERLAP. This content overlaps with previous sections. Edit it to provide a unique angle or deeper specific details that haven't been mentioned."
                    },
                    draft_to_fix=content,
                    area=state.get("area"),
                    used_phrases=used_phrases,
                    used_internal_links=state.get("used_internal_links", []),
                    used_external_links=state.get("used_external_links", []), 
                    section_index=section_index,
                    total_sections=total_sections,
                    brand_context=brand_context,
                    section_source_text=section_source_text,
                    external_sources=external_sources,
                    brand_name=state.get("brand_name", ""),
                    workflow_logger=state.get("workflow_logger"),
                    prohibited_competitors=state.get("prohibited_competitors", []),
                    full_outline=state.get("outline", []),
                    introduction_text=state.get("introduction_text", ""),
                    external_resources=state.get("external_resources", []),
                    style_blueprint=state.get("style_blueprint", {}),
                    used_claims=state.get("used_claims", []),

                    previous_section_text=state.get("last_section_content", ""),

                    previous_content_summary=optimized_context,
                    cta_type=cta_type, # Pass the tiered strategy on retry too
                    ctas_placed=state.get("ctas_placed", 0),
                    serp_data=state.get("serp_data", {}),
                    area_neighborhoods=state.get("area_neighborhoods", []),
                    brand_mentions_count=brand_mentions_count
                )
                content = res_data.get("content", "")
                used_links = res_data.get("used_links", [])
                brand_link_used_in_sec = res_data.get("brand_link_used", False)

        # Multi-Layer Paragraph Structure and Strict SEO Validation
        if content:
            is_valid, validation_errors = await self.validator.validate_section_output(
                content=content, 
                section=section, 
                section_index=section_index, 
                total_sections=total_sections, 
                area=state.get("area"),
                blocked_domains=state.get("blocked_external_domains", set()),
                brand_url=state.get("brand_url", ""),
                content_type=content_type
            )
            
            if not is_valid:
                error_msg = "; ".join(validation_errors)
                writing_mode = "creative rephrasing"
                structure_rule = f"CRITICAL ERRORS TO FIX: {error_msg}. EXACTLY 3-5 PARAGRAPHS."
                
                # Specialized Fallback for Conclusion CTAs
                if "MISSING_CONCLUSION_CTA" in error_msg:
                    writing_mode = "conclusion fallback"
                    structure_rule = (
                        "CRITICAL: YOUR PREVIOUS CONCLUSION WAS REJECTED FOR MISSING A CTA. "
                        "YOU MUST INCLUDE A STRONG, ACTION-ORIENTED CTA AT THE VERY END. "
                        f"Target Brand: {state.get('brand_name', 'The Brand')}. "
                        f"Target URL: {brand_url}. "
                        "Format: [Action Text](URL)."
                    )

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
                    allow_external_links=bool(external_sources),
                    execution_plan={
                        **execution_plan, 
                        "writing_mode": "refinement",
                        "structure_rule": structure_rule
                    },
                    draft_to_fix=content,
                    area=state.get("area"),
                    used_phrases=used_phrases,
                    used_internal_links=state.get("used_internal_links", []),
                    used_external_links=state.get("used_external_links", []),
                    section_index=section_index,
                    total_sections=total_sections,
                    brand_context=brand_context,
                    section_source_text=section_source_text,
                    external_sources=external_sources,
                    brand_name=state.get("brand_name", ""),
                    workflow_logger=state.get("workflow_logger"),
                    prohibited_competitors=state.get("prohibited_competitors", []),
                    full_outline=state.get("outline", []),
                    introduction_text=state.get("introduction_text", ""),
                    external_resources=state.get("external_resources", []),
                    style_blueprint=state.get("style_blueprint", {}),
                    used_claims=state.get("used_claims", []),

                    previous_section_text=state.get("last_section_content", ""),

                    previous_content_summary=state.get("full_content_so_far", ""),
                    ctas_placed=state.get("ctas_placed", 0),
                    serp_data=state.get("serp_data", {}),
                    brand_mentions_count=brand_mentions_count
                )
                content = res_data.get("content", "")
                used_links = res_data.get("used_links", [])
                brand_link_used_in_sec = res_data.get("brand_link_used", False)

        # --- ENTITY LOCKDOWN CHECK (REMOVED FOR CREATIVITY) ---
        # We now rely on the AI's natural expert knowledge and strict 'No Competitor' policy.

        # Repetition Guard (Retry Loop)
        if content:
            repeated = self.validator.detect_repetition(content, used_phrases)
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
                    allow_external_links=bool(external_sources),
                    execution_plan={**execution_plan, "writing_mode": "refinement"},
                    draft_to_fix=content,
                    area=state.get("area"),
                    used_phrases=used_phrases + repeated,
                    used_internal_links=state.get("used_internal_links", []),
                    used_external_links=state.get("used_external_links", []), 
                    section_index=section_index,
                    total_sections=total_sections,
                    brand_context=brand_context,
                    section_source_text=section_source_text,
                    external_sources=external_sources,
                    brand_name=state.get("brand_name", ""),
                    workflow_logger=state.get("workflow_logger"),
                    prohibited_competitors=state.get("prohibited_competitors", []),
                    full_outline=state.get("outline", []),
                    introduction_text=state.get("introduction_text", ""),
                    external_resources=state.get("external_resources", []),
                    style_blueprint=state.get("style_blueprint", {}),
                    used_claims=state.get("used_claims", []),

                    previous_section_text=state.get("last_section_content", ""),

                    previous_content_summary=state.get("full_content_so_far", ""),
                    ctas_placed=state.get("ctas_placed", 0),
                    serp_data=state.get("serp_data", {}),
                    brand_mentions_count=brand_mentions_count
                )
                content = res_data.get("content", "")
                used_links = res_data.get("used_links", [])
                brand_link_used_in_sec = res_data.get("brand_link_used", False)

        if content:
            new_sentences = self.validator.extract_sentences(content)
            state.setdefault("used_phrases", [])
            state.setdefault("used_claims", [])
            state.setdefault("used_internal_links", [])
            state.setdefault("used_external_links", [])
            # --- SEMANTIC MEMORY & KNOWLEDGE FIREWALL (CRITICAL) ---
            # Persist explicit AI knowledge units (High precision facts/topics)
            knowledge_units = res_data.get("knowledge_units_established") or res_data.get("topics_covered") or []
            if knowledge_units:
                for unit in knowledge_units:
                    if unit not in state["used_claims"]:
                        state["used_claims"].append(unit)
            
            # Fallback/Supplemental: Extract substantial sentences if no explicit units provided
            if not knowledge_units:
                substantial_sentences = [s for s in new_sentences if len(s) > 60] # Increased threshold to reduce noise
                state["used_claims"].extend(substantial_sentences)
            
            # Also sync to used_topics for legacy monitoring
            if knowledge_units:
                state.setdefault("used_topics", [])
                state["used_topics"].extend(knowledge_units)
            # ----------------------------------------------

            transformed_content = LinkManager.sanitize_section_links(
                content=content,
                state=state,
                brand_url=brand_url or "",
                max_external=2 # Increased to allow 3-4 across article
            )

            res_data["content"] = transformed_content
            content = transformed_content

            logger.info(f"Section '{section.get('heading_text')}' finalized. Current external links in state: {len(state.get('used_external_links', []))}")
            if state.get("workflow_logger"):
                state["workflow_logger"].log_event(f"Section Finalized: {section.get('heading_text')}", {
                    "external_links_count": len(state.get("used_external_links", [])),
                    "internal_links_count": len(state.get("used_internal_links", []))
                })

            # classify links after sanitize
            found_links = re.findall(r'\[.*?\]\((https?://.*?)\)', content)
            for link in found_links:
                cu = LinkManager.canon_url(link)
                if cu in state.get("internal_url_set", set()) or LinkManager.is_same_site(cu, brand_url or ""):
                    if cu not in state["used_internal_links"]:
                        state["used_internal_links"].append(cu)
                else:
                    if cu not in state["used_external_links"]:
                        state["used_external_links"].append(cu)

            # update brand link flag
            if brand_url:
                if any(LinkManager.is_same_site(l, brand_url) for l in found_links):
                    state["brand_link_used"] = True

            final_content = self.validator.enforce_paragraph_structure(content)

            # Count brand mentions in finalized content
            brand_name = state.get("brand_name", "")
            mentions_in_section = 0
            if brand_name and final_content:
                # Use word boundaries or just count occurrences
                pattern = r'\b{}\b'.format(re.escape(brand_name.lower()))
                mentions_in_section = len(re.findall(pattern, final_content.lower()))
                
                # In Arabic, word boundaries might be tricky with prefixes. Let's do a direct count as fallback if word boundaries fail, but regex with \b works decently.
                if mentions_in_section == 0 and brand_name.lower() in final_content.lower():
                     mentions_in_section = final_content.lower().count(brand_name.lower())

            return {
                **section,
                "section_id": section_id,
                "generated_content": final_content,
                "used_links": found_links,
                "brand_link_used": state.get("brand_link_used", False),
                "brand_mentions_count": mentions_in_section
            }
        return None
    
    async def _step_4_generate_image_prompts(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Generates image prompts using the image client."""
        if not self.enable_images:
            logger.info("Image pipeline skipped (disabled in state).")
            state["image_prompts"] = []
            return state

        input_data = state.get("input_data", {})
        title = input_data.get("title", "Untitled")
        keywords = input_data.get("keywords", [])
        outline = state.get("outline", [])
        primary_keyword = state.get("primary_keyword")
        brand_visual_style = state.get("brand_visual_style", "")

        # Zero out previous step tokens to prevent token leakage in metrics log
        state["last_step_tokens"] = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

        # FIX: generate() returns a plain list, not a dict with 'assets/prompts' key
        image_prompts = await self.image_prompt_planner.generate(
            title=title,
            primary_keyword=primary_keyword,
            keywords=keywords,
            outline=outline,
            brand_visual_style=brand_visual_style
        )

        # image_prompts is already a list — no .get() needed
        if not isinstance(image_prompts, list):
            logger.error(f"image_prompt_planner.generate returned unexpected type: {type(image_prompts)}")
            image_prompts = []

        logger.info(f"FINAL IMAGE PROMPTS COUNT: {len(image_prompts)}")

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
        if not self.enable_images:
            return state

        logo_path = state.get("input_data", {}).get("logo_image_path") or state.get("logo_path")
        brand_colors = state.get("brand_colors", [])
        
        if not logo_path or not brand_colors:
            logger.info("Skipping Master Frame generation: No logo or brand colors found.")
            return state

        color_str = ", ".join(brand_colors)
        primary_keyword = state.get("primary_keyword") or state.get("input_data", {}).get("primary_keyword", "Professional Business")
        
        # Design a prompt for a functional 'Picture Frame' border
        # Use a simplified keyword for the frame to avoid content leakage
        simple_keyword = primary_keyword.split(',')[0].strip()[:30]
        
        frame_prompt = f"""Minimalist 'Bottom Wave' corporate template for {simple_keyword}.
        Create a clean, professional horizontal 16:9 template.
        Design a VERY SUBTLE, thin artistic wave or curve strictly at the BOTTOM 10% of the image using {color_str}.
        The remaining 90% of the image MUST be a PERFECTLY FLAT, SOLID, PURE WHITE CANVAS (RGB 255,255,255).
        STRICTLY: NO BACKGROUND IMAGES, NO SCENES, NO CONTENT, NO PEOPLE, NO TEXT, NO ICONS.
        Only a pure white empty top area and a thin {color_str} wave at the very bottom edge.
        The design should be extremely clean, like a blank high-end professional header/footer paper."""

        logger.info(f"Generating Master Frame with colors: {color_str}")
        
        # We use a single generation for the Master Frame
        try:
            # Create a temporary 'prompt' object for the image client
            frame_prompt_obj = {
                "prompt": frame_prompt,
                "alt_text": "Master Brand Frame",
                "image_type": "MasterFrame",
                "section_id": "master_frame"
            }
            
            output_dir = state.get("output_dir", self.work_dir)
            frames_dir = os.path.join(output_dir, "assets/images")
            os.makedirs(frames_dir, exist_ok=True)
            
            self.image_client.save_dir = frames_dir
            master_frame_res = await self.image_client.generate_images(
                [frame_prompt_obj],
                primary_keyword=primary_keyword,
                workflow_logger=state.get("workflow_logger")
            )
            
            if master_frame_res and "local_path" in master_frame_res[0]:
                raw_frame_path = os.path.abspath(master_frame_res[0]["local_path"])
                
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
        if not self.enable_images:
            state["assets/images"] = []
            return state

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
        images_dir = os.path.join(output_dir, "assets/images")
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
                img["local_path"] = f"assets/images/{os.path.basename(img['local_path'])}"

        state["assets/images"] = images
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
                similarity = self.validator.calculate_similarity(content, prev_content)
                if similarity > 0.7:
                    logger.warning(f"High similarity ({similarity:.2f}) detected between section '{section.get('heading_text')}' and a previous section. Flagging for pruning.")
                    is_redundant = True
                    break
            
            # Prune redundant intros anyway for consistent quality
            section["generated_content"] = self.validator.prune_redundant_intros(content)
            final_sections.append(section)

        assembled = await self.assembler.assemble(
            title=title, 
            sections=final_sections, 
            article_language=article_language,
            content_type=state.get("content_type", "informational")
        )
        
        # Final pass redundancy pruning on the whole assembled markdown
        if "final_markdown" in assembled:
            md = assembled["final_markdown"]
            # Mechanical Paragraph Deduplication (Fail-safe)
            md = self.validator.deduplicate_paragraphs_in_markdown(md, threshold=0.85)
            md = self.validator.prune_redundant_intros(md)
            brand_url = state.get("brand_url", "")
            brand_domain = LinkManager.domain(brand_url) if brand_url else ""
            md = LinkManager.deduplicate_links_in_markdown(md, brand_domain=brand_domain, max_internal=6)

            assembled["final_markdown"] = md

        state["final_output"] = assembled
        return state

    async def _step_5_1_final_humanizer(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Post-processes the entire assembled article section by section."""
        draft_markdown = state.get("final_output", {}).get("final_markdown", "")
        if not draft_markdown:
            return state
            
        outline = state.get("outline", [])
        sections_dict = state.get("sections", {})
        ordered_sections = [
            sections_dict[s["section_id"]]
            for s in outline
            if s.get("section_id") in sections_dict
        ]
        
        article_language = state.get("article_language") or state.get("input_data", {}).get("article_language", "ar")
        brand_name = state.get("brand_name", "")
        brand_source_text = state.get("input_data", {}).get("brand_source_text", "")
        weaponized_usps = state.get("seo_intelligence", {}).get("weaponized_usps", "")

        for i, section in enumerate(ordered_sections):
            content = section.get("generated_content", "")
            heading = section.get("heading_text", "")
            is_intro = (i == 0)
            is_conclusion = (section.get("section_type") == "conclusion" or i == len(ordered_sections) - 1)
            
            # --- DYNAMIC CONTEXT REBUILD ---
            # Rebuild the draft text on each iteration so the Humanizer sees the live updates
            live_draft_parts = []
            for s in ordered_sections:
                lvl = str(s.get("heading_level", "H2")).replace("H", "")
                lvl_num = int(lvl) if lvl.isdigit() else 2
                if s.get("section_type") != "introduction":
                    live_draft_parts.append(f"{'#' * lvl_num} {s.get('heading_text', '')}")
                live_draft_parts.append(s.get("generated_content", ""))
            
            dynamic_draft = "\n\n".join(live_draft_parts)

            logger.info(f"Humanizing section: {heading}")
            try:
                new_content = await self.final_humanizer.humanize_section(
                    full_article_context=dynamic_draft,
                    target_section_content=content,
                    target_section_heading=heading,
                    article_language=article_language,
                    brand_name=brand_name,
                    brand_source_text=brand_source_text,
                    weaponized_usps=weaponized_usps,
                    section=section,
                    is_introduction=is_intro,
                    is_conclusion=is_conclusion,
                    brand_mentions_total_count=state.get("brand_mentions_count", 0)
                )
                if new_content:
                    section["generated_content"] = new_content
            except Exception as e:
                logger.error(f"Humanization failed for section '{heading}': {e}. Falling back to original.")
            
        # Re-assemble the article after humanization
        title = state.get("input_data", {}).get("title", "Untitled")
        assembled = await self.assembler.assemble(
            title=title, 
            sections=ordered_sections, 
            article_language=article_language,
            content_type=state.get("content_type", "informational")
        )
        
        # Final pass redundancy pruning on the whole assembled markdown
        if "final_markdown" in assembled:
            md = assembled["final_markdown"]
            # Mechanical Paragraph Deduplication (Final Safety Gate)
            md = self.validator.deduplicate_paragraphs_in_markdown(md, threshold=0.85)
            md = self.validator.prune_redundant_intros(md)
            brand_url = state.get("brand_url", "")
            brand_domain = LinkManager.domain(brand_url) if brand_url else ""
            md = LinkManager.deduplicate_links_in_markdown(md, brand_domain=brand_domain, max_internal=6)
            assembled["final_markdown"] = md
            
            # Final Article-Level CTA Budget Validation
            word_count = len(md.split())
            is_budget_ok, budget_error = self.validator.validate_article_cta_budget(
                full_markdown=md,
                word_count=word_count,
                content_type=state.get("content_type", "informational")
            )
            if not is_budget_ok:
                logger.warning(f"[cta_budget] {budget_error}")
                # We don't fail the article here, but we log the warning for transparency.
                # In the future, this could trigger a pruning pass.
            
        state["final_output"] = assembled
        return state

    async def _step_6_image_inserter(self, state):
        final_md = state.get("final_output", {}).get("final_markdown", "")
        images = state.get("assets/images", [])

        if not final_md or not images:
            return state

        new_md = await self.image_inserter.insert(final_md, images)
        # Run a second dedup pass after image insertion to catch any links added by images
        brand_url = state.get("brand_url", "")
        brand_domain = LinkManager.domain(brand_url) if brand_url else ""
        new_md = LinkManager.deduplicate_links_in_markdown(new_md, brand_domain=brand_domain, max_internal=6)
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
            article_language=state.get("article_language") or state.get("input_data", {}).get("article_language", "en"),
            state=state,
            secondary_keywords=state.get("input_data", {}).get("keywords", []),
            include_meta_keywords=state.get("include_meta_keywords", False),
            article_url=state.get("final_url"),
            images=state.get("assets/images", []),
            word_count=len(final_md.split())
        )

        meta_json = recover_json(meta_raw)

        if not meta_json:
            logger.error("Meta schema returned invalid JSON")
            return state

        meta_json = enforce_meta_lengths(meta_json)

        # Deterministic fallback so HTML never ships with empty schema blocks.
        if not meta_json.get("article_schema"):
            logger.warning("Meta schema missing article_schema. Building deterministic fallback schema.")
            meta_json["article_schema"] = {
                "@context": "https://schema.org",
                "@type": "Article",
                "headline": meta_json.get("meta_title") or state.get("input_data", {}).get("title", ""),
                "description": meta_json.get("meta_description", ""),
                "author": {"@type": "Organization", "name": state.get("brand_name") or "Editorial Team"},
                "publisher": {
                    "@type": "Organization",
                    "name": state.get("brand_name") or "Editorial Team",
                    "logo": {"@type": "ImageObject", "url": state.get("logo_path", "")}
                },
                "mainEntityOfPage": {"@type": "WebPage", "@id": state.get("final_url", "")},
                "url": state.get("final_url", ""),
                "datePublished": datetime.now().date().isoformat(),
                "dateModified": datetime.now().date().isoformat(),
                "image": [img.get("url") or img.get("local_path") for img in state.get("assets/images", []) if isinstance(img, dict)],
                "articleSection": state.get("content_type", "article"),
                "wordCount": len(final_md.split())
            }

        # Enforce H1 Length (Strict)
        h1 = meta_json.get("h1", "")
        if h1 and not self.validator.validate_h1_length(h1):
            logger.warning(f"H1 length invalid ({len(h1)} chars). Falling back to explicit title.")
            meta_json["h1"] = state.get("input_data", {}).get("title", h1)
            
        state["seo_meta"] = meta_json
        return state

    async def _step_8_article_validation(self, state):

        final_md = state.get("final_output", {}).get("final_markdown", "")
        meta = state.get("seo_meta", {})
        images = state.get("assets/images", [])
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
        
        
        # Safeguard link hygiene (final article level)
        final_md = LinkManager.sanitize_links(
            final_md,
            max_external=input_data.get("max_external_links", 6),
            max_brand=6,
            brand_url=state.get("brand_url"),
            internal_url_set=state.get("internal_url_set"),
            blocked_domains=state.get("blocked_external_domains"),
            allowed_domains=state.get("authority_domains")
        )
        critical_issues = []
        warnings = []

        # Trust-link floor: target at least N useful external links when available.
        min_external_links = state.get("min_external_links", 2)
        if min_external_links > 0:
            all_links = re.findall(r'\[.*?\]\((https?://.*?)\)', final_md)
            brand_url = state.get("brand_url", "")
            blocked = state.get("blocked_external_domains", set()) or set()
            external_unique = set()
            for link in all_links:
                dom = LinkManager.domain(link)
                if not dom:
                    continue
                if brand_url and LinkManager.is_same_site(link, brand_url):
                    continue
                if dom in blocked:
                    continue
                external_unique.add(LinkManager.canon_url(link))
            if len(external_unique) < min_external_links:
                warnings.append(
                    f"EXTERNAL_TRUST_LINKS_ADVISORY: Found {len(external_unique)} useful external links. "
                    f"Target is at least {min_external_links}."
                )

        state["final_output"]["final_markdown"] = final_md

        word_count, keyword_count, keyword_density = self.validator.calculate_keyword_stats(
            final_md,
            primary_keyword
        )

        # Heuristic checks
        ok, issue = self.validator.validate_sales_intro(final_md, state.get("intent"))
        if not ok:
            critical_issues.append(issue)

        if state.get("content_type") == "brand_commercial":
            structural_intel = state.get("seo_intelligence", {}).get("strategic_analysis", {}).get("structural_intelligence", {})
            # article_language = state.get("article_language", "en")
            article_language = state.get("article_language") or state.get("input_data", {}).get("article_language", "en")
            
            is_dense_enough = self.validator.calculate_sales_density(
                final_md, 
                state.get("intent"), 
                article_language, 
                structural_intel
            )
            
            if not is_dense_enough:
                intensity = structural_intel.get("cta_intensity_pattern", "soft commercial")
                critical_issues.append(f"Sales density too low for {intensity} mode")

        ok, local_issues = self.validator.validate_local_seo(
            final_md,
            meta,
            state.get("area")
        )
        critical_issues.extend(local_issues)

        # Enforce Contextual Local SEO (Warning only, don't waste tokens)
        area = state.get("area")
        if area:
            if not self.validator.validate_local_context(final_md, area, article_language):
                msg = f"Weak local contextualization for area '{area}'"
                logger.warning(msg)
                warnings.append(msg)

        ok, angle_issue = self.validator.validate_content_angle(
            final_md,
            state.get("content_strategy", {})
        )
        if not ok:
            warnings.append(angle_issue)

        # Enforce Final CTA in Conclusion (Commercial Articles) - Warning instead of crash
        if state.get("intent", "").lower() == "commercial":
            if not self.validator.validate_final_cta(final_md, article_language):
                error_msg = "Missing final CTA in conclusion for Commercial article."
                logger.warning(error_msg)
                warnings.append(error_msg)

        final_md = self.validator.enforce_paragraph_structure(final_md)
        state["final_output"]["final_markdown"] = final_md

        # Enforce Paragraph Length Rules (Warning only)
        if not self.validator.validate_paragraph_structure(final_md):
            msg = "Paragraph structure violation detected (too many sentences)."
            logger.warning(msg)
            warnings.append(msg)

        # --- SEMANTIC TOPIC ARCHITECTURE (PHASE 1.5) ---
        semantic_metadata = {
            "semantic_entities": state.get("semantic_entities", []),
            "semantic_concepts": state.get("semantic_concepts", []),
            "intent_clusters": state.get("intent_clusters", [])
        }
        outline = state.get("outline", [])
        
        semantic_report = self.validator.validate_semantic_coverage(
            final_md, 
            semantic_metadata, 
            outline
        )
        state["semantic_coverage_report"] = semantic_report
        
        # Add semantic warnings if coverage is low (Advisory)
        if not semantic_report.get("semantic_coverage_ok", True):
            missing = semantic_report.get("missing_concepts", [])
            warnings.append(f"SEMANTIC_GAP_DETECTED: Significant topical concepts are missing: {', '.join(missing[:5])}")

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
            content_strategy=state.get("content_strategy", {}),
            prohibited_competitors=state.get("prohibited_competitors", []),
            reference_authority_links=state.get("serp_data", {}).get("reference_authority_links", [])
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
        # Ensure the renderer receives the full assembled output including schemas
        render_data = final_output.copy()
        render_data["output_dir"] = output_dir # Ensure output_dir is present if not in final_output
        render_data["final_markdown"] = final_output.get("final_markdown")

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
    
    # ---------------- UTILITIES ---

    def _build_execution_plan(self, section: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        """Constructs the per-section execution plan with CTA rules and writing constraints."""
        content_type = state.get("content_type", "informational")
        section_type = (section.get("section_type") or "").lower()
        
        # Base plan
        plan = {
            "writing_mode": "standard",
            "cta_type": section.get("cta_type", "none"),
            "cta_position": section.get("cta_position", "none"),
            "structure_rule": "EXACTLY 2-3 PARAGRAPHS. 2-3 SENTENCES PER PARAGRAPH.",
            "local_context_required": bool(state.get("area")),
            "tone_override": state.get("tone"),
            "pov_override": state.get("pov")
        }

        # Override for specific section types
        if section_type == "introduction":
            plan["writing_mode"] = "hooks-driven"
            
        elif section_type == "conclusion":
            plan["writing_mode"] = "summary-driven"
            if content_type == "brand_commercial":
                plan["cta_eligible"] = True
                plan["cta_type"] = "strong"
                section["cta_eligible"] = True
                section["cta_type"] = "strong"
        
        elif section_type == "faq":
            plan["writing_mode"] = "direct-answer"
            plan["structure_rule"] = "H3 Questions followed by concise answers."

        return plan

    def _assemble_final_output(self, state: Dict[str, Any]) -> Dict[str, Any]:
        import re
        input_data = state.get("input_data", {})
        final_out = state.get("final_output", {})
        seo_meta = state.get("seo_meta", {})
        images = state.get("assets/images", [])
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
                domain = LinkManager.domain(brand_url)  # e.g., "cems-it.com"
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
            "assets/images": images,

            # Validation
            "seo_report": seo_report,

            # Performance
            "performance": performance,

            # Debug / Storage
            "output_dir": state.get("output_dir", ""),
        }
    
    async def _step_3_humanizer(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Final editorial pass to smooth transitions and remove redundancies."""
        logger.info("Starting final 'Humanizer' editorial pass...")
        
        full_content = state.get("full_content_so_far", "")
        if not full_content:
            logger.warning("No content found to humanize.")
            return state
            
        style_blueprint = state.get("style_blueprint", {})
        tone = state.get("tone") or style_blueprint.get("writing_tone", "Conversational")
        audience_level = style_blueprint.get("tonal_dna", {}).get("audience_level", "General")
        
        # In this system, StrategyService or Template loader should handle the template
        # Let's ensure we use a generic render if the template isn't explicitly in self.templates
        humanizer_template = self.outline_gen.templates.get("humanizer") # OutlineGen usually has access to common templates
        if not humanizer_template:
            # Fallback to loading it manually if necessary, but typically we want it in the registry
            # For now, let's use the template I just created
            from jinja2 import Environment, FileSystemLoader
            env = Environment(loader=FileSystemLoader("assets/prompts/templates"))
            humanizer_template = env.get_template("09_humanizer_editor.txt")

        humanize_prompt = humanizer_template.render(
            full_content=full_content,
            tone=tone,
            audience_level=audience_level,
            area=state.get("area", "Global"),
            content_type=state.get("content_type", "article")
        )
        
        try:
            res = await self.ai_client.send(humanize_prompt, step="humanizer_polish")
            polished_content = res.get("content", full_content)
            
            # Update state with polished content
            state["full_content_so_far"] = polished_content
            
            logger.info("Humanizer pass completed successfully.")
        except Exception as e:
            logger.error(f"Humanizer pass failed: {e}")
            
        return state
