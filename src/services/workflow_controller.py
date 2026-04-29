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
import copy
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
from src.services.ai_client_base import BaseAIClient
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
from src.utils.contract_safety import PipelineContractError, validate_service_call, is_signature_mismatch
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
                
                # FATAL CONTRACT FAILURE: Non-retryable
                if isinstance(e, PipelineContractError) or is_signature_mismatch(e):
                    logger.critical(f"FATAL CONTRACT FAILURE in step '{step_name}': {e}. Aborting.")
                    return {"status": "error", "step": step_name, "duration": duration, "error": str(e), "data": state, "retryable": False}

                attempt += 1
                if attempt <= retries:
                    await asyncio.sleep(0.1) # Reduced from 1s for better responsiveness
                else:
                    return {"status": "error", "step": step_name, "duration": duration, "error": str(e), "data": state}
        
        return {"status": "error", "step": step_name, "error": "Max retries exceeded", "data": state}

class AsyncWorkflowController:
    """Central async orchestrator for SEO article generation."""

    def __init__(self, work_dir: str = ".", ai_client: Optional[BaseAIClient] = None):
        # AI Client Injection Support
        self.ai_client = ai_client or OpenRouterClient()
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
        
        # Hard-Stop Flag for critical failures
        self.workflow_failed = False

        # Image generator
        self.image_client = ImageGenerator(
            ai_client=self.ai_client,
            save_dir=os.path.join(work_dir, "assets/images"),
        )
        
        # Run startup contract audit (smoke test)
        self.preflight_system_audit()

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
        state.setdefault("brand_name", ""); state.setdefault("display_brand_name", ""); state.setdefault("official_brand_name", ""); state.setdefault("brand_aliases", []); state.setdefault("domain_brand_name", "")
        state["max_external_links"] = 3
        state.setdefault("global_keyword_count", 0)
        state.setdefault("used_topics", [])
        state.setdefault("full_content_so_far", "")
        state.setdefault("brand_mentions_count", 0)
        state.setdefault("used_anchors", [])
        
        # Check for Heading-Only Mode
        heading_only_mode = state.get("input_data", {}).get("heading_only_mode", False)
        state["heading_only_mode"] = heading_only_mode

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
            ("global_coherence", self._step_3_global_coherence_pass, 1),
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
            # ("article_validation", self._step_8_article_validation, 0),
            ("render_html", self._step_render_html, 0)
        ])
        for name, func, retries in steps:
            result = await self.executor.run_step(name, func, state, retries=retries)
            state = result.get("data", state)
            
            if result["status"] == "error":
                if name in self.CRITICAL_STEPS:
                    logger.error(f"FATAL ERROR at critical step '{name}': {result.get('error')}")
                    self.workflow_failed = True
                    # Immediate stop - do not attempt further processing
                    return {"status": "error", "message": f"Workflow aborted: Critical failure in {name}", "error": result.get("error")}
                else:
                    logger.warning(f"Non-critical step '{name}' failed. Continuing...")
                    continue
            
            # Runtime Debug: Trace current step and mode
            print(f"[TRACER_V1] Step: '{name}' | heading_only_mode={state.get('heading_only_mode')} (type: {type(state.get('heading_only_mode'))})")
            
            # Heading-Only Mode: Stop immediately after outline generation
            if state.get("heading_only_mode") and name == "outline_generation":
                logger.info("Heading-Only Mode active: Stopping workflow after outline generation.")
                print(f"[TRACER_V1] SUCCESS: Triggered Heading-Only early stop for step '{name}'.")
                break

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

        keyword_profile = self.validator._derive_keyword_profile(state.get("primary_keyword", ""), area or "")
        head_entity = keyword_profile.get("head_entity", "")
        entity_phrase = keyword_profile.get("entity_phrase", "") or head_entity
        service_phrase = keyword_profile.get("service_phrase", "") or entity_phrase

        structural = seo_intelligence.get("market_analysis", {}).get("structural_intelligence", {})
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
        outline_validated = False
        last_validation_errors = []

        for attempt in range(3):
            logger.info(f"Generating outline (Attempt {attempt + 1}/3)...")
            # PREFLIGHT CONTRACT CHECK
            validate_service_call(
                self.outline_gen.generate,
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
                article_size=state.get("article_size", "1000"),
                include_conclusion=state.get("include_conclusion", True),
                include_faq=state.get("include_faq", True),
                include_tables=state.get("include_tables", True),
                include_bullet_lists=state.get("include_bullet_lists", True),
                include_comparison_blocks=state.get("include_comparison_blocks", True),
                bold_key_terms=state.get("bold_key_terms", True),
                secondary_keywords=state.get("secondary_keywords", []),
                brand_name=state.get("brand_name", ""),
                brand_url=state.get("brand_url", ""),
                brand_advantages=seo_intelligence.get("market_analysis", {}).get("market_insights", {}).get("brand_advantages", []),
                writing_blueprint=seo_intelligence.get("market_analysis", {}).get("market_insights", {}).get("writing_blueprint", ""),
                market_angle=content_strategy.get("market_angle", ""),
                heading_only_mode=state.get("heading_only_mode", False),
                head_entity=head_entity,
                entity_phrase=entity_phrase,
                service_phrase=service_phrase
            )

            # --- Heading-Only Strategy Detox (Localized to this step) ---
            h_content_strategy = content_strategy
            h_brand_context = state.get("brand_context", "")
            h_brand_advantages = seo_intelligence.get("market_analysis", {}).get("market_insights", {}).get("brand_advantages", [])
            h_writing_blueprint = seo_intelligence.get("market_analysis", {}).get("market_insights", {}).get("writing_blueprint", "")
            h_seo_intelligence = seo_intelligence

            if state.get("heading_only_mode"):
                h_seo_intelligence = self._distill_serp_intelligence(
                    seo_intelligence=seo_intelligence,
                    primary_keyword=state.get("primary_keyword", ""),
                    intent=intent
                )
                h_content_strategy, h_brand_context, h_brand_advantages, h_writing_blueprint = self._apply_heading_only_detox(
                    content_strategy=content_strategy,
                    brand_context=h_brand_context,
                    brand_advantages=h_brand_advantages,
                    writing_blueprint=h_writing_blueprint,
                    primary_keyword=state.get("primary_keyword", ""),
                    content_type=content_type,
                    area=area or "",
                    seo_intelligence=h_seo_intelligence,
                )
                logger.info(
                    "[TRACER_V1] Heading-Only Detox & Distillation fired for '%s'.",
                    state.get("primary_keyword", ""),
                )

            try:
                outline_data = await self.outline_gen.generate(
                    title=title,
                    keywords=keywords,
                    urls=urls_norm,
                    article_language=article_language,
                    intent=intent,
                    seo_intelligence=h_seo_intelligence,
                    content_type=content_type,
                    content_strategy=h_content_strategy,
                    brand_context=h_brand_context,
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
                    brand_name=state.get("brand_name", ""),
                    brand_url=state.get("brand_url", ""),
                    market_angle=h_content_strategy.get("market_angle", ""),
                    brand_advantages=h_brand_advantages,
                    writing_blueprint=h_writing_blueprint,
                    heading_only_mode=state.get("heading_only_mode", False),
                    head_entity=head_entity,
                    entity_phrase=entity_phrase,
                    service_phrase=service_phrase
                )
            except (ContentGeneratorError, Exception) as e:
                logger.warning(f"Outline generation failed on attempt {attempt + 1}: {e}")
                if attempt < 2:
                    feedback = f"Your previous response failed to parse as valid JSON. Error: {str(e)}. Please try again and ensure you return a strictly valid JSON object."
                    continue
                else:
                    logger.error("Outline generation failed after all retries.")
                    raise
            # Store metadata for WorkflowLogger
            if "metadata" in outline_data:
                state["last_step_prompt"] = outline_data["metadata"]["prompt"]
                state["last_step_response"] = outline_data["metadata"]["response"]
                state["last_step_tokens"] = outline_data["metadata"]["tokens"]
                state["last_step_model"] = outline_data["metadata"].get("model", "unknown")

            if not outline_data or not outline_data.get("outline"):
                if attempt < 2:
                    feedback = "Outline generation returned empty result. Please provide a full, structured JSON outline."

            # (Redundant block removed)
            
            outline = outline_data.get("outline", [])
            
            # Validation Layer
            errors = []
            
            # 0. FAQ Consolidation (Robustness)
            outline = self.validator.consolidate_faq(outline)
            
            # Pruning and Repair (Deterministic)
            # TEMPORARY: Relaxed validation for heading-only mode
            heading_only_mode = state.get("heading_only_mode", False)
            # Use this flag to bypass heavy structural/semantic rules
            heading_only_relaxed_validation = heading_only_mode

            if not heading_only_relaxed_validation:
                if heading_only_mode:
                    outline = self.validator.prune_unsupported_optional_subheadings(
                        outline,
                        primary_keyword=state.get("primary_keyword", ""),
                        content_strategy=h_content_strategy,
                        seo_intelligence=h_seo_intelligence,
                    )
                
                outline = self.validator.repair_outline_deterministic(
                    outline,
                    primary_keyword=state.get("primary_keyword", ""),
                    content_strategy=h_content_strategy,
                    seo_intelligence=h_seo_intelligence,
                    brand_name=state.get("brand_name", ""),
                    area=area or ""
                )
                
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
                if heading_only_mode:
                    quality_errors = self.validator.validate_heading_outline_quality(
                        outline,
                        content_type=content_type,
                        area=area or "",
                        primary_keyword=state.get("primary_keyword", ""),
                        brand_name=state.get("brand_name", ""),
                        content_strategy=h_content_strategy,
                        seo_intelligence=h_seo_intelligence,
                    )
                else:
                    quality_errors = self.validator.validate_outline_quality(
                        outline,
                        content_type=content_type,
                    )
                errors.extend(quality_errors)
            else:
                logger.info("Heading-only mode: Heavy quality validation and deterministic repairs bypassed.")
            
            last_validation_errors = list(errors)

            if not errors:
                logger.info(f"Outline validated successfully on attempt {attempt + 1}.")
                outline_validated = True
                break
            
            feedback = "Validation failed. Please correct the following issues and regenerate the outline:\n- " + "\n- ".join(errors)
            logger.warning(f"Outline validation failed (attempt {attempt + 1}): {feedback}")

        if not outline_validated:
            fatal_errors = [e for e in last_validation_errors if not e.startswith("WARNING_")]
            if not fatal_errors:
                logger.warning("Outline validation had only soft warnings after all retries. Proceeding with warnings: " + ", ".join(last_validation_errors))
            else:
                error_summary = "\n- ".join(fatal_errors) if fatal_errors else "Unknown outline validation failure."
                logger.error("Outline validation failed after all retries. Fatal validation errors:\n- %s", error_summary)
                raise StructureError(
                    "Outline validation failed after all retries. Last issues were:\n- " + error_summary
                )

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
            .get("market_analysis", {})
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
        
        # --- Smart Link Pool Preparation (Contextual Flow) ---
        internal_pool = list(state.get("internal_url_set", set()))
        
        # External Authority References (Broad pool for the AI to choose from)
        external_refs = []
        for item in serp_data.get("reference_authority_links", []):
            url = item.get("url") if isinstance(item, dict) else item
            if url: 
                external_refs.append(LinkManager.canon_url(url))
        
        # Limit to top 15 internal links to avoid prompt bloat, but keep it a broad pool
        internal_pool = list(dict.fromkeys(internal_pool))[:15]
        external_refs = list(dict.fromkeys(external_refs))[:10]
        
        state["available_links_pool"] = {
            "internal": internal_pool,
            "external_references": external_refs
        }
        logger.info(f"Smart Link Pool initialized with {len(internal_pool)} internal and {len(external_refs)} authority references.")

        # Ensure all sections have clean link assignments for the start
        for section in outline:
            section["assigned_links"] = []

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
            logger.error("Sanity Check Failed: No outline found for section writing. Potential trace of bypassed critical error.")
            raise RuntimeError("CRITICAL ERROR: Content writing started with an empty or invalid outline. Stopping to prevent corrupted output.")

        content_type = state.get("content_type", "informational")
        content_strategy = state.get("content_strategy", {})
        market_angle = content_strategy.get("market_angle", "")


        # Initialize global quality tracking
        state["used_claims"] = []
        state["ctas_placed"] = 0
        state["tables_placed"] = 0
        state["full_content_so_far"] = ""
        state["last_section_content"] = ""

        # Force sequential for commercial to allow used-and-delete link logic
        is_commercial = content_type == "brand_commercial"
        use_parallel = PARALLEL_SECTIONS and not is_commercial

        if use_parallel:
            # Parallel logic for non-commercial
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
                    brand_mentions_count=state.get("brand_mentions_count", 0),
                    brand_advantages=seo_intelligence.get("market_analysis", {}).get("market_insights", {}).get("brand_advantages", []),
                    writing_blueprint=seo_intelligence.get("market_analysis", {}).get("market_insights", {}).get("writing_blueprint", "")
                )
                for idx, section in enumerate(outline)
            ]
            logger.info(f"Writing {len(tasks)} sections in PARALLEL mode")
            results = await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logger.info(f"Writing {len(outline)} sections in SEQUENTIAL mode (Smart Pool Enforcement: {is_commercial})")
            results = []
            available_pool = state.get("available_links_pool", {"internal": [], "external": []})
            
            for idx, section in enumerate(outline):
                # Inject current pool into section context for the prompt
                section["available_link_pool"] = available_pool
                
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
                    brand_mentions_count=state.get("brand_mentions_count", 0),
                    brand_advantages=seo_intelligence.get("market_analysis", {}).get("market_insights", {}).get("brand_advantages", []),
                    writing_blueprint=seo_intelligence.get("market_analysis", {}).get("market_insights", {}).get("writing_blueprint", "")
                )
                
                # UPDATE POOL: Extract used links and remove them
                if res and res.get("generated_content"):
                    content = res["generated_content"]
                    # UPDATE POOL: Prune used internal links only (External are per-fact)
                    content = res["generated_content"]
                    used_urls = re.findall(r'\[.*?\]\((https?://.*?)\)', content)
                    
                    old_internal = available_pool.get("internal", [])
                    available_pool["internal"] = [u for u in old_internal if u not in used_urls]
                    if len(old_internal) != len(available_pool["internal"]):
                        logger.info(f"Pruned {len(old_internal) - len(available_pool['internal'])} internal links.")

                    state["available_links_pool"] = available_pool

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
                
                # Robust regex for Arabic & English: handles common Arabic prefixes/suffixes
                # [و|ب|ل|ف|ك|ال]* -> matches common prefixes
                # (keyword)
                # [ة|ات|ون|ين|ه|ها|هم|نا|ي]* -> matches common suffixes
                # Use \b for English or standard word boundaries
                if any(ord(c) > 127 for c in primary_keyword): # Arabic/Non-ASCII detection
                    # Arabic-friendly regex: allow common prefixes/suffixes
                    pattern = r'(?:[وبلفك]|ال)*{}(?:[ةاتونينههمناي])*'.format(re.escape(primary_keyword.lower()))
                else:
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
                    total_sections=len(outline),
                    brand_advantages=seo_intelligence.get("market_analysis", {}).get("market_insights", {}).get("brand_advantages", []),
                    writing_blueprint=seo_intelligence.get("market_analysis", {}).get("market_insights", {}).get("writing_blueprint", "")
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
        brand_mentions_count: int = 0,
        brand_advantages: List[str] = None,
        writing_blueprint: str = "",
        market_angle: str = ""
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
        # Instead of sending the entire article text (token heavy), 
        # we send the Intro + Full Map of Headings + last 3 sections.
        intro_text = state.get("introduction_text", "")
        
        # Get generated content and headings of all sections written so far
        all_sections_data = list(state.get("sections", {}).values())
        all_headings = [s.get("heading_text", "No Heading") for s in all_sections_data if "generated_content" in s]
        all_content = [s["generated_content"] for s in all_sections_data if "generated_content" in s]
        
        # Keep the last 3 sections for immediate narrative flow
        recent_context = "\n\n".join(all_content[-3:]) if all_content else ""
        
        # Build a cumulative map of what has been covered so far to prevent conceptual repetition
        full_article_map = " | ".join(all_headings) if all_headings else "None"
        
        cumulative_history = f"STORY SO FAR (Headings): {full_article_map}\n\n"
        optimized_context = f"{cumulative_history}ARTICLE INTRODUCTION:\n{intro_text}\n\nRECENT CONTEXT (Last 3 Sections):\n{recent_context}" if intro_text else recent_context

        # PREFLIGHT CONTRACT CHECK
        validate_service_call(
            self.section_writer.write,
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
            cta_type=cta_type,
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
            previous_content_summary=optimized_context,
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
            brand_mentions_count=brand_mentions_count,
            brand_advantages=brand_advantages,
            writing_blueprint=writing_blueprint,
            market_angle=market_angle,
            used_anchors=state.get("used_anchors", [])
        )

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
            brand_mentions_count=brand_mentions_count,
            brand_advantages=brand_advantages,
            writing_blueprint=writing_blueprint,
            market_angle=market_angle,
            used_anchors=state.get("used_anchors", [])
        )
        
        content = res_data.get("content", "")
        # --- Extract and track Anchor Texts for rotation ---
        if content:
            new_anchors = re.findall(r'\[(.*?)\]\(.*?\)', content)
            if new_anchors:
                state.setdefault("used_anchors", [])
                for anchor in new_anchors:
                    clean_anchor = anchor.strip().lower()
                    if clean_anchor not in state["used_anchors"]:
                        state["used_anchors"].append(clean_anchor)
        
        used_links = res_data.get("used_links", [])
        brand_link_used_in_sec = res_data.get("brand_link_used", False)
        

        # --- ENTITY LOCKDOWN CHECK (REMOVED FOR CREATIVITY) ---
        # We now rely on the AI's natural expert knowledge and strict 'No Competitor' policy.

        # if content:
        #     repeated = self.validator.detect_repetition(content, used_phrases)
        #     if repeated and len(repeated) > 0:
        #         ...

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

            # --- QUALITY VALIDATION & ACTIVE REPAIR LOOP ---
            try:
                is_valid, validation_errors = await self.validator.validate_section_output(
                    content=final_content,
                    section=section,
                    state=state
                )
                
                # Check for "Fixable Quality Issues" that warrant an automated repair attempt
                # We specifically look for errors defined in ValidationService, following v2.2 priorities
                priority_map = {
                    "SECTION_TYPE_CRITICAL_ERROR": 1,
                    "INTRO_PK_MISSING": 1,
                    "INTRO_PK_FORCED": 1,
                    "INTRO_TOPIC_ANCHOR_MISSING": 1,
                    "INTRO_HOOK_QUALITY_REQUIRED": 2,
                    "INTRO_HOOK_CLARITY_REQUIRED": 2,
                    "INTRO_GEO_SCOPE_DRIFT": 2,
                    "STRUCTURE_FORMAT_MISMATCH": 3,
                    "HIDDEN_SUBSECTIONS_DETECTED": 3,
                    "PLAIN_LANGUAGE_REQUIRED": 3,
                    "INTRO_TONE_PROFILE_MISMATCH": 4,
                    "INTRO_INTENT_SIGNAL_WARNING": 5,
                    "PREMATURE_COMMERCIAL_FRAMING": 5,
                    "METRIC_DATA_MISSING": 6,
                    "VISUAL_FORMAT_MISSING": 6,
                    "DECORATIVE_BULLETS_DETECTED": 6,
                    "TONE_INFLATION_HIGH": 7,
                    "POTENTIAL_BIAS": 7
                }
                fixable_issues = list(priority_map.keys())
                active_repair_needed = any(any(issue in err for issue in fixable_issues) for err in validation_errors) if (not is_valid and validation_errors) else False
                
                if active_repair_needed:
                    logger.info(f"Active Repair Triggered for section '{section.get('heading_text')}'. Total errors: {len(validation_errors)}")
                    
                    # Sort errors by priority so we don't overwhelm the AI
                    # We group errors by their base code to identify the highest priority one
                    scoped_errors = []
                    for err in validation_errors:
                        prio = 99
                        for issue, p in priority_map.items():
                            if issue in err:
                                prio = p
                                break
                        scoped_errors.append((prio, err))
                    
                    scoped_errors.sort(key=lambda x: x[0])
                    
                    # Only send top 1-2 priorities in the first repair attempt to keep feedback actionable
                    top_priority = scoped_errors[0][0]
                    filtered_errors = [e for p, e in scoped_errors if p <= top_priority + 1] # Allow one level deeper if needed
                    
                    feedback_str = "\n".join([f"- {err}" for err in filtered_errors])
                    
                    # Update execution plan for repair mode (used by template's REFINEMENT MODE)
                    repair_plan = execution_plan.copy()
                    repair_plan["structure_rule"] = f"FIX QUALITY ERRORS (Strategic Correction):\n{feedback_str}"
                    
                    # PREFLIGHT CONTRACT CHECK (Repair Mode)
                    validate_service_call(
                        self.section_writer.write,
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
                        execution_plan=repair_plan, # Pass the repair plan
                        draft_to_fix=final_content, # Pass the failed draft
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
                        cta_type=cta_type,
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
                        previous_content_summary=optimized_context,
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
                        brand_mentions_count=brand_mentions_count,
                        brand_advantages=brand_advantages,
                        writing_blueprint=writing_blueprint,
                        market_angle=market_angle,
                        used_anchors=state.get("used_anchors", [])
                    )

                    # RETRY 1: Surgical Edit Mode
                    repair_data = await self.section_writer.write(
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
                        execution_plan=repair_plan, # Pass the repair plan
                        draft_to_fix=final_content, # Pass the failed draft
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
                        cta_type=cta_type,
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
                        previous_content_summary=optimized_context,
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
                        brand_mentions_count=brand_mentions_count,
                        brand_advantages=brand_advantages,
                        writing_blueprint=writing_blueprint,
                        market_angle=market_angle,
                        used_anchors=state.get("used_anchors", [])
                    )
                    
                    new_content = repair_data.get("content", "")
                    if new_content:
                        logger.info(f"Section '{section.get('heading_text')}' repaired successfully.")
                        final_content = self.validator.enforce_paragraph_structure(new_content)
                        # Re-calculate links and brand link usage for the repaired content
                        found_links = re.findall(r'\[.*?\]\((https?://.*?)\)', final_content)
                        if any(LinkManager.is_same_site(l, brand_url) for l in found_links):
                            state["brand_link_used"] = True

                # Log final validation results to the audit file
                if not is_valid and validation_errors:
                    output_dir = state.get("output_dir", self.work_dir)
                    val_err_path = os.path.join(output_dir, "validation_errors.txt")
                    section_title = section.get("heading_text", "Untitled Section")
                    
                    with open(val_err_path, "a", encoding="utf-8") as f:
                        f.write(f"\n--- SECTION: {section_title} ({section_id}) ---\n")
                        for err in validation_errors:
                            f.write(f"- [QUALITY ISSUE]: {err}\n")
                        
                        repeated = self.validator.detect_repetition(final_content, state.get("used_phrases", []))
                        if repeated and len(repeated) > 0:
                            for rep in repeated:
                                f.write(f"- [REPETITION ISSUE]: Found duplicated phrase: '{rep}'\n")

                        f.write("-" * 50 + "\n")
            except Exception as e:
                logger.error(f"Validation or Repair loop failed: {e}")
            # --------------------------------------------------

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
        
        # PREFLIGHT CONTRACT CHECK
        validate_service_call(
            self.assembler.assemble,
            title=title, 
            sections=final_sections, 
            article_language=article_language,
            content_type=state.get("content_type", "informational")
        )

        assembled = await self.assembler.assemble(
            title=title, 
            sections=final_sections, 
            article_language=article_language,
            content_type=state.get("content_type", "informational")
        )
        
        # Final pass redundancy pruning on the whole assembled markdown
        # One final pass at the very end will suffice
        # md = LinkManager.deduplicate_links_in_markdown(md, brand_domain=brand_domain, max_internal=6)
        assembled["final_markdown"] = assembled.get("final_markdown", "")

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
        # Safely extract brand advantages for humanizer anchoring
        brand_advantages_list = []
        market_analysis = state.get("seo_intelligence", {}).get("market_analysis", {})
        if isinstance(market_analysis, dict):
            market_insights = market_analysis.get("market_insights", {})
            if isinstance(market_insights, dict):
                brand_advantages_list = market_insights.get("brand_advantages", [])
        
        brand_advantages = "\n".join(brand_advantages_list) if isinstance(brand_advantages_list, list) else str(brand_advantages_list)

        for i, section in enumerate(ordered_sections):
            content = section.get("generated_content", "")
            heading = section.get("heading_text", "")
            is_intro = (section.get("section_type", "").lower() == "introduction")
            is_conclusion = (section.get("section_type", "").lower() == "conclusion")
            
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
            # PREFLIGHT CONTRACT CHECK
            validate_service_call(
                self.final_humanizer.humanize_section,
                full_article_context=dynamic_draft,
                target_section_content=content,
                target_section_heading=heading,
                article_language=article_language,
                brand_name=brand_name,
                brand_source_text=brand_source_text,
                brand_advantages=brand_advantages,
                section=section,
                is_introduction=is_intro,
                is_conclusion=is_conclusion,
                brand_mentions_total_count=state.get("brand_mentions_count", 0),
                global_keyword_count=state.get("global_keyword_count", 0)
            )

            try:
                new_content = await self.final_humanizer.humanize_section(
                    full_article_context=dynamic_draft,
                    target_section_content=content,
                    target_section_heading=heading,
                    article_language=article_language,
                    brand_name=brand_name,
                    brand_source_text=brand_source_text,
                    brand_advantages=brand_advantages,
                    section=section,
                    is_introduction=is_intro,
                    is_conclusion=is_conclusion,
                    brand_mentions_total_count=state.get("brand_mentions_count", 0),
                    global_keyword_count=state.get("global_keyword_count", 0)
                )
                if new_content:
                    section["generated_content"] = new_content
            except Exception as e:
                logger.error(f"Humanization failed for section '{heading}': {e}. Falling back to original.")
            
        # Re-assemble the article after humanization
        # PREFLIGHT CONTRACT CHECK
        validate_service_call(
            self.assembler.assemble,
            title=title, 
            sections=ordered_sections, 
            article_language=article_language,
            content_type=state.get("content_type", "informational")
        )

        title = state.get("input_data", {}).get("title", "Untitled")
        assembled = await self.assembler.assemble(
            title=title, 
            sections=ordered_sections, 
            article_language=article_language,
            content_type=state.get("content_type", "informational")
        )
        
        # Final pass redundancy pruning on the whole assembled markdown
        # Sanitization disabled per quality hardening plan - relying on LinkManager's final pass
        # md = LinkManager.deduplicate_links_in_markdown(md, brand_domain=brand_domain, max_internal=6)
        md = assembled.get("final_markdown", "")
        
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
        # md = LinkManager.deduplicate_links_in_markdown(new_md, brand_domain=brand_domain, max_internal=6)
        state["final_output"]["final_markdown"] = new_md
        return state

    async def _step_7_meta_schema(self, state):
        final_md = state.get("final_output", {}).get("final_markdown", "")
        if not final_md:
            return state

        # PREFLIGHT CONTRACT CHECK
        validate_service_call(
            self.meta_schema.generate,
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
        
        
        # Article Validation Silent Mode (Disabled as requested)
        critical_issues = []
        warnings = []

        word_count, keyword_count, keyword_density = self.validator.calculate_keyword_stats(
            final_md,
            primary_keyword
        )

        # Heuristic checks
        ok, issue = self.validator.validate_sales_intro(final_md, state.get("intent"))
        if not ok:
            critical_issues.append(issue)

        if state.get("content_type") == "brand_commercial":
            structural_intel = state.get("seo_intelligence", {}).get("market_analysis", {}).get("structural_intelligence", {})
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

        # PREFLIGHT CONTRACT CHECK
        validate_service_call(
            self.article_validator.validate,
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
    
    def preflight_system_audit(self):
        """
        Lightweight smoke test for service availability and required methods.
        Ensures that critical services are injected and satisfy the basic interface contract.
        """
        logger.info("Starting Pipeline Preflight System Audit...")
        critical_components = [
            (self.outline_gen, "generate"),
            (self.section_writer, "write"),
            (self.assembler, "assemble"),
            (self.final_humanizer, "humanize_section"),
            (self.meta_schema, "generate"),
            (self.article_validator, "validate"),
            (self.title_generator, "generate"),
            (self.research_service, "run_hybrid_research"),
            (self.strategy_service, "run_content_strategy")
        ]
        
        for service, method_name in critical_components:
            if service is None:
                raise PipelineContractError(f"Startup Audit Failed: {type(service).__name__} is missing (None).")
            
            method = getattr(service, method_name, None)
            if method is None:
                raise PipelineContractError(f"Startup Audit Failed: Service '{type(service).__name__}' is missing required method '{method_name}'.")
            
            if not callable(method):
                raise PipelineContractError(f"Startup Audit Failed: '{type(service).__name__}.{method_name}' is not callable.")
                
        logger.info("Pipeline Preflight System Audit: PASS (Structural Integrity Verified)")

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
                    # candidate = candidate[:65] # Optional trimming

        if state.get("heading_only_mode"):
            outline = state.get("outline", [])
            heading_map = []
            
            # Build a clear structural map for review
            for sec in outline:
                sec_type = (sec.get("section_type") or "").lower()
                
                # Omit Introduction as an H2 (Rule #2)
                if sec_type == "introduction":
                    heading_map.append({
                        "section_id": sec.get("section_id"),
                        "note": "[Note: Unheaded Introduction Block (Problem + Context)]",
                        "section_type": "introduction"
                    })
                    continue

                item = {
                    "section_id": sec.get("section_id"),
                    "heading_text": sec.get("heading_text"),
                    "heading_level": sec.get("heading_level", "H2"),
                    "section_type": sec.get("section_type"),
                    "section_intent": sec.get("section_intent"),
                    "subheadings": sec.get("subheadings", []) # Explicit H3s (Rule #3)
                }
                heading_map.append(item)
            
            # Generate readable markdown preview (Rule: No content, only headings)
            preview_lines = [f"# {raw_title}", ""]
            for sec in heading_map:
                if sec.get("section_type") == "introduction":
                    preview_lines.append("[Unheaded Introduction Block]")
                    preview_lines.append("")
                else:
                    level = sec.get("heading_level", "H2").upper()
                    prefix = "##" if level == "H2" else "###"
                    preview_lines.append(f"{prefix} {sec.get('heading_text', 'Untitled Section')}")
                    
                    # Add H3 subheadings if present
                    for sub in sec.get("subheadings", []):
                        preview_lines.append(f"### {sub}")
                    
                    preview_lines.append("")

            return {
                "title": raw_title,
                "slug": state.get("slug", "unknown"),
                "primary_keyword": state.get("primary_keyword", ""),
                "heading_only_mode": True,
                "outline_structure": heading_map,
                "heading_preview_markdown": "\n".join(preview_lines).strip(),
                "status": "success",
                "message": "Heading structure generated successfully for review.",
                "performance": performance,
                "output_dir": state.get("output_dir", "")
            }

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
    
    async def _step_3_global_coherence_pass(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Performs an article-level coherence audit. 
        Takes the full assembled markdown (with section markers), polishes narrative flow 
        and deduplicates concepts, then splits the result back into state['sections'].
        """
        logger.info("Starting Global Coherence & Redundancy Pass...")
        
        # 1. Assemble current sections into a structured draft with ID markers
        title = state.get("input_data", {}).get("title", "Untitled")
        outline = state.get("outline", [])
        sections_dict = state.get("sections", {})
        article_language = state.get("article_language") or state.get("input_data", {}).get("article_language", "en")
        
        if not sections_dict:
            logger.warning("No sections found for global coherence pass.")
            return state

        ordered_sections = [
            sections_dict[s["section_id"]]
            for s in outline
            if s.get("section_id") in sections_dict
        ]
        
        # PREFLIGHT CONTRACT CHECK
        validate_service_call(
            self.assembler.assemble,
            title=title, 
            sections=ordered_sections, 
            article_language=article_language,
            content_type=state.get("content_type", "informational")
        )

        assembled_data = await self.assembler.assemble(
            title=title, 
            sections=ordered_sections, 
            article_language=article_language,
            content_type=state.get("content_type", "informational")
        )
        full_content_with_markers = assembled_data.get("final_markdown", "")

        if not full_content_with_markers:
            logger.warning("Assembled content is empty. Skipping coherence pass.")
            return state

        # 2. Prepare Prompt
        style_blueprint = state.get("style_blueprint", {})
        tone = state.get("tone") or style_blueprint.get("writing_tone", "Conversational")
        audience_level = style_blueprint.get("tonal_dna", {}).get("audience_level", "General")
        
        # Load template (reusing existing path for consistency)
        try:
            from jinja2 import Environment, FileSystemLoader
            env = Environment(loader=FileSystemLoader("assets/prompts/templates"))
            coherence_template = env.get_template("09_humanizer_editor.txt")
        except Exception as e:
            logger.error(f"Failed to load coherence template: {e}")
            return state

        prompt = coherence_template.render(
            full_content=full_content_with_markers,
            tone=tone,
            audience_level=audience_level,
            area=state.get("area", "Global"),
            content_type=state.get("content_type", "article"),
            primary_keyword=state.get("primary_keyword", ""),
            brand_name=state.get("brand_name", "")
        )
        
        # 3. AI Execution
        try:
            res = await self.ai_client.send(prompt, step="global_coherence_audit")
            polished_full_md = res.get("content", "")
            
            if not polished_full_md:
                logger.warning("AI returned empty content for coherence pass. Falling back.")
                return state
                
            # 4. Validated Splitting Logic
            # Pattern to find markers: <!-- section_id: ... -->
            marker_pattern = r"<!-- section_id: (.*?) -->"
            
            # Split the content. re.split with a group returns the separators in the list.
            parts = re.split(marker_pattern, polished_full_md)
            
            # Reconstruct sections: [prelude, id1, content1, id2, content2, ...]
            revised_sections_map = {}
            for i in range(1, len(parts), 2):
                sid = parts[i].strip()
                content = parts[i+1].strip()
                revised_sections_map[sid] = content

            # Validation 1: Marker Count Consistency
            original_ids = set(sections_dict.keys())
            revised_ids = set(revised_sections_map.keys())
            
            # Validation 2: Structural Integrity
            if original_ids == revised_ids and len(revised_ids) == len(original_ids):
                # Success! Propagate changes back to sections
                for sid, new_content in revised_sections_map.items():
                    # Preserve any metadata while updating the generated_content
                    sections_dict[sid]["generated_content"] = new_content
                
                state["sections"] = sections_dict
                logger.info(f"Global Coherence Pass: Successfully synchronized {len(revised_ids)} sections.")
                
                # Update full_content_so_far from the new truth
                state["full_content_so_far"] = "\n\n".join([s["generated_content"] for s in ordered_sections])
            else:
                missing = original_ids - revised_ids
                extra = revised_ids - original_ids
                logger.warning(f"Global Coherence Pass validation failed. Structural drift detected.")
                logger.warning(f"Missing IDs: {missing} | Extra IDs: {extra}")
                # Fallback: We do nothing to state['sections'], keeping the original work safe.
                
            return state
            
        except Exception as e:
            logger.error(f"Global Coherence Pass failed: {e}")
            return state

    def _apply_heading_only_detox(
        self,
        content_strategy: dict,
        brand_context: str,
        brand_advantages: list,
        writing_blueprint: str,
        primary_keyword: str,
        content_type: str,
        area: str = "",
        seo_intelligence: Optional[dict] = None,
    ) -> tuple:
        """
        Strips heavy investment, legal, and brand-overreach framing from strategy inputs
        when in heading-only mode, to prevent outline drift.
        """
        # 1. Setup deep copies to protect original state
        sanitized_strategy = copy.deepcopy(content_strategy)
        sanitized_brand_context = brand_context
        sanitized_brand_advantages = copy.deepcopy(brand_advantages)
        sanitized_writing_blueprint = writing_blueprint
        
        kw_lower = primary_keyword.lower()

        if content_type == "brand_commercial":
            sanitized_strategy = self.strategy_service._apply_brand_commercial_contract(
                strategy=sanitized_strategy,
                primary_keyword=primary_keyword,
                area=area,
                seo_intelligence=seo_intelligence,
            )

            if sanitized_brand_context:
                sanitized_brand_context = (
                    "Keep the informational flow buyer-first. Use the brand as a soft supporting mention "
                    "only when it helps orientation, and reserve stronger differentiation for the dedicated "
                    "brand or conclusion sections."
                )

            if sanitized_brand_advantages:
                sanitized_brand_advantages = [
                    str(item).strip()
                    for item in sanitized_brand_advantages
                    if str(item).strip()
                ][:3]

            if sanitized_writing_blueprint:
                sanitized_writing_blueprint = (
                    "Keep headings buyer-focused, entity-anchored, comparison-friendly, and easy to expand "
                    "into practical commercial content. Prefer clarity and decision support over markety or "
                    "brand-first phrasing."
                )

            return sanitized_strategy, sanitized_brand_context, sanitized_brand_advantages, sanitized_writing_blueprint
        
        # 2. Heuristic Triggers
        # Investment Triggers: استثمار (investment), عائد (return), ROI, تأجير (rent/lease), resale, capital appreciation
        investment_triggers = ["استثمار", "عائد", "roi", "تأجير", "resale", "capital appreciation", "investment", "yield"]
        # Legal Triggers: عقد (contract), قانوني (legal), ترخيص (license), ملكية (ownership), توثيق (documentation), نزاع (dispute)
        legal_triggers = ["عقد", "قانوني", "ترخيص", "ملكية", "توثيق", "نزاع", "legal", "law", "contract", "dispute"]
        # Commercial Triggers (indicates commercial intent but not investment/legal)
        commercial_triggers = ["buy", "للبيع", "شراء", "price", "سعر", "تجاري", "commercial", "shop"]

        has_investment = any(t in kw_lower for t in investment_triggers)
        has_legal = any(t in kw_lower for t in legal_triggers)
        has_commercial = any(t in kw_lower for t in commercial_triggers) or content_type == "brand_commercial"

        # 3. Sanitize primary_angle (Intent-Aware)
        if has_commercial:
            sanitized_strategy["primary_angle"] = f"Help the reader compare available options for {primary_keyword} and move toward a confident purchase decision."
        else:
            sanitized_strategy["primary_angle"] = f"Help the reader understand {primary_keyword} clearly and answer the main search question."
            
        # 4. Downgrade Authority Strategy
        if not has_investment and not has_legal:
            sanitized_strategy["authority_strategy"] = [
                s for s in sanitized_strategy.get("authority_strategy", [])
                if not any(t in str(s).lower() for t in investment_triggers + legal_triggers)
            ]
            
        # 5. Sanitize section_role_map
        roles = sanitized_strategy.get("section_role_map", {})
        if "introduction" in roles:
            roles["introduction"] = f"Define {primary_keyword} and address core search intent clearly without sales urgency or industry hooks."
        
        if not has_investment:
            if "proof" in roles:
                roles["proof"] = "Show general evidence of quality or standard benefits, avoiding ROI or financial growth metrics."
            if "pricing" in roles:
                roles["pricing"] = f"Outline general costs or factors affecting {primary_keyword} price, avoiding investment/resale framing."

        if not has_legal and "process_or_how" in roles:
             roles["process_or_how"] = "Explain the standard practical steps simply, omitting legal or technical compliance checklists."

        # 6. Compress Brand Context
        if sanitized_brand_context:
            sanitized_brand_context = "Provide objective structural guidance. Brand differentiation should be secondary and used only in conclusion or for unique value-adds, never for pricing or FAQ headings."
            
        # 7. Downgrade Brand Advantages & Writing Blueprint
        if not has_commercial:
            sanitized_brand_advantages = []
            sanitized_writing_blueprint = ""
        else:
            if sanitized_brand_advantages:
                sanitized_brand_advantages = ["Professional service provider with relevant market expertise."]
            if sanitized_writing_blueprint:
                sanitized_writing_blueprint = "Focus on direct value and clear comparisons. Avoid aggressive sales copy."
            
        return sanitized_strategy, sanitized_brand_context, sanitized_brand_advantages, sanitized_writing_blueprint

    def _distill_serp_intelligence(
        self,
        seo_intelligence: dict,
        primary_keyword: str,
        intent: str
    ) -> dict:
        """
        Intercepts and sanitizes SERP/PAA signals to prevent structural drift.
        Downgrades investment/legal signals to factual context unless justified.
        """
        # Deep copy to avoid mutating the original global intelligence
        h_intel = copy.deepcopy(seo_intelligence)
        market_analysis = h_intel.get("market_analysis", {})
        market_insights = market_analysis.get("market_insights", {})
        mandatory_topics = market_insights.get("mandatory_serp_topics", [])
        
        paa_questions = h_intel.get("serp_raw", {}).get("paa_questions", [])
        kw_lower = primary_keyword.lower()
        
        # 1. Triggers (Shared with Strategy Detox)
        investment_triggers = ["استثمار", "عائد", "roi", "تأجير", "resale", "capital appreciation", "investment", "yield"]
        legal_triggers = ["عقد", "قانوني", "ترخيص", "ملكية", "توثيق", "نزاع", "legal", "law", "contract", "dispute"]
        all_drift_triggers = investment_triggers + legal_triggers

        has_justification = any(t in kw_lower for t in all_drift_triggers)
        
        distilled_facts = []
        new_mandatory = []
        
        # 2. Process Mandatory SERP Topics
        for topic in mandatory_topics:
            topic_lower = str(topic).lower()
            contains_drift = any(t in topic_lower for t in all_drift_triggers)
            
            if contains_drift and not has_justification:
                # WEAK SIGNAL: Downgrade to context/facts, remove from mandatory H2s
                distilled_facts.append(f"Competitor signal (Downgraded): {topic}")
                continue
            
            # Check if tied to primary keyword entity
            # e.g. if keyword is "apartments", we want "Apartment prices" not "Real estate prices"
            # This is a soft check for now
            new_mandatory.append(topic)
            
        # 3. Process PAA Questions for Placement
        # If a PAA question is very frequent but drifted, it should be an FAQ candidate, not H2
        paa_faq_candidates = []
        for q in paa_questions:
            q_text = q.get("question", str(q)) if isinstance(q, dict) else str(q)
            if any(t in q_text.lower() for t in all_drift_triggers) and not has_justification:
                paa_faq_candidates.append(q_text)
                
        # 4. Update the localized intelligence view
        market_insights["mandatory_serp_topics"] = new_mandatory
        market_insights["distilled_serp_context"] = {
            "downgraded_competitor_signals": distilled_facts,
            "paa_faq_candidates": paa_faq_candidates,
            "entity_focus_warning": f"Structural focus MUST remain on the entity: '{primary_keyword}'."
        }
        
        # 5. Sanitize Writing Guide
        guide = market_insights.get("writing_guide", "")
        if not has_justification:
            for t in all_drift_triggers:
                if t in guide.lower():
                    guide = guide.replace(t, f"[Sanitized: {t}]")
            market_insights["writing_guide"] = guide

        return h_intel
