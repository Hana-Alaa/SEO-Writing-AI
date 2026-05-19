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
from src.services.outline_repair_service import OutlineRepairService
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
        self.outline_repair_service = OutlineRepairService()

        # Hardened Error Management: Essential steps that MUST succeed
        self.CRITICAL_STEPS = {
            "analysis_init",
            "brand_discovery",
            "web_research",
            "content_strategy",
            "approved_outline_load",
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
        content_only_mode = state.get("input_data", {}).get("content_only_mode", False)
        state["content_only_mode"] = content_only_mode
        content_stage_only_mode = state.get("input_data", {}).get("content_stage_only_mode", False)
        state["content_stage_only_mode"] = bool(content_stage_only_mode)
        topic_packs_enabled = state.get("input_data", {}).get(
            "topic_packs_enabled",
            state.get("topic_packs_enabled", False),
        )
        state["topic_packs_enabled"] = bool(topic_packs_enabled)

        steps = [
            # ("semantic_layer", self._step_semantic_layer, 1),
            ("analysis_init", self._step_0_init, 0),
            ("brand_discovery", self._step_brand_discovery_router, 1),
            ("web_research", self._step_web_research_router, 1),
            ("serp_analysis", self._step_serp_analysis_router, 1),
            ("intent_title", self.strategy_service.run_intent_title, 0),
            ("style_analysis", self.strategy_service.run_style_analysis, 1),
            ("content_strategy", self.strategy_service.run_content_strategy, 3),
        ]

        if content_only_mode:
            logger.info("Content-Only Mode active: using approved outline and skipping outline generation.")
            steps.append(("approved_outline_load", self._step_load_approved_outline, 0))
        else:
            steps.append(("outline_generation", self._step_1_outline, 1))

        steps.extend([
            ("content_writing", self._step_2_write_sections, 1),
        ])

        if not content_stage_only_mode:
            steps.append(("global_coherence", self._step_3_global_coherence_pass, 1))

        # Dynamic Image Skipping
        generate_images = state.get("generate_images", True)
        num_images = state.get("num_images", 7)

        if content_stage_only_mode:
            logger.info("Content Stage Only Mode active: stopping after section writing; skipping coherence/finalization/rendering.")
        else:
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

        final_output = self._assemble_final_output(state)

        # Final Export
        if state.get("workflow_logger"):
            if state.get("heading_only_mode"):
                state["workflow_logger"].log_step_details(
                    "final_heading_response",
                    0,
                    output_data=final_output,
                )
            elif state.get("content_stage_only_mode"):
                state["workflow_logger"].log_step_details(
                    "final_content_stage_response",
                    0,
                    output_data=final_output,
                )
            state["workflow_logger"].export_csv()
            state["workflow_logger"].export_diagnostic_report(state)

        return final_output

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
        state["content_only_mode"] = bool(input_data.get("content_only_mode", False))
        state["content_stage_only_mode"] = bool(input_data.get("content_stage_only_mode", False))
        state["topic_packs_enabled"] = bool(input_data.get("topic_packs_enabled", state.get("topic_packs_enabled", False)))
        state["approved_outline"] = input_data.get("approved_outline")

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

    def _extract_observed_pricing_signals(self, state: Dict[str, Any]) -> List[str]:
        """Extracts numeric pricing patterns from SERP data (titles, snippets, meta)."""
        serp_data = state.get("serp_data", {})
        if not serp_data:
            return []

        # Pricing keywords to filter context
        price_terms = [
            "سعر", "اسعار", "أسعار", "تكلفة", "ريال", "درهم", "ايجار", "إيجار",
            "شهري", "سنوي", "rent", "price", "pricing", "cost", "sar", "aed",
            "fees", "monthly", "yearly", "annual", "starts from", "تبدأ من"
        ]
        
        # Pattern to find numbers with 3+ digits or decimals (e.g., 110,000 or 2.000 or 1500)
        price_pattern = re.compile(r"(\d{3,}(?:[.,\s]\d{3})*(?:\.\d+)?|\d{1,3}(?:[.,\s]\d{3})+(?:\.\d+)?)")
        
        found_mentions = []
        
        def _normalise_price_context(text: str) -> str:
            return (
                str(text or "")
                .lower()
                .replace("إ", "ا")
                .replace("أ", "ا")
                .replace("آ", "ا")
            )

        def _add_text(value: Any, output: List[str]) -> None:
            if isinstance(value, str) and value.strip():
                output.append(value.strip())
            elif isinstance(value, list):
                for item in value:
                    _add_text(item, output)
            elif isinstance(value, dict):
                for item in value.values():
                    _add_text(item, output)

        def _result_text_blobs(result: Any) -> List[str]:
            if not isinstance(result, dict):
                return []
            blobs: List[str] = []
            for key in (
                "title",
                "snippet",
                "description",
                "meta_title",
                "meta_description",
                "h1",
            ):
                _add_text(result.get(key), blobs)
            headings = result.get("headings")
            if isinstance(headings, dict):
                for key in ("h1", "h2", "h3"):
                    _add_text(headings.get(key), blobs)
            return blobs

        # Scrape observed ranking titles, snippets, meta descriptions, and H1/H2/H3 snippets.
        text_blobs: List[str] = []
        for collection_key in ("results", "top_results"):
            for result in serp_data.get(collection_key, []) or []:
                result_blobs = _result_text_blobs(result)
                text_blobs.extend(result_blobs)
                # Keep a result-level blob so a price term in the title can ground
                # a numeric value observed in a meta description or heading.
                if result_blobs:
                    text_blobs.append(" ".join(result_blobs))
        
        # Add PAA and related searches
        _add_text(serp_data.get("paa_questions", []), text_blobs)
        _add_text(serp_data.get("related_searches", []), text_blobs)
        
        for blob in text_blobs:
            if not blob: continue
            blob_l = _normalise_price_context(blob)
            
            # Context Check: only extract if price-related word is nearby
            if any(term in blob_l for term in price_terms):
                matches = price_pattern.findall(blob)
                for match in matches:
                    cleaned = re.sub(r"[.,\s]", "", match)
                    if cleaned.isdigit() and len(cleaned) >= 3:
                        # Extract context: 30 chars before and after
                        start_idx = blob.find(match)
                        context_start = max(0, start_idx - 30)
                        context_end = min(len(blob), start_idx + len(match) + 30)
                        context = blob[context_start:context_end].strip()
                        context = " ".join(context.split())
                        # Avoid obvious guide years such as "2026" unless the
                        # local context also contains a stronger price marker.
                        match_digits = re.sub(r"\D", "", match)
                        if re.fullmatch(r"\d{4}", match_digits):
                            as_int = int(match_digits)
                            local_l = _normalise_price_context(context)
                            strong_price_terms = (
                                "ريال", "درهم", "sar", "aed", "شهري", "سنوي",
                                "monthly", "yearly", "annual", "rent", "ايجار",
                            )
                            if 1900 <= as_int <= 2099 and not any(t in local_l for t in strong_price_terms):
                                continue
                        found_mentions.append(context)

        # Store in state at the required path
        intelligence = state.setdefault("seo_intelligence", {})
        market_analysis = intelligence.setdefault("market_analysis", {})
        market_insights = market_analysis.setdefault("market_insights", {})
        market_data_signals = market_insights.setdefault("market_data_signals", {})
        existing_mentions = market_data_signals.get("observed_price_mentions") or []
        if isinstance(existing_mentions, str):
            existing_mentions = [existing_mentions]
        elif not isinstance(existing_mentions, list):
            existing_mentions = []
        final_mentions = list(dict.fromkeys(
            [str(item).strip() for item in existing_mentions + found_mentions if str(item).strip()]
        ))[:15]
        market_data_signals["observed_price_mentions"] = final_mentions
        
        return final_mentions

    async def _step_web_research_router(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Consolidates research routing."""
        return await self.research_service.run_web_research(state)

    async def _step_serp_analysis_router(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Runs dedicated SERP analysis to extract intent and gaps."""
        state = await self.research_service.run_serp_analysis(state)
        # Extract pricing signals from SERP raw data
        self._extract_observed_pricing_signals(state)
        # Build grounding brief for outline generator
        state["serp_outline_brief"] = self.research_service.build_serp_outline_brief(state)
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
            # Runtime inspection for debugging
            import inspect as _inspect
            logger.error(
                "ACTIVE GEN CLASS: %s | MODULE: %s | SIG: %s",
                self.outline_gen.__class__,
                self.outline_gen.__class__.__module__,
                _inspect.signature(self.outline_gen.generate),
            )

            outline_heading_v2_mode = bool(
                state.get("heading_only_mode") or state.get("content_stage_only_mode")
            )

            # PREFLIGHT CONTRACT CHECK
            _gen_params = _inspect.signature(self.outline_gen.generate).parameters
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
                heading_only_mode=outline_heading_v2_mode,
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

            if outline_heading_v2_mode:
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
                if state.get("enforced_structural_rules"):
                    h_content_strategy = dict(h_content_strategy)
                    h_content_strategy["enforced_structural_rules"] = state.get("enforced_structural_rules", [])
                logger.info(
                    "[TRACER_V1] Heading v2 Detox & Distillation fired for '%s'.",
                    state.get("primary_keyword", ""),
                )

            try:
                # Defensive: only pass serp_outline_brief if the runtime signature supports it
                _gen_kwargs = dict(
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
                    heading_only_mode=outline_heading_v2_mode,
                    head_entity=head_entity,
                    entity_phrase=entity_phrase,
                    service_phrase=service_phrase,
                )
                if "serp_outline_brief" in _inspect.signature(self.outline_gen.generate).parameters:
                    _gen_kwargs["serp_outline_brief"] = state.get("serp_outline_brief")
                else:
                    logger.error(
                        "Runtime generate() does NOT support serp_outline_brief — skipping injection. "
                        "Class: %s | Module: %s",
                        self.outline_gen.__class__,
                        self.outline_gen.__class__.__module__,
                    )
                outline_data = await self.outline_gen.generate(**_gen_kwargs)
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
            # Use this flag to bypass heavy structural/semantic rules
            heading_only_relaxed_validation = outline_heading_v2_mode

            if not heading_only_relaxed_validation:
                if outline_heading_v2_mode:
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

                # TASK 2: Deterministic Repairs (Visitor Intent Promotion)
                outline = self.outline_repair_service.promote_visitor_intents(
                    outline,
                    primary_keyword=state.get("primary_keyword", ""),
                    entity_phrase=entity_phrase,
                    serp_brief=state.get("serp_outline_brief")
                )

                # TASK 3: FAQ De-duplication
                outline = self.outline_repair_service.dedupe_faq_against_h2(outline)
                # TASK 3b: FAQ Refill (restore minimum 4 FAQs after dedupe)
                outline = self.outline_repair_service.refill_faq_after_dedupe(
                    outline,
                    entity_phrase=entity_phrase
                )

                # TASK 3c: Deterministic FAQ Enrichment (Brand Utility)
                outline = self.outline_repair_service.enrich_brand_utility_faq(
                    outline,
                    serp_brief=state.get("serp_outline_brief", {}),
                    brand_context=state.get("brand_name", "") or state.get("display_brand_name", ""),
                    content_type=content_type,
                    entity_phrase=entity_phrase
                )
                outline = self.outline_repair_service.normalize_heading_only_section_types(outline)

                # Apply Anti-Echo and Strategic Map Repairs
                outline = self.outline_repair_service.clean_echo_and_repetition(
                    outline, 
                    title=state.get("title", ""),
                    primary_keyword=state.get("primary_keyword", "")
                )
                outline = self.outline_repair_service.apply_strategic_map_and_roles(
                    outline,
                    primary_keyword=state.get("primary_keyword", ""),
                    content_type=content_type
                )

                # TASK 4: Conclusion Cleanup
                outline = self.outline_repair_service.clean_conclusion_heading(
                    outline,
                    entity_phrase=entity_phrase
                )

                # 3. Quality (Thin, Duplicates, CTAs)
                if outline_heading_v2_mode:
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
                        primary_keyword=state.get("primary_keyword", ""),
                        serp_brief=state.get("serp_outline_brief"),
                        content_strategy=content_strategy,
                    )
                errors.extend(quality_errors)
            else:
                logger.info("Heading-only mode: Heavy quality validation and deterministic repairs bypassed.")

            if outline_heading_v2_mode:
                # Keep lightweight, deterministic heading-only fixes active even when
                # heavy validation is relaxed. These do not force a regeneration and
                # protect practical visitor intents such as brand-assisted booking.
                outline = self.outline_repair_service.dedupe_faq_against_h2(outline)
                outline = self.outline_repair_service.enrich_brand_utility_faq(
                    outline,
                    serp_brief=state.get("serp_outline_brief", {}),
                    brand_context=state.get("brand_name", "") or state.get("display_brand_name", ""),
                    content_type=content_type,
                    entity_phrase=entity_phrase,
                )
                outline = self.outline_repair_service.normalize_heading_only_section_types(outline)
                outline = self.outline_repair_service.clean_conclusion_heading(
                    outline,
                    entity_phrase=entity_phrase,
                )

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

        if state.get("content_stage_only_mode"):
            state = self._prepare_outline_for_content(
                state,
                outline,
                source="heading_v2_generated_outline",
            )
            outline = state.get("outline", outline)
            present_types = {sec.get("section_type") for sec in outline}

        missing = self.validator._missing_required_sections(present_types, mandatory)

        if missing:
            logger.error(f"[outline_validate] Missing mandatory sections: {missing}")
            # we could raise error or just log depending on strictness
            # raise ValueError(f"Missing mandatory sections: {missing}")

        if state.get("heading_only_mode"):
            try:
                audit_brand_name = state.get("brand_name") or state.get("display_brand_name") or ""
                audit_display_brand_name = state.get("display_brand_name") or audit_brand_name
                report = self.validator.audit_heading_outline_quality(
                    outline=outline,
                    content_type=content_type,
                    area=area,
                    primary_keyword=primary_keyword,
                    brand_name=audit_brand_name,
                    display_brand_name=audit_display_brand_name,
                    content_strategy=content_strategy,
                    seo_intelligence=seo_intelligence,
                    entity_phrase=entity_phrase,
                    service_phrase=service_phrase
                )
                state["heading_quality_audit"] = report
                if state.get("workflow_logger"):
                    state["workflow_logger"].log_event("heading_quality_audit", report)
                logger.info(f"Heading quality audit complete. Passed: {report.get('passed')}")

                # AI Outline Critique (Diagnostic Only)
                if (
                    state.get("outline")
                    and state.get("heading_only_mode")
                    and hasattr(self.outline_gen, "critique_outline")
                ):
                    try:
                        critique = await self.outline_gen.critique_outline(
                            primary_keyword=primary_keyword,
                            title=title,
                            outline=outline,
                            intent=intent,
                            area=area or "",
                            entity_phrase=entity_phrase or "",
                            service_phrase=service_phrase or "",
                            display_brand_name=audit_display_brand_name,
                            content_strategy=content_strategy,
                            heading_quality_audit=report
                        )
                        state["ai_outline_critique"] = critique
                        if state.get("workflow_logger"):
                            state["workflow_logger"].log_event("ai_outline_critique", critique)
                        logger.info("AI Outline Critique complete.")
                    except Exception as crit_e:
                        logger.error(f"AI Outline Critique step failed: {crit_e}")

                # Controlled Heading Fix Layer: disabled by default. Audit mode must not mutate outlines.
                if (
                    state.get("heading_only_mode")
                    and state.get("heading_fix_enabled") is True
                    and hasattr(self.outline_gen, "fix_outline_headings")
                ):
                    state["heading_quality_audit_before_fix"] = state.get("heading_quality_audit")
                    state["ai_outline_critique_before_fix"] = state.get("ai_outline_critique")

                    fix_result = await self._run_controlled_heading_fix(state)
                    state["heading_fix"] = fix_result

                    if fix_result.get("accepted"):
                        logger.info("Heading fix candidate accepted and applied.")
                    else:
                        logger.info(f"Heading fix candidate rejected: {fix_result.get('reason')}")

            except Exception as e:
                import traceback
                logger.error(f"Heading quality audit failed: {e}\n{traceback.format_exc()}")

        return state

    async def _run_controlled_heading_fix(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Orchestrates the controlled heading fix layer with validation."""
        outline = state.get("outline", [])
        if not outline:
            return {"enabled": True, "attempted": False, "accepted": False, "reason": "No outline to fix"}

        audit = state.get("heading_quality_audit", {})
        critique = state.get("ai_outline_critique", {})

        # Actionable check v1: only run if there are warnings or critique issues
        has_warnings = bool(audit.get("warnings"))
        has_critique_issues = critique.get("overall_score", 10) < 9.0

        if not (has_warnings or has_critique_issues):
            return {"enabled": True, "attempted": False, "accepted": False, "reason": "No actionable issues detected"}

        input_data = state.get("input_data", {})
        primary_keyword = str(state.get("primary_keyword", ""))
        content_type = state.get("content_type", "informational")
        area = state.get("area", "")
        entity_phrase = state.get("entity_phrase", "")
        service_phrase = state.get("service_phrase", "")
        display_brand_name = state.get("display_brand_name", "")
        content_strategy = copy.deepcopy(state.get("content_strategy", {}))

        # Inject calibration rules into strategy for the fix layer
        calibration_rules = [
            "H3 consistency: All H3s inside an 'Offer' section must reflect the same intent (e.g. rental) as the parent H2.",
            "Intent words: Ensure intent words like 'للايجار' are present where appropriate.",
            "Generic H2 tightening: Rewrite generic patterns like 'أهم المزايا' or 'المرافق' to be decision-focused (e.g. 'المزايا التي يجب توفرها عند استئجار شقة').",
            "Semantic consistency: All headings must stay aligned with entity_phrase, service_phrase, and intent."
        ]
        content_strategy["heading_quality_calibration"] = calibration_rules

        logger.info("Attempting controlled heading fix with semantic calibration...")
        fix_data = await self.outline_gen.fix_outline_headings(
            primary_keyword=primary_keyword,
            outline=outline,
            area=area,
            entity_phrase=entity_phrase,
            service_phrase=service_phrase,
            display_brand_name=display_brand_name,
            content_strategy=content_strategy,
            heading_quality_audit=audit,
            ai_outline_critique=critique
        )

        fixed_candidate = fix_data.get("outline", [])
        raw_changes = fix_data.get("changes", [])

        if not fixed_candidate or fixed_candidate == outline:
            logger.info("Heading fix: AI proposed no changes.")
            return {"enabled": True, "attempted": True, "accepted": False, "reason": "No changes proposed by AI"}

        # --- Tightening Layer: Revert Over-edits ---
        final_fixed = []
        final_changes = []

        # Helper to identify sections with issues
        warned_section_ids = {w.get("section_id") for w in audit.get("warnings", []) if w.get("section_id")}

        # FIX: Critique categories are lists of dicts, must extract section_id
        critique_issue_ids = set()
        for category in ["weak_sections", "h3_issues", "brand_alignment_issues", "faq_issues"]:
            for item in critique.get(category, []):
                if isinstance(item, dict) and item.get("section_id"):
                    critique_issue_ids.add(item.get("section_id"))

        # Repetition issues have a list of sections
        for item in critique.get("repetition_issues", []):
            if isinstance(item, dict) and item.get("sections"):
                for sid in item.get("sections", []):
                    critique_issue_ids.add(sid)

        problematic_ids = warned_section_ids | critique_issue_ids

        logger.info(f"Heading fix debugging: Problematic section IDs: {problematic_ids}")

        # Helper for intro severity
        intro_severity = "low"
        intro_warnings = [w for w in audit.get("warnings", []) if w.get("section_id") == "sec_01" or w.get("heading_level") == "INTRO"]
        if any(w.get("severity") in ["medium", "high"] for w in intro_warnings):
            intro_severity = "medium"

        for orig, fixed in zip(outline, fixed_candidate):
            sid = orig.get("section_id")
            stype = orig.get("section_type")

            revert = False

            # Rule 4: Do NOT modify CONCLUSION
            if stype == "conclusion":
                revert = True

            # Rule 4: Do NOT modify INTRO unless severity >= medium
            elif stype == "introduction" or orig.get("heading_level") == "INTRO":
                if intro_severity == "low":
                    revert = True

            # Rule 3: Only modify sections that have audit warnings or critique issues
            elif sid not in problematic_ids:
                logger.debug(f"Reverting change to section {sid} as it was not flagged as problematic.")
                revert = True

            if revert:
                final_fixed.append(orig)
            else:
                if orig.get("heading_text") != fixed.get("heading_text"):
                    logger.info(f"Applying fix to section {sid}: '{orig.get('heading_text')}' -> '{fixed.get('heading_text')}'")
                final_fixed.append(fixed)
                # Keep changes for this section
                for c in raw_changes:
                    if c.get("section_id") == sid:
                        final_changes.append(c)

        fixed_candidate = final_fixed
        changes = final_changes

        if fixed_candidate == outline:
            logger.warning("Heading fix: All proposed changes were reverted by tightening layer. Check problematic_ids logic.")
            return {"enabled": True, "attempted": True, "accepted": False, "reason": "All AI changes were reverted by tightening layer (over-editing prevention)"}

        # 1. Structural Validation
        if len(fixed_candidate) != len(outline):
            return {"enabled": True, "attempted": True, "accepted": False, "reason": "Structural failure: Section count changed", "changes": changes}

        for orig, fixed in zip(outline, fixed_candidate):
            for field in ["section_id", "section_type", "section_intent", "heading_level"]:
                if orig.get(field) != fixed.get(field):
                    return {"enabled": True, "attempted": True, "accepted": False, "reason": f"Structural failure: Field {field} changed in section {orig.get('section_id')}", "changes": changes}

        # 2. Quality Validation (Rerun Audit)
        try:
            audit_brand_name = state.get("brand_name") or state.get("display_brand_name") or ""
            audit_display_brand_name = state.get("display_brand_name") or audit_brand_name
            new_report = self.validator.audit_heading_outline_quality(
                outline=fixed_candidate,
                content_type=content_type,
                area=area,
                primary_keyword=primary_keyword,
                brand_name=audit_brand_name,
                display_brand_name=audit_display_brand_name,
                content_strategy=content_strategy,
                seo_intelligence=state.get("seo_intelligence", {}),
                entity_phrase=entity_phrase,
                service_phrase=service_phrase
            )

            old_warnings_count = len(audit.get("warnings", []))
            new_warnings_count = len(new_report.get("warnings", []))

            # Reject if warnings increased
            if new_warnings_count > old_warnings_count:
                return {
                    "enabled": True,
                    "attempted": True,
                    "accepted": False,
                    "reason": f"Quality failure: Warnings increased from {old_warnings_count} to {new_warnings_count}",
                    "warnings_before": old_warnings_count,
                    "warnings_after": new_warnings_count,
                    "changes": changes
                }

            # Check for new HIGH severity warnings
            old_high = [w for w in audit.get("warnings", []) if w.get("severity") == "high"]
            new_high = [w for w in new_report.get("warnings", []) if w.get("severity") == "high"]
            if len(new_high) > len(old_high):
                 return {
                    "enabled": True,
                    "attempted": True,
                    "accepted": False,
                    "reason": "Quality failure: New high-severity warnings introduced",
                    "changes": changes
                }

            # Acceptance!
            state["outline"] = fixed_candidate
            state["heading_quality_audit"] = new_report
            # We don't rerun critique to save tokens/time as requested (diagnostic only)

            return {
                "enabled": True,
                "attempted": True,
                "accepted": True,
                "reason": "Applied fixes successfully",
                "warnings_before": old_warnings_count,
                "warnings_after": new_warnings_count,
                "changed_sections": [c.get("section_id") for c in changes],
                "changes": changes
            }

        except Exception as e:
            logger.error(f"Validation of fixed outline failed: {e}")
            return {"enabled": True, "attempted": True, "accepted": False, "reason": f"Validation error: {str(e)}", "changes": changes}

    def _subheading_text(self, item: Any) -> str:
        if isinstance(item, dict):
            return str(item.get("heading_text") or item.get("text") or item.get("question") or "").strip()
        return str(item or "").strip()

    def _parse_approved_outline_payload(self, payload: Any) -> tuple[str, List[Dict[str, Any]]]:
        """Parse a heading-review response or raw outline list without changing headings."""
        if not payload:
            raise StructureError("Content-only mode requires an approved_outline payload.")

        parsed = payload
        if isinstance(payload, str):
            raw = payload.strip()
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = recover_json(raw)

        title = ""
        outline = None
        if isinstance(parsed, dict):
            title = str(parsed.get("title") or "").strip()
            outline = (
                parsed.get("outline_structure")
                or parsed.get("outline")
                or parsed.get("sections")
            )
        elif isinstance(parsed, list):
            outline = parsed

        if not isinstance(outline, list) or not outline:
            raise StructureError("approved_outline must be a non-empty list or a heading response object.")

        cleaned = []
        for idx, section in enumerate(outline, start=1):
            if not isinstance(section, dict):
                continue
            sec = dict(section)
            if not sec.get("heading_text"):
                sec["heading_text"] = sec.get("note") or (
                    "Opening hook" if idx == 1 else f"Section {idx}"
                )
            sec.setdefault("section_id", f"sec_{idx:02d}")
            sec.setdefault("heading_level", "INTRO" if idx == 1 else "H2")
            sec.setdefault("section_type", "introduction" if idx == 1 else "core")
            sec.setdefault("section_intent", "informational")
            sec["subheadings"] = [
                text for text in (self._subheading_text(item) for item in sec.get("subheadings", []) or [])
                if text
            ]
            cleaned.append(sec)

        if not cleaned:
            raise StructureError("approved_outline did not contain any valid section objects.")

        return title, cleaned

    def _infer_contract_format(self, section: Dict[str, Any]) -> str:
        section_type = (section.get("section_type") or "").lower()
        subheadings = section.get("subheadings") or []
        heading_blob = " ".join([
            str(section.get("heading_text") or ""),
            " ".join(self._subheading_text(item) for item in subheadings),
        ]).lower()
        comparison_terms = ("مقارنة", "الفرق", "مقابل", "compare", "comparison", "versus", " vs ")
        criteria_terms = ("معايير", "اختيار", "تختار", "كيف تختار", "criteria", "choose", "selection")
        if section.get("requires_table") or section_type in {"comparison", "pricing"} or any(term in heading_blob for term in comparison_terms):
            return "table" if not subheadings else "mixed"
        if section.get("requires_list"):
            return "bullets"
        if section_type in {"process", "process_or_how"} or any(term in heading_blob for term in criteria_terms):
            return "bullets" if not subheadings else "mixed"
        if section_type == "faq" or subheadings:
            return "mixed"
        return "paragraphs"

    def _decompose_heading_promises(self, heading: str, state: Dict[str, Any]) -> List[str]:
        """Turn compound H2 promises into explicit execution targets for the writer."""
        heading_l = str(heading or "").lower()
        is_ar = bool(re.search(r"[\u0600-\u06FF]", heading_l)) or str(
            state.get("article_language") or ""
        ).lower().startswith("ar")
        promises: List[str] = []

        if any(term in heading_l for term in ("أنواع", "نوع", "خيارات", "تصنيفات", "types", "options", "categories")):
            promises.append(
                "فرّق بين الأنواع أو الخيارات المذكورة بوضوح عملي."
                if is_ar
                else "Clearly differentiate the mentioned types or options."
            )
        if any(term in heading_l for term in ("كيف تختار", "طريقة الاختيار", "اختيار", "تختار", "how to choose", "choose", "selection")):
            promises.append(
                "اشرح كيف يختار القارئ الخيار الأنسب باستخدام معايير عملية، وليس وصف الأنواع فقط."
                if is_ar
                else "Explain how the reader should choose the right option using practical criteria, not only type descriptions."
            )
        if any(term in heading_l for term in ("معايير", "criteria")):
            promises.append(
                "قدّم المعايير في نقاط واضحة، مع نتيجة أو قرار مرتبط بكل معيار."
                if is_ar
                else "Present criteria as clear points, with a decision impact for each one."
            )
        if any(term in heading_l for term in ("مقارنة", "الفرق", "مقابل", "compare", "comparison", "versus", " vs ")):
            promises.append(
                "حوّل المقارنة إلى فروق قابلة للمسح، ويفضل جدول عند توفر مساحة الجداول."
                if is_ar
                else "Turn the comparison into scannable differences, preferably a table when table slots are available."
            )
        return promises

    def _infer_brand_policy(self, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        brand_name = state.get("brand_name") or state.get("display_brand_name") or ""
        if not brand_name:
            return "none"

        content_type = (state.get("content_type") or "").lower()
        intent = (state.get("intent") or "").lower()
        if content_type == "brand_commercial" or "commercial" in intent:
            return "commercial"

        text = " ".join([
            section.get("heading_text", ""),
            " ".join(section.get("subheadings", []) or []),
            state.get("primary_keyword", ""),
        ]).lower()
        strategy_terms = (
            "strategy", "implementation", "service", "seo", "sem", "ppc",
            "استراتيجية", "تنفيذ", "تطبيق", "خدمة", "خدمات", "تسويق"
        )
        if brand_name.lower() in text or any(term in text for term in strategy_terms):
            return "soft_implementation"
        return "none"

    def _infer_location_policy(self, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        area = str(state.get("area") or "").strip()
        if not area or area.lower() in {"global", "general", "international"}:
            return "neutral"

        content_type = (state.get("content_type") or "").lower()
        primary_keyword = str(state.get("primary_keyword") or "")
        heading_text = " ".join([
            section.get("heading_text", ""),
            " ".join(section.get("subheadings", []) or []),
        ])
        section_type = (section.get("section_type") or "").lower()

        if area.lower() in primary_keyword.lower() or area.lower() in heading_text.lower() or section_type in {
            "location", "visitor_information"
        }:
            return "local_required"
        if content_type in {"brand_commercial", "listing", "real_estate"}:
            return "local_allowed"
        return "neutral"

    def _build_section_contract(
        self,
        section: Dict[str, Any],
        outline: List[Dict[str, Any]],
        index: int,
        state: Dict[str, Any],
    ) -> Dict[str, Any]:
        subheadings = [self._subheading_text(item) for item in section.get("subheadings", []) or []]
        subheadings = [item for item in subheadings if item]
        heading = str(section.get("heading_text") or "").strip()
        section_type = (section.get("section_type") or "").lower()

        if section_type == "introduction" or (section.get("heading_level") or "").upper() == "INTRO":
            must_answer = [
                f"Open with a specific, non-generic hook for {state.get('primary_keyword', heading)}",
                "Start from a concrete reader tension, trade-off, mistake risk, or decision problem",
                "Orient the reader without defining the topic in detail",
            ]
        elif section_type == "faq" and subheadings:
            must_answer = subheadings
        else:
            must_answer = [heading] + self._decompose_heading_promises(heading, state) + subheadings

        prior = []
        for prev in outline[:index]:
            prev_heading = str(prev.get("heading_text") or "").strip()
            if prev_heading:
                prior.append(prev_heading)
            for sub in prev.get("subheadings", []) or []:
                sub_text = self._subheading_text(sub)
                if sub_text:
                    prior.append(sub_text)

        contract = {
            "must_answer": list(dict.fromkeys([item for item in must_answer if item])),
            "must_not_repeat": list(dict.fromkeys(prior[-8:])),
            "format": self._infer_contract_format(section),
            "brand_policy": self._infer_brand_policy(section, state),
            "location_policy": self._infer_location_policy(section, state),
        }
        return contract

    def _infer_taxonomy_axis(self, section: Dict[str, Any]) -> str:
        """Infer a broad editorial axis without making topic-specific assumptions."""
        section_type = str(section.get("section_type") or "").lower()
        heading_blob = " ".join([
            str(section.get("heading_text") or ""),
            " ".join(self._subheading_text(item) for item in section.get("subheadings", []) or []),
        ]).lower()

        if section_type == "faq":
            return "faq"
        if section_type == "conclusion":
            return "conclusion"
        if section_type in {"differentiators", "brand_support", "brand", "testimonials"}:
            return "brand_support"
        if any(term in heading_blob for term in ("سعر", "أسعار", "تكلفة", "ميزانية", "price", "pricing", "cost", "budget", "fee")):
            return "pricing"
        if section_type in {"location", "visitor_information"} or any(
            term in heading_blob
            for term in ("منطقة", "مناطق", "أحياء", "حي ", "موقع", "أين", "location", "area", "district", "neighborhood", "where")
        ):
            return "location_area"
        if any(term in heading_blob for term in ("أنواع", "نوع", "خيارات", "تصنيفات", "فئات", "types", "options", "categories")):
            return "category_or_type"
        if section_type in {"process", "process_or_how"} or any(
            term in heading_blob for term in ("خطوات", "طريقة", "كيف", "process", "steps", "how")
        ):
            return "process"
        if section_type == "comparison" or any(term in heading_blob for term in ("مقارنة", "الفرق", "comparison", "versus", " vs ")):
            return "comparison"
        if section_type in {"introduction"} or str(section.get("heading_level") or "").upper() == "INTRO":
            return "introduction"
        return "criteria"

    def _collect_observed_data_mentions(self, section: Dict[str, Any], state: Dict[str, Any]) -> List[str]:
        """Read already-observed SERP/market signals; do not parse or infer new data."""
        seo_intelligence = state.get("seo_intelligence", {}) if isinstance(state.get("seo_intelligence", {}), dict) else {}
        market = (
            seo_intelligence.get("market_analysis", {})
            .get("market_insights", {})
            if isinstance(seo_intelligence.get("market_analysis", {}), dict)
            else {}
        )
        market_signals = market.get("market_data_signals", {}) if isinstance(market.get("market_data_signals", {}), dict) else {}
        semantic_assets = (
            seo_intelligence.get("market_analysis", {})
            .get("semantic_assets", {})
            if isinstance(seo_intelligence.get("market_analysis", {}), dict)
            else {}
        )
        serp_data = state.get("serp_data", {}) if isinstance(state.get("serp_data", {}), dict) else {}

        candidates: List[str] = []

        def add_value(value: Any) -> None:
            if isinstance(value, str) and value.strip():
                candidates.append(value.strip())
            elif isinstance(value, list):
                for item in value:
                    add_value(item)
            elif isinstance(value, dict):
                for item in value.values():
                    add_value(item)

        for key in (
            "observed_price_mentions",
            "avg_unit_price_range",
            "common_down_payment_or_fees",
            "typical_duration_or_terms",
            "notable_market_trends",
        ):
            add_value(market_signals.get(key))
        for key in (
            "paa_questions",
            "related_searches",
            "autocomplete_suggestions",
            "lsi_keywords",
            "common_strengths",
            "common_patterns",
            "observed_notes",
        ):
            add_value(serp_data.get(key))
            add_value(semantic_assets.get(key))

        heading_terms = set(
            term.strip("،:؛؟?!.,()[]{}\"'").lower()
            for term in " ".join([
                str(section.get("heading_text") or ""),
                " ".join(self._subheading_text(item) for item in section.get("subheadings", []) or []),
                str(state.get("primary_keyword") or ""),
            ]).split()
            if len(term.strip("،:؛؟?!.,()[]{}\"'")) > 2
        )

        filtered = []
        for item in candidates:
            item_l = item.lower()
            if not heading_terms or any(term in item_l for term in heading_terms):
                filtered.append(item)
        if not filtered:
            filtered = candidates
        return list(dict.fromkeys(filtered))[:6]

    def _enrichment_text(self, state: Dict[str, Any], arabic: str, english: str) -> str:
        lang = str(state.get("article_language") or state.get("input_data", {}).get("article_language") or "").lower()
        primary = str(state.get("primary_keyword") or "")
        if lang.startswith("ar") or re.search(r"[\u0600-\u06FF]", primary):
            return arabic
        return english

    def _detect_active_topic_packs(self, state: Dict[str, Any]) -> List[str]:
        """Detect thematic detail packs from keyword and observed SERP signals only."""
        active_packs = []
        input_data = state.get("input_data", {}) if isinstance(state.get("input_data", {}), dict) else {}
        if not bool(state.get("topic_packs_enabled", input_data.get("topic_packs_enabled", False))):
            return active_packs

        def _normalise_signal(value: Any) -> str:
            text = str(value or "").lower()
            text = (
                text.replace("إ", "ا")
                .replace("أ", "ا")
                .replace("آ", "ا")
                .replace("ى", "ي")
            )
            return re.sub(r"\s+", " ", text).strip()

        def _collect_text(value: Any) -> List[str]:
            if isinstance(value, str):
                return [value]
            if isinstance(value, list):
                items: List[str] = []
                for item in value:
                    items.extend(_collect_text(item))
                return items
            if isinstance(value, dict):
                items = []
                for item in value.values():
                    items.extend(_collect_text(item))
                return items
            return []

        def _has_rental_signal(value: Any) -> bool:
            text = _normalise_signal(value)
            if not text:
                return False
            arabic_terms = (
                "شقق للايجار",
                "شقة للايجار",
                "للايجار",
                "للايجار",
                "ايجار شقق",
            )
            if any(term in text for term in arabic_terms):
                return True
            english_patterns = (
                r"\bapartments?\s+for\s+rent\b",
                r"\bapartment\b",
                r"\brentals?\b",
                r"\brent\b",
            )
            return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in english_patterns)

        keyword_sources = [
            state.get("primary_keyword"),
            state.get("raw_title"),
            state.get("input_data", {}).get("title") if isinstance(state.get("input_data"), dict) else "",
            state.get("keywords"),
        ]
        if any(_has_rental_signal(source) for source in keyword_sources):
            active_packs.append("rental_real_estate_pack")

        if "rental_real_estate_pack" not in active_packs:
            serp_data = state.get("serp_data", {}) if isinstance(state.get("serp_data"), dict) else {}
            seo_intelligence = state.get("seo_intelligence", {}) if isinstance(state.get("seo_intelligence"), dict) else {}
            serp_sources = _collect_text(serp_data) + _collect_text(
                seo_intelligence.get("market_analysis", {}) if isinstance(seo_intelligence.get("market_analysis", {}), dict) else {}
            )
            if any(_has_rental_signal(source) for source in serp_sources):
                active_packs.append("rental_real_estate_pack")

        return active_packs

    def _topic_pack_details(self, pack: str, taxonomy_axis: str, state: Dict[str, Any]) -> List[str]:
        """Returns role-specific enrichment details for a given topic pack."""
        if pack == "rental_real_estate_pack":
            axis = "pricing" if taxonomy_axis in {"pricing_by_area", "pricing_by_type"} else taxonomy_axis
            details = {
                "introduction": [
                    self._enrichment_text(state, "سياق السوق العقاري في المدينة المذكورة.", "Real estate market context for the specified city."),
                    self._enrichment_text(state, "اختلاف الطلب والاختيارات حسب الأحياء ونمط السكن.", "How demand and options vary by neighborhood and living pattern."),
                ],
                "category_or_type": [
                    self._enrichment_text(state, "سياق المدينة وتنوّع خيارات الإيجار داخلها.", "City context and the variety of rental options within it."),
                    self._enrichment_text(state, "توقعات عدد الغرف والمساحات المتاحة.", "Room-count or size expectations."),
                    self._enrichment_text(state, "الفروق بين الشقق المفروشة وغير المفروشة.", "Differences between furnished and unfurnished units."),
                    self._enrichment_text(state, "مدى ملاءمة الشقق للعزاب أو العوائل.", "Suitability for bachelors vs families."),
                ],
                "location_area": [
                    self._enrichment_text(state, "تنوع الأحياء السكنية وتصنيفاتها.", "Neighborhood variation and classifications."),
                    self._enrichment_text(state, "القرب من الخدمات والمدارس والطرق وأماكن العمل والمعالم القريبة.", "Proximity to services, schools, roads, workplaces, and nearby landmarks."),
                    self._enrichment_text(state, "مدى ملاءمة المنطقة للعوائل أو الأفراد حسب نمط الحياة.", "How the area fits families or individuals based on lifestyle."),
                ],
                "pricing": [
                    self._enrichment_text(state, "محركات الأسعار: الموقع، المساحة، التأثيث، الخدمات، والقرب من المدارس أو الطرق أو أماكن العمل.", "Price drivers: location, size, furnishing, services, and proximity to schools, roads, or workplaces."),
                    self._enrichment_text(state, "أهمية الإيجار الشهري مقابل السنوي.", "Relevance of monthly vs yearly rental terms."),
                    self._enrichment_text(state, "استخدم مستويات سعرية نسبية عند غياب أرقام موثوقة.", "Use relative pricing tiers when reliable numbers are missing."),
                ],
                "process": [
                    self._enrichment_text(state, "اعتبارات الفحص العملي للعين أو بنود العقد.", "Practical inspection or contract considerations."),
                    self._enrichment_text(state, "مراجعة مدة الإيجار وطريقة الدفع وما يشمله السعر من خدمات.", "Review rental duration, payment method, and services included in the price."),
                ],
                "criteria": [
                    self._enrichment_text(state, "اربط الاختيار بالمساحة وعدد الغرف ونمط السكن.", "Connect the choice to space, room count, and living pattern."),
                    self._enrichment_text(state, "وضّح أثر الموقع والتأثيث والخدمات القريبة على القرار.", "Explain how location, furnishing, and nearby services affect the decision."),
                ],
                "brand_support": [
                    self._enrichment_text(state, "وضح كيف يساعد البراند في مقارنة خيارات الإيجار حسب الموقع والمساحة والتأثيث.", "Explain how the brand helps compare rental options by location, size, and furnishing."),
                ]
            }
            return details.get(axis, [])
        return []

    def _section_contract_details(self, taxonomy_axis: str, state: Dict[str, Any]) -> List[str]:
        detail_map = {
            "introduction": [
                self._enrichment_text(state, "حدد المشكلة أو الحاجة الأساسية التي جاء القارئ بسببها.", "Identify the core problem or need that brought the reader here."),
                self._enrichment_text(state, "ابدأ بتوتر أو مخاطرة قرار محددة، وليس بجملة افتتاحية عامة.", "Start with a concrete tension or decision risk, not a generic opening."),
                self._enrichment_text(state, "مهّد للموضوع دون الدخول في تفاصيل السكاشن اللاحقة.", "Set up the topic without leaking details from later sections."),
            ],
            "category_or_type": [
                self._enrichment_text(state, "فرّق بين الخيارات أو الفئات بوضوح عملي.", "Differentiate the options or categories in a practical way."),
                self._enrichment_text(state, "اذكر متى يناسب كل خيار نوعًا مختلفًا من القراء أو الاحتياجات.", "Explain when each option fits a different reader need."),
                self._enrichment_text(state, "إذا وعد العنوان بطريقة الاختيار، أضف خلاصة واضحة لكيف يختار القارئ بين الخيارات.", "If the heading promises how to choose, add a clear takeaway on how the reader should choose among options."),
                self._enrichment_text(state, "اجعل كل فئة تضيف معلومة مختلفة لا تصلح لكل الفئات الأخرى.", "Make each category provide a distinct insight that cannot apply to every other category."),
            ],
            "location_area": [
                self._enrichment_text(state, "اربط كل موقع أو منطقة بسبب عملي يهم القارئ.", "Connect each location or area to a practical reader reason."),
                self._enrichment_text(state, "وضح أثر القرب أو الوصول أو الخدمات على القرار دون ادعاءات غير مدعومة.", "Explain how proximity, access, or services affect the decision without unsupported claims."),
            ],
            "pricing": [
                self._enrichment_text(state, "وضح العوامل التي تغيّر السعر أو التكلفة.", "Explain the factors that change price or cost."),
                self._enrichment_text(state, "استخدم البيانات المرصودة بحذر، أو قدّم مستويات نسبية واضحة عند غياب الأرقام الموثوقة.", "Use observed data carefully, or provide clear relative tiers when reliable numbers are missing."),
                self._enrichment_text(state, "اجعل القارئ يفهم كيف يوازن بين السعر والقيمة.", "Help the reader understand how to balance price and value."),
            ],
            "comparison": [
                self._enrichment_text(state, "اعرض الفروق التي تغيّر قرار القارئ فعلًا.", "Focus on differences that materially change the reader's decision."),
                self._enrichment_text(state, "تجنب المقارنة العامة واذكر معيارًا واضحًا لكل فرق.", "Avoid generic comparison; attach each difference to a clear criterion."),
                self._enrichment_text(state, "استخدم جدولًا للمقارنة إذا كان عدد الجداول المتاح يسمح بذلك، وإلا استخدم نقاطًا منظمة.", "Use a comparison table when table slots allow it; otherwise use structured bullets."),
            ],
            "criteria": [
                self._enrichment_text(state, "حوّل العنوان إلى معايير عملية يمكن للقارئ استخدامها.", "Turn the heading into practical criteria the reader can use."),
                self._enrichment_text(state, "اربط كل معيار بنتيجة أو قرار واضح.", "Tie every criterion to a clear outcome or decision."),
                self._enrichment_text(state, "اكتب المعايير في نقاط قابلة للمسح بدل فقرة طويلة عامة.", "Write criteria as scannable bullets instead of one long generic paragraph."),
            ],
            "process": [
                self._enrichment_text(state, "رتب الخطوات أو الطريقة بشكل منطقي قابل للتطبيق.", "Order the steps or method in a practical sequence."),
                self._enrichment_text(state, "اذكر ما يجب الانتباه له في كل مرحلة مهمة.", "Mention what to watch for at each important stage."),
            ],
            "brand_support": [
                self._enrichment_text(state, "اربط دور البراند بالمشكلة العملية التي يحاول القارئ حلها.", "Tie the brand role to the practical problem the reader is trying to solve."),
                self._enrichment_text(state, "اجعل ذكر البراند مساعدًا ومحددًا لا دعائيًا عامًا.", "Keep brand mentions specific and helpful, not generic promotion."),
            ],
            "faq": [
                self._enrichment_text(state, "أجب عن كل سؤال مباشرة قبل أي تفصيل إضافي.", "Answer each question directly before adding detail."),
                self._enrichment_text(state, "اجعل الإجابات قصيرة ومفيدة وغير مكررة للمتن السابق.", "Keep answers concise, useful, and not repetitive of earlier sections."),
            ],
            "conclusion": [
                self._enrichment_text(state, "لخّص القرار أو الفائدة النهائية دون إعادة تفاصيل السكاشن.", "Synthesize the final decision or value without repeating section details."),
                self._enrichment_text(state, "اختم بتوجيه عملي واضح يناسب نية المقال.", "Close with a practical next step aligned with the article intent."),
            ],
        }
        return detail_map.get(taxonomy_axis, detail_map["criteria"])

    def _plan_taxonomy_axis(
        self,
        section: Dict[str, Any],
        outline: List[Dict[str, Any]],
        index: int,
        state: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Pre-writing taxonomy-axis planner (enrichment-only, no heading changes).

        Tracks which editorial axes have been used by previous H2 sections and
        returns a planning dict with:
          - taxonomy_axis: resolved axis for this section
          - forbidden_taxonomy_axis: axis to avoid when conflict is detected
          - preferred_axis: recommended alternative axis
          - h3_rewrite_needed: True ONLY when overlap is confirmed and obvious
          - h3_corrected_subheadings: replacement H3 list (only when h3_rewrite_needed)

        Core rule: if a prior H2 used ``category_or_type``, a pricing section
        must not reuse the same segmentation axis.  H2 headings are NEVER modified.
        H3s are only rewritten when confirmed identical-segmentation overlap exists
        (>=50 % of the current H3s mirror the segmentation of the prior section).
        """
        current_axis = self._infer_taxonomy_axis(section)

        # Collect axes used by all previous H2 sections
        used_axes: List[str] = []
        for prev_sec in outline[:index]:
            if str(prev_sec.get("heading_level", "")).upper() != "H2":
                continue
            prev_axis = (
                prev_sec.get("taxonomy_axis")
                or self._infer_taxonomy_axis(prev_sec)
            )
            used_axes.append(prev_axis)

        forbidden_axis = ""
        preferred_axis = current_axis
        h3_rewrite_needed = False
        h3_corrected_subheadings: Optional[List[str]] = None

        # Core rule: pricing section must not reuse category_or_type segmentation axis
        if current_axis == "pricing" and "category_or_type" in used_axes:
            forbidden_axis = "category_or_type"

            # Determine whether location signals justify a pricing_by_area axis
            area = str(state.get("area") or "").strip()
            area_neighborhoods = state.get("area_neighborhoods") or []
            has_location_signals = bool(area) or bool(area_neighborhoods)

            # Check if any prior section was location_area
            if not has_location_signals:
                for prev_sec in outline[:index]:
                    _pax = (
                        prev_sec.get("taxonomy_axis")
                        or self._infer_taxonomy_axis(prev_sec)
                    )
                    if _pax == "location_area":
                        has_location_signals = True
                        break

            # Check heading/subheading text for geographic terms
            if not has_location_signals:
                heading_blob = " ".join([
                    str(section.get("heading_text") or ""),
                    " ".join(
                        self._subheading_text(item)
                        for item in section.get("subheadings", []) or []
                    ),
                ]).lower()
                geo_terms = (
                    "\u0645\u0646\u0637\u0642\u0629", "\u062d\u064a ", "\u0634\u0645\u0627\u0644",
                    "\u062c\u0646\u0648\u0628", "\u0634\u0631\u0642", "\u063a\u0631\u0628",
                    "\u0648\u0633\u0637",
                    "north", "south", "east", "west", "center",
                    "area", "district", "region", "zone",
                )
                if any(t in heading_blob for t in geo_terms):
                    has_location_signals = True

            preferred_axis = "pricing_by_area" if has_location_signals else "pricing_by_type"

            # --- Detect confirmed H3 overlap ---
            # Only check the first matching category_or_type section
            _price_prefix_re = re.compile(
                r"^(\u0623\u0633\u0639\u0627\u0631|\u062a\u0643\u0644\u0641\u0629"
                r"|\u0633\u0639\u0631|price of|pricing of|cost of|prices? for)\s*",
                re.IGNORECASE,
            )
            for prev_sec in outline[:index]:
                if str(prev_sec.get("heading_level", "")).upper() != "H2":
                    continue
                _pax = (
                    prev_sec.get("taxonomy_axis")
                    or self._infer_taxonomy_axis(prev_sec)
                )
                if _pax != "category_or_type":
                    continue

                prev_subs = [
                    self._subheading_text(item).strip().lower()
                    for item in prev_sec.get("subheadings", []) or []
                    if self._subheading_text(item).strip()
                ]
                curr_subs = [
                    self._subheading_text(item).strip().lower()
                    for item in section.get("subheadings", []) or []
                    if self._subheading_text(item).strip()
                ]

                if not prev_subs or not curr_subs:
                    break  # Can't determine overlap without both H3 lists

                def _normalize_text(t: str) -> str:
                    # Remove Arabic definite article "ال" from start of words
                    t = re.sub(r"\b\u0627\u0644", "", t)
                    # Remove all whitespace for robust matching
                    return re.sub(r"\s+", "", t)

                overlap_count = 0
                for curr_sub in curr_subs:
                    bare = _price_prefix_re.sub("", curr_sub).strip()
                    if not bare:
                        continue
                    norm_bare = _normalize_text(bare)
                    for prev_sub in prev_subs:
                        norm_prev = _normalize_text(prev_sub)
                        if norm_bare in norm_prev or norm_prev in norm_bare or norm_bare == norm_prev:
                            overlap_count += 1
                            break

                # Confirmed when >= 50% of current H3s mirror the category section
                if overlap_count / len(curr_subs) >= 0.5:
                    h3_rewrite_needed = True
                    if has_location_signals and area:
                        is_arabic = bool(re.search(r"[\u0600-\u06FF]", area))
                        directions = (
                            ["\u0634\u0645\u0627\u0644", "\u062c\u0646\u0648\u0628",
                             "\u0634\u0631\u0642", "\u063a\u0631\u0628", "\u0648\u0633\u0637"]
                            if is_arabic
                            else ["north", "south", "east", "west", "center"]
                        )
                        heading_core = str(section.get("heading_text") or "").strip()
                        # Strip existing price prefix from heading_core to avoid "أسعار أسعار..."
                        heading_core = _price_prefix_re.sub("", heading_core).strip()
                        
                        h3_corrected_subheadings = [
                            f"\u0623\u0633\u0639\u0627\u0631 {heading_core} \u0641\u064a {d} {area}".strip()
                            if is_arabic
                            else f"prices for {heading_core} in {d} {area}"
                            for d in directions
                        ]
                break  # Only evaluate the first matching category section

        result: Dict[str, Any] = {
            "taxonomy_axis": current_axis,
            "forbidden_taxonomy_axis": forbidden_axis,
            "preferred_axis": preferred_axis,
            "h3_rewrite_needed": h3_rewrite_needed,
        }
        if h3_corrected_subheadings is not None:
            result["h3_corrected_subheadings"] = h3_corrected_subheadings
        return result

    def _enrich_section_contract(
        self,
        section: Dict[str, Any],
        outline: List[Dict[str, Any]],
        index: int,
        state: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Fill missing editorial instructions before section writing without changing headings."""
        contract = section.get("section_contract") or self._build_section_contract(section, outline, index, state)
        section["section_contract"] = contract

        heading = str(section.get("heading_text") or state.get("primary_keyword") or "this section").strip()
        taxonomy_axis = section.get("taxonomy_axis") or contract.get("taxonomy_axis") or self._infer_taxonomy_axis(section)

        # --- Taxonomy-Axis Planning (pre-writing enrichment; never changes H2 headings) ---
        _axis_plan = self._plan_taxonomy_axis(section, outline, index, state)
        taxonomy_axis = _axis_plan.get("taxonomy_axis", taxonomy_axis)
        _planned_forbidden = _axis_plan.get("forbidden_taxonomy_axis", "")
        _planned_preferred = _axis_plan.get("preferred_axis", taxonomy_axis)

        # Apply controlled H3 correction only when overlap is confirmed and obvious
        if _axis_plan.get("h3_rewrite_needed") and _axis_plan.get("h3_corrected_subheadings"):
            _old_subs = list(section.get("subheadings") or [])
            section["subheadings"] = _axis_plan["h3_corrected_subheadings"]
            logger.info(
                "[TaxonomyPlanner] Confirmed H3 overlap in '%s'. "
                "Rewrote H3s to '%s' axis. Old: %s \u2192 New: %s",
                heading,
                _planned_preferred,
                _old_subs[:3],
                section["subheadings"][:3],
            )

        preferred_axis = (
            section.get("preferred_axis")
            or contract.get("preferred_axis")
            or _planned_preferred
        )
        observed_mentions = list(dict.fromkeys(
            (section.get("observed_data_mentions") or contract.get("observed_data_mentions") or [])
            + self._collect_observed_data_mentions(section, state)
        ))[:6]

        defaults = {
            "section_promise": self._enrichment_text(
                state,
                f"تقديم إجابة واضحة ومباشرة عن: {heading}",
                f"Give a clear, direct answer to: {heading}",
            ),
            "reader_takeaway": self._enrichment_text(
                state,
                f"يفهم القارئ أهم ما يجب معرفته عن {heading} دون تكرار أو تعميم.",
                f"The reader understands the key practical point about {heading} without repetition or generic filler.",
            ),
            "depth_goal": self._enrichment_text(
                state,
                f"حوّل {heading} إلى فهم عملي يساعد القارئ على المقارنة أو الاختيار أو اتخاذ خطوة أوضح.",
                f"Turn {heading} into practical insight that helps the reader compare, choose, or take a clearer next step.",
            ),
            "practical_decision_value": self._enrichment_text(
                state,
                "يساعد هذا السكشن القارئ على تضييق الخيارات وفهم ما يستحق الانتباه قبل القرار.",
                "This section helps the reader narrow options and understand what matters before deciding.",
            ),
            "taxonomy_axis": taxonomy_axis,
            "preferred_axis": preferred_axis,
            "forbidden_taxonomy_axis": (
                section.get("forbidden_taxonomy_axis")
                or contract.get("forbidden_taxonomy_axis")
                or _planned_forbidden
            ),
            "observed_data_mentions": observed_mentions,
        }

        for key, value in defaults.items():
            if key == "observed_data_mentions":
                section[key] = value
            elif not section.get(key):
                section[key] = value
            if not contract.get(key):
                contract[key] = section.get(key, value)

        existing_details = section.get("must_include_details") or contract.get("must_include_details") or []
        if isinstance(existing_details, str):
            existing_details = [existing_details]
        
        # --- Topic Pack Enrichment (Dynamic) ---
        active_packs = self._detect_active_topic_packs(state)
        pack_details = []
        for pack in active_packs:
            pack_details.extend(self._topic_pack_details(pack, taxonomy_axis, state))

        detail_items = list(dict.fromkeys([
            str(item).strip()
            for item in list(existing_details) + self._section_contract_details(taxonomy_axis, state) + pack_details
            if str(item).strip()
        ]))
        section["must_include_details"] = detail_items[:8]
        contract["must_include_details"] = section["must_include_details"]
        return section

    async def _step_load_approved_outline(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Load an approved heading outline and prepare it for content writing only."""
        approved_title, outline = self._parse_approved_outline_payload(
            state.get("approved_outline") or state.get("input_data", {}).get("approved_outline")
        )
        if approved_title:
            state["input_data"]["title"] = approved_title

        state = self._prepare_outline_for_content(state, outline, source="approved_outline")
        if state.get("workflow_logger"):
            state["workflow_logger"].log_event("approved_outline_load", {
                "sections": len(state.get("outline", [])),
                "content_only_mode": True,
            })
        return state

    def _prepare_outline_for_content(
        self,
        state: Dict[str, Any],
        outline: List[Dict[str, Any]],
        source: str = "generated_outline",
    ) -> Dict[str, Any]:
        """Attach writing metadata to an approved/generated outline without changing headings."""
        input_data = state.get("input_data", {})
        primary_keyword = state.get("primary_keyword") or (state.get("keywords") or [input_data.get("title", "")])[0]
        article_language = state.get("article_language") or input_data.get("article_language", "ar")

        content_type = state.get("content_type", "informational")
        content_strategy = state.get("content_strategy", {})
        area = state.get("area")
        keywords = state.get("keywords") or input_data.get("keywords") or [primary_keyword]
        seo_intelligence = state.get("seo_intelligence", {})

        # Collect observed pricing signals for injection
        market_analysis = seo_intelligence.get("market_analysis", {})
        market_insights = market_analysis.get("market_insights", {})
        market_data_signals = market_insights.get("market_data_signals", {})
        observed_price_mentions = market_data_signals.get("observed_price_mentions", [])


        safe_outline: List[Dict[str, Any]] = []
        for idx, raw_section in enumerate(outline):
            section = dict(raw_section)
            section["subheadings"] = [
                text for text in (self._subheading_text(item) for item in section.get("subheadings", []) or [])
                if text
            ]
            self.outline_gen._normalize_section(section, idx, content_type, content_strategy, area)

            # --- Pricing Enrichment (Grounded Guidance) ---
            # If this is a pricing section, inject the observed mentions harvested from SERP.
            tax_axis = str(section.get("taxonomy_axis", "")).lower()
            if tax_axis.startswith("pricing") and observed_price_mentions:
                existing_mentions = section.get("observed_data_mentions", [])
                section["observed_data_mentions"] = list(dict.fromkeys(
                    [str(m).strip() for m in existing_mentions + observed_price_mentions if str(m).strip()]
                ))

            section["primary_keyword"] = primary_keyword
            section["article_language"] = article_language
            section.setdefault("assigned_keywords", keywords[:3] if keywords else [primary_keyword])
            safe_outline.append(section)

        # Assign requires_table based on priority, respecting the 2-table cap
        tables_assigned = 0
        
        # Priority 1: Comparison sections
        for section in safe_outline:
            if tables_assigned >= 2:
                break
            tax_axis = self._infer_taxonomy_axis(section)
            sec_type = (section.get("section_type") or "").lower()
            if sec_type == "comparison" or tax_axis == "comparison":
                section["requires_table"] = True
                tables_assigned += 1
                logger.info(f"[TableAssigner] Assigned table to comparison section: {section.get('heading_text')}")
                
        # Priority 2: Pricing/Proof sections
        for section in safe_outline:
            if tables_assigned >= 2:
                break
            if section.get("requires_table"):
                continue
            tax_axis = self._infer_taxonomy_axis(section)
            sec_type = (section.get("section_type") or "").lower()
            if sec_type in {"pricing", "proof"} or tax_axis == "pricing":
                if sec_type not in {"introduction", "conclusion", "faq"}:
                    section["requires_table"] = True
                    tables_assigned += 1
                    logger.info(f"[TableAssigner] Assigned table to pricing section: {section.get('heading_text')}")
                    
        # Priority 3: Explicit visual_format == "table"
        for section in safe_outline:
            if tables_assigned >= 2:
                break
            if section.get("requires_table"):
                continue
            sec_type = (section.get("section_type") or "").lower()
            if section.get("visual_format") == "table":
                if sec_type not in {"introduction", "conclusion", "faq"}:
                    section["requires_table"] = True
                    tables_assigned += 1
                    logger.info(f"[TableAssigner] Assigned table based on visual_format to section: {section.get('heading_text')}")

        # Now, build and enrich the section contracts with the correct requires_table value already present
        for idx, section in enumerate(safe_outline):
            section["section_contract"] = self._build_section_contract(section, safe_outline, idx, state)
            self._enrich_section_contract(section, safe_outline, idx, state)
            section["must_not_repeat"] = list(dict.fromkeys(
                (section.get("must_not_repeat") or []) + section["section_contract"]["must_not_repeat"]
            ))
            if section["section_contract"]["format"] == "bullets":
                section["requires_list"] = True

        semantic_assets = (
            seo_intelligence.get("market_analysis", {})
            .get("semantic_assets", {})
        )
        serp_data = state.get("serp_data", {}) if isinstance(state.get("serp_data", {}), dict) else {}
        state["global_keywords"] = {
            "primary": primary_keyword,
            "lsi": list(dict.fromkeys(
                (semantic_assets.get("lsi_keywords", []) or [])
                + (serp_data.get("lsi_keywords", []) or [])
                + state.get("secondary_keywords", [])[:5]
            ))[:12],
            "semantic": list(dict.fromkeys(
                (semantic_assets.get("related_searches", []) or [])
                + (semantic_assets.get("autocomplete_suggestions", []) or [])
            ))[:12],
        }

        user_urls = input_data.get("urls", []) or []
        internal_links = [u.get("link") for u in user_urls if isinstance(u, dict) and u.get("link")]
        state["internal_url_set"] = {LinkManager.canon_url(url) for url in internal_links if url}

        reference_links = serp_data.get("reference_authority_links", []) if isinstance(serp_data, dict) else []
        external_refs = []
        authority_domains = set()
        for item in reference_links:
            url = item.get("url") if isinstance(item, dict) else item
            if url:
                external_refs.append(LinkManager.canon_url(url))
                dom = LinkManager.domain(url)
                if dom:
                    authority_domains.add(dom)
        state["authority_domains"] = authority_domains

        brand_url = state.get("brand_url", "")
        state["blocked_external_domains"] = LinkManager.extract_competitor_domains(serp_data, brand_url)
        state["prohibited_competitors"] = [
            domain.split(".")[0].capitalize()
            for domain in state.get("blocked_external_domains", set())
            if domain and len(domain.split(".")[0]) > 1
        ]

        state["available_links_pool"] = {
            "internal": list(dict.fromkeys(internal_links))[:15],
            "external_references": list(dict.fromkeys(external_refs))[:10],
        }
        state["link_strategy"] = {
            "internal_topics": [
                {"text": item.get("text", "Internal Resource"), "link": item.get("link"), "type": "internal"}
                for item in user_urls if isinstance(item, dict) and item.get("link")
            ],
            "affiliate_policy": {"max_per_section": 3, "placement": "distributed", "tone": "neutral"},
        }

        state["outline"] = safe_outline
        state["approved_outline_source"] = source
        logger.info("Prepared %s sections for content writing from %s.", len(safe_outline), source)
        return state

    def _build_previous_sections_summary(self, state: Dict[str, Any]) -> str:
        sections = list((state.get("sections") or {}).values())
        sections.sort(key=lambda item: item.get("section_index", 0))

        lines = []
        for item in sections:
            heading = str(item.get("heading_text") or item.get("section_id") or "Previous section").strip()
            units = item.get("knowledge_units_established") or item.get("topics_covered") or []
            if units:
                unit_text = "; ".join(str(unit).strip() for unit in units[:3] if str(unit).strip())
            else:
                unit_text = "covered without reusable details"
            lines.append(f"- {heading}: {unit_text}")

        summary = "\n".join(lines)
        return summary[-1200:]

    def _enforce_section_heading_lock(self, content: str, section: Dict[str, Any]) -> str:
        """Keep body content under the approved outline headings only."""
        if not content:
            return content

        approved_h3 = {
            re.sub(r"\s+", " ", self._subheading_text(item)).strip().lower()
            for item in section.get("subheadings", []) or []
            if self._subheading_text(item)
        }
        kept = []
        removed = []
        for line in content.splitlines():
            stripped = line.strip()
            if re.match(r"^#{1,2}\s+", stripped):
                removed.append(stripped)
                continue
            if re.match(r"^#{3,6}\s+", stripped):
                heading_text = re.sub(r"^#{3,6}\s+", "", stripped).strip()
                normalized = re.sub(r"\s+", " ", heading_text).lower()
                if approved_h3 and normalized in approved_h3:
                    kept.append(f"### {heading_text}")
                else:
                    removed.append(stripped)
                continue
            kept.append(line)

        if removed:
            logger.info(
                "[SectionWriter] Removed non-approved heading lines from section '%s': %s",
                section.get("heading_text", ""),
                removed[:5],
            )
        return "\n".join(kept).strip()

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

        for idx, section in enumerate(outline):
            section["subheadings"] = [
                text for text in (self._subheading_text(item) for item in section.get("subheadings", []) or [])
                if text
            ]
            if not section.get("section_contract"):
                section["section_contract"] = self._build_section_contract(section, outline, idx, state)
            self._enrich_section_contract(section, outline, idx, state)
            section["must_not_repeat"] = list(dict.fromkeys(
                (section.get("must_not_repeat") or []) + section["section_contract"].get("must_not_repeat", [])
            ))


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

            # Track CTAs using has_cta helper
            def has_cta_local(text):
                return bool(re.search(r'<a\b|<button\b|\[.*?\]\(https?://', text))

            content = res.get("generated_content", "")
            if has_cta_local(content):
                 state["ctas_placed"] = state.get("ctas_placed", 0) + 1

            # Track Tables (Max 2 rule)
            table_count = len(re.findall(r"(?m)^\s*\|.*\|\s*$\n^\s*\|[\s:\-|]+\|\s*$", content))
            if table_count:
                 state["tables_placed"] = state.get("tables_placed", 0) + table_count

            # Update global brand mention count
            state["brand_mentions_count"] = state.get("brand_mentions_count", 0) + res.get("brand_mentions_count", 0)

            # Update global keyword count
            primary_keyword = global_keywords.get("primary", "")
            if primary_keyword:
                full_text_for_search = (res.get("heading_text") or "") + "\n" + content
                if any(ord(c) > 127 for c in primary_keyword):
                    pattern = r'(?:[وبلفك]|ال)*{}(?:[ةاتونينههمناي])*'.format(re.escape(primary_keyword.lower()))
                else:
                    pattern = r'\b{}\b'.format(re.escape(primary_keyword.lower()))
                matches = re.findall(pattern, full_text_for_search.lower())
                state["global_keyword_count"] = state.get("global_keyword_count", 0) + len(matches)

            # ONLY update full_content_so_far if it wasn't already updated (Parallel mode)
            if use_parallel:
                state["full_content_so_far"] = state.get("full_content_so_far", "") + "\n\n" + content

        state["sections"] = sections_content

        # Local SEO Enforcement (Retry first section if area is missing)
        area = state.get("area")
        if area and sections_content:
            first_id = outline[0]["section_id"]
            first_res = sections_content.get(first_id)

            if first_res and area.lower() not in (first_res.get("generated_content") or "").lower():
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
        location_policy = (section.get("section_contract") or {}).get("location_policy", "neutral")
        area_for_section = state.get("area") if location_policy != "neutral" else ""

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

        # Context windowing: send a short memory summary, not full previous text.
        optimized_context = self._build_previous_sections_summary(state)

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
            area=area_for_section,
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
            previous_section_text="",
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
            area=area_for_section,
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
            previous_section_text="",
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

        content = self._enforce_section_heading_lock(res_data.get("content", ""), section)
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
            is_valid = True
            validation_errors = []
            if state.get("content_stage_only_mode"):
                logger.info(
                    "Content Stage Only Mode: skipping per-section validation/repair for '%s'.",
                    section.get("heading_text", "")
                )
            else:
                try:
                    is_valid, validation_errors = await self.validator.validate_section_output(
                        content=final_content,
                        section=section,
                        state=state
                    )
                except Exception as e:
                    logger.error(f"Validation or Repair loop failed: {e}")

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
                        area=area_for_section,
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
                        previous_section_text="",
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
                        area=area_for_section,
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
                        previous_section_text="",
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
                        new_content = self._enforce_section_heading_lock(new_content, section)
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
                "section_index": section_index,
                "generated_content": final_content,
                "used_links": found_links,
                "brand_link_used": state.get("brand_link_used", False),
                "brand_mentions_count": mentions_in_section,
                "knowledge_units_established": res_data.get("knowledge_units_established", []),
                "topics_covered": res_data.get("topics_covered", []),
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

        title = state.get("input_data", {}).get("title", "Untitled")
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

        # Final signature check
        import inspect
        sig = inspect.signature(self.section_writer.write)
        if "content_type" not in sig.parameters:
             raise PipelineContractError("SectionWriter.write missing content_type")

        logger.info("Pipeline Preflight System Audit: PASS (Structural & Argument Integrity Verified)")

    # ---------------- UTILITIES ---

    def _build_execution_plan(self, section: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        """Constructs the per-section execution plan with CTA rules and writing constraints."""
        content_type = state.get("content_type", "informational")
        section_type = (section.get("section_type") or "").lower()
        location_policy = (section.get("section_contract") or {}).get("location_policy", "neutral")

        # Base plan
        plan = {
            "writing_mode": "standard",
            "cta_type": section.get("cta_type", "none"),
            "cta_position": section.get("cta_position", "none"),
            "structure_rule": "EXACTLY 2-3 PARAGRAPHS. 2-3 SENTENCES PER PARAGRAPH.",
            "local_context_required": location_policy == "local_required",
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

    def _build_content_stage_markdown(self, state: Dict[str, Any], title: str) -> str:
        """Assemble a review draft directly from approved headings and generated section bodies."""
        outline = state.get("outline", []) or []
        sections_dict = state.get("sections", {}) or {}

        parts = [f"# {title}"]
        for outline_section in outline:
            section_id = outline_section.get("section_id")
            generated_section = sections_dict.get(section_id, {}) if section_id else {}
            section = {**outline_section, **generated_section}

            content = self._enforce_section_heading_lock(
                str(section.get("generated_content", "") or "").strip(),
                outline_section,
            )

            section_type = (outline_section.get("section_type") or "").lower()
            heading = str(outline_section.get("heading_text") or "").strip()
            heading_level = str(outline_section.get("heading_level") or "H2").upper()

            if section_type != "introduction" and heading:
                level_num = 2
                if heading_level.startswith("H"):
                    try:
                        level_num = int(heading_level.replace("H", ""))
                    except ValueError:
                        level_num = 2
                level_num = max(2, min(level_num, 6))
                parts.append(f"{'#' * level_num} {heading}")

            if section_id:
                parts.append(f"<!-- section_id: {section_id} -->")

            if content:
                parts.append(content)

        final_markdown = "\n\n".join(part for part in parts if part).strip()

        output_dir = state.get("output_dir", self.work_dir)
        os.makedirs(output_dir, exist_ok=True)
        for filename in ("article_content_draft.md", "article_final.md"):
            with open(os.path.join(output_dir, filename), "w", encoding="utf-8") as f:
                f.write(final_markdown)

        state["final_output"] = {"final_markdown": final_markdown}
        return final_markdown

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

        # For commercial articles, inject the strongest known display brand into title/meta.
        # Domain-derived names are a last resort only; never override discovered/input brands.
        if content_type == "brand_commercial":
            brand_name = (
                state.get("display_brand_name")
                or state.get("brand_name")
                or state.get("official_brand_name")
                or ""
            )
            if not brand_name:
                brand_url = state.get("brand_url", "")
                if brand_url:
                    domain = LinkManager.domain(brand_url)  # e.g., "cems-it.com"
                    domain_brand = domain.split(".")[0] if domain else ""
                    brand_name = domain_brand.replace("-", " ").replace("_", " ").title()

            if brand_name:
                if brand_name.lower() not in raw_title.lower():
                    raw_title = f"{raw_title} | {brand_name}"

                if meta_title and brand_name.lower() not in meta_title.lower():
                    candidate = f"{meta_title} | {brand_name}"
                    if len(candidate) <= 65:
                        meta_title = candidate

        if state.get("heading_only_mode"):
            outline = state.get("outline", [])
            outline = self.outline_repair_service.enrich_brand_utility_faq(
                outline,
                serp_brief=state.get("serp_outline_brief", {}),
                brand_context=state.get("display_brand_name", "") or state.get("brand_name", ""),
                content_type=state.get("content_type", ""),
                entity_phrase=state.get("entity_phrase", "") or state.get("primary_keyword", ""),
            )
            outline = self.outline_repair_service.normalize_heading_only_section_types(outline)
            state["outline"] = outline
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
                        sub_text = sub.get("heading_text", "") if isinstance(sub, dict) else str(sub)
                        preview_lines.append(f"### {sub_text}")

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

        if state.get("content_stage_only_mode"):
            final_markdown = self._build_content_stage_markdown(state, raw_title)
            outline_map = []
            for sec in state.get("outline", []) or []:
                outline_map.append({
                    "section_id": sec.get("section_id"),
                    "heading_text": sec.get("heading_text"),
                    "heading_level": sec.get("heading_level"),
                    "section_type": sec.get("section_type"),
                    "section_intent": sec.get("section_intent"),
                    "subheadings": sec.get("subheadings", []),
                    "section_contract": sec.get("section_contract", {}),
                })

            return {
                "title": raw_title,
                "slug": state.get("slug", "unknown"),
                "primary_keyword": state.get("primary_keyword", ""),
                "content_stage_only_mode": True,
                "content_only_mode": state.get("content_only_mode", False),
                "heading_only_mode": False,
                "final_markdown": final_markdown,
                "outline_structure": outline_map,
                "status": "success",
                "message": "Content draft generated successfully for review.",
                "performance": performance,
                "output_dir": state.get("output_dir", ""),
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
            if has_commercial:
                roles["introduction"] = (
                    f"Open with concise buyer context for {primary_keyword} and clarify the search need "
                    "without sales urgency or generic market hooks."
                )
            else:
                roles["introduction"] = (
                    f"Open with a helpful hook and visitor/reader context for {primary_keyword}. "
                    "Do not define the topic here; keep the definition for the first visible H2."
                )

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
