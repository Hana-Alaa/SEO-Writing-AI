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
from jinja2 import Environment, Template, StrictUndefined
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
from src.services.brand_evidence_service import BrandEvidenceService, build_brand_offer_contract
from src.utils.contract_safety import PipelineContractError, validate_service_call, is_signature_mismatch
from src.config.paths import (
    create_prompt_template_loader,
    prompt_template_path,
    read_prompt_template,
)


# Custom errors
class StructureError(Exception):
    pass

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO, format="%(message)s")

DetectorFactory.seed = 0
PARALLEL_SECTIONS = False

_PRICING_ARABIC_INDIC_TRANSLATION = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_PRICING_CONTEXT_WINDOW = 45
_PRICING_CURRENCY_TOKEN_RE = re.compile(
    r"(?:"
    r"ريال|ر\.س|جنيه|ج\.م|درهم|د\.إ|دينار|د\.ك|د\.ب|دولار|يورو|باوند|ليرة|"
    r"sar|saudi\s+riyals?|egp|aed|usd|eur|gbp|kwd|bhd|qar|omr|"
    r"dollars?|euros?|pounds?|dirhams?|dinars?|riyals?|"
    r"[\$€£]"
    r")",
    re.IGNORECASE,
)
_PRICING_PHRASE_RE = re.compile(
    r"(?:"
    r"سعر|الاسعار|الأسعار|تكلفة|يبدأ\s+من|تبدأ\s+من|"
    r"prices?|pricing|cost|starts?\s+from|starting\s+from|\bfrom\b"
    r")",
    re.IGNORECASE,
)
_PRICING_LISTING_MARKERS_RE = re.compile(
    r"(?:"
    r"شقق|عقارات|نتائج|مشاريع|عملاء|سنوات|"
    r"pages?|listings?|results?|projects?|clients?"
    r")",
    re.IGNORECASE,
)
_PRICING_DISCOUNT_ONLY_RE = re.compile(
    r"(?:%|٪|خصم|off\b|discount)",
    re.IGNORECASE,
)
_PRICING_ASCII_NUMBER = r"(?:\d{1,3}(?:[,\s]\d{3})+(?:\.\d+)?|\d{3,}(?:\.\d+)?)"
_PRICING_SCALED_ASCII_NUMBER = r"\d+(?:\.\d+)?\s*[KkMm]"
_PRICING_ARABIC_SCALE_AMOUNT = r"\d+(?:[.,]\d+)?\s+(?:مليون|ألف|الف|الاف)"
_PRICING_ARABIC_INDIC_NUMBER = r"(?:[٠-٩]{1,3}(?:[٬،,\s][٠-٩]{3})+(?:[.,][٠-٩]+)?|[٠-٩]{3,}(?:[.,][٠-٩]+)?)"
_PRICING_NUMBER = rf"(?:{_PRICING_ASCII_NUMBER}|{_PRICING_ARABIC_INDIC_NUMBER}|{_PRICING_SCALED_ASCII_NUMBER})"
_PRICING_SCALE = r"(?:\s*[KkMm]|\s+(?:مليون|ألف|الف|الاف))"
_PRICING_CURRENCY_GROUP = (
    r"(?:ريال|ر\.س|جنيه|ج\.م|درهم|د\.إ|دينار|د\.ك|د\.ب|دولار|يورو|باوند|ليرة|"
    r"SAR|EGP|AED|USD|EUR|GBP|KWD|BHD|QAR|OMR|"
    r"dollars?|euros?|pounds?|dirhams?|dinars?|riyals?)"
)
_PRICING_CANDIDATE_PATTERNS = [
    re.compile(rf"[\$€£]\s*{_PRICING_ASCII_NUMBER}{_PRICING_SCALE}?", re.IGNORECASE),
    re.compile(
        rf"(?:{_PRICING_CURRENCY_GROUP})\s*{_PRICING_NUMBER}{_PRICING_SCALE}?",
        re.IGNORECASE,
    ),
    re.compile(
        rf"{_PRICING_NUMBER}{_PRICING_SCALE}?\s*(?:{_PRICING_CURRENCY_GROUP})",
        re.IGNORECASE,
    ),
    re.compile(
        rf"{_PRICING_ARABIC_SCALE_AMOUNT}\s*(?:{_PRICING_CURRENCY_GROUP})?",
        re.IGNORECASE,
    ),
    re.compile(
        rf"{_PRICING_NUMBER}{_PRICING_SCALE}\s+(?:{_PRICING_CURRENCY_GROUP})",
        re.IGNORECASE,
    ),
    re.compile(
        rf"(?:{_PRICING_PHRASE_RE.pattern})\s*:?\s*{_PRICING_NUMBER}{_PRICING_SCALE}?",
        re.IGNORECASE,
    ),
    re.compile(
        rf"{_PRICING_SCALED_ASCII_NUMBER}\s+(?:{_PRICING_CURRENCY_GROUP})",
        re.IGNORECASE,
    ),
    re.compile(
        rf"{_PRICING_NUMBER}{_PRICING_SCALE}?",
        re.IGNORECASE,
    ),
]


def _normalize_pricing_context(text: str) -> str:
    normalized = (
        str(text or "")
        .lower()
        .replace("إ", "ا")
        .replace("أ", "ا")
        .replace("آ", "ا")
        .replace("ى", "ي")
        .replace("ة", "ه")
    )
    return normalized.translate(_PRICING_ARABIC_INDIC_TRANSLATION)


def _pricing_digits_only(value: str) -> str:
    return re.sub(r"\D", "", _normalize_pricing_context(value))


def _pricing_amount_strength(value: str) -> int:
    digits = _pricing_digits_only(value)
    if not digits:
        return 0
    if re.search(r"[KkMm]|مليون|الف|ألف|الاف", value, re.IGNORECASE):
        return max(len(digits), 4)
    return len(digits)


def _pricing_local_window(blob: str, start: int, end: int, pad: int = _PRICING_CONTEXT_WINDOW) -> str:
    return blob[max(0, start - pad): min(len(blob), end + pad)]


def _pricing_has_currency_token(text: str) -> bool:
    return bool(_PRICING_CURRENCY_TOKEN_RE.search(_normalize_pricing_context(text)))


def _pricing_has_currency_or_phrase(text: str) -> bool:
    normalized = _normalize_pricing_context(text)
    return bool(_PRICING_CURRENCY_TOKEN_RE.search(normalized) or _PRICING_PHRASE_RE.search(normalized))


def _pricing_is_guide_year(match_text: str, local_window: str) -> bool:
    digits = _pricing_digits_only(match_text)
    if not re.fullmatch(r"\d{4}", digits):
        return False
    year = int(digits)
    if not 1900 <= year <= 2099:
        return False
    return not _pricing_has_currency_token(local_window)


def _pricing_is_listing_count_context(local_window: str, match_text: str) -> bool:
    normalized = _normalize_pricing_context(local_window)
    if _pricing_has_currency_token(local_window):
        return False
    match_norm = _normalize_pricing_context(match_text)
    if not match_norm:
        return False
    start = normalized.find(match_norm)
    if start == -1:
        start = 0
    end = start + len(match_norm)
    tight_after = normalized[end:end + 18]
    tight_before = normalized[max(0, start - 18):start]
    tight = f"{tight_before} {match_norm} {tight_after}"
    if not _PRICING_LISTING_MARKERS_RE.search(tight):
        return False
    return not _pricing_has_currency_or_phrase(tight)


def _pricing_is_discount_only(local_window: str, match_text: str) -> bool:
    if not _PRICING_DISCOUNT_ONLY_RE.search(local_window):
        return False
    if _pricing_has_currency_token(local_window):
        return False
    return _pricing_amount_strength(match_text) < 3


def _iter_observed_price_candidates(blob: str) -> List[tuple[int, int, str]]:
    seen: set[tuple[int, int]] = set()
    candidates: List[tuple[int, int, str]] = []
    for pattern in _PRICING_CANDIDATE_PATTERNS:
        for match in pattern.finditer(blob):
            start, end = match.start(), match.end()
            key = (start, end)
            if key in seen:
                continue
            seen.add(key)
            candidates.append((start, end, match.group(0).strip()))
    candidates.sort(key=lambda item: item[0])
    return candidates


def _is_valid_observed_price_mention(blob: str, start: int, end: int, matched: str) -> bool:
    if _pricing_amount_strength(matched) < 2 and not _pricing_has_currency_token(matched):
        return False
    local_window = _pricing_local_window(blob, start, end)
    if not _pricing_has_currency_or_phrase(local_window):
        return False
    if _pricing_is_guide_year(matched, local_window):
        return False
    if _pricing_is_listing_count_context(local_window, matched):
        return False
    if _pricing_is_discount_only(local_window, matched):
        return False
    return True


def _observed_price_context(blob: str, start: int, end: int) -> str:
    context = _pricing_local_window(blob, start, end, pad=35).strip()
    return " ".join(context.split())


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
            template_path=prompt_template_path("06_image_planner.txt"),
        )
        self.env = Environment(
            loader=create_prompt_template_loader(),
            undefined=StrictUndefined
        )

        self.intent_template = Template(
            read_prompt_template("00_intent_classifier.txt"),
            undefined=StrictUndefined,
        )

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
        self.brand_evidence_service = BrandEvidenceService()

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
            state["workflow_logger"].export_csv(state=state)
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
        state = await self.research_service.run_brand_discovery(state)
        
        # --- Additive Phase 1.5: Crawl, Build Map, Offer Contract & Guardrails ---
        from src.services.brand_evidence_service import (
            get_empty_brand_offer_contract,
            build_brand_offer_contract,
            build_brand_generation_guardrails,
            build_brand_evidence_boundaries,
        )
        
        # 1. Same-domain defensive crawl enrichment
        try:
            raw_max = state.get("brand_crawl_max_pages")
            max_pages = 10
            if raw_max is not None:
                try:
                    max_pages = int(raw_max)
                except (ValueError, TypeError):
                    max_pages = 10
            max_pages = max(1, min(max_pages, 15))
            state = await self.brand_evidence_service.enrich_brand_internal_resources(state, max_pages=max_pages)
        except Exception as e:
            logger.warning(f"Brand site evidence enrichment crawl failed: {e}. Fallback to existing resources.")

        # 2. Build Evidence Map and Offer Contract
        try:
            state = await self.brand_evidence_service.run_brand_evidence_map(state)
            contract = build_brand_offer_contract(state)
            state["brand_offer_contract"] = contract
        except Exception as e:
            logger.warning(f"Brand evidence mapping or contract generation failed: {e}. Populating fallback state.")
            # Minimal/empty safe map
            if "brand_evidence_map" not in state or not state["brand_evidence_map"]:
                state["brand_evidence_map"] = {
                    "strong_signals": [],
                    "medium_signals": [],
                    "weak_signals": [],
                    "strong_source_urls": [],
                    "source_counts": {"headings": 0, "cta_labels": 0, "anchors": 0, "urls": 0},
                    "missing_evidence": []
                }
            # Fallback contract with full schema
            contract = get_empty_brand_offer_contract(state)
            state["brand_offer_contract"] = contract

        # 3. Build Guardrails
        try:
            guardrails = build_brand_generation_guardrails(state)
            state["brand_generation_guardrails"] = guardrails
        except Exception as e:
            logger.warning(f"Brand guardrails generation failed: {e}. Populating fallback guardrails.")
            guardrails = {
                "brand_confidence": "low",
                "brand_usage_mode": "soft_context_only",
                "allowed_brand_claims": [],
                "allowed_conversion_actions": [],
                "forbidden_brand_claims": [
                    "delivery timelines", "response time guarantees", "payment gateway support",
                    "testimonials or client proof", "portfolio claims", "verified/award claims",
                    "local team claims", "custom/no-template process claims"
                ],
                "brand_section_policy": "do_not_create_dedicated_brand_proof_or_why_choose_sections"
            }
            state["brand_generation_guardrails"] = guardrails

        # 4. Separate brand_guardrail_context (Clearly marked, clean, non-mutating)
        summary = "\n[BRAND GENERATION GUARDRAILS - DO NOT TREAT AS BRAND DESCRIPTION]\n"
        summary += f"- Brand confidence: {guardrails.get('brand_confidence', 'low')}\n"
        summary += f"- Brand usage mode: {guardrails.get('brand_usage_mode', 'soft_context_only')}\n"
        summary += f"- Allowed brand claims: {', '.join(guardrails.get('allowed_brand_claims', []))}\n"
        summary += f"- Allowed conversion actions: {', '.join(guardrails.get('allowed_conversion_actions', []))}\n"
        summary += f"- Forbidden brand claims: {', '.join(guardrails.get('forbidden_brand_claims', []))}\n"
        summary += f"- Brand section policy: {guardrails.get('brand_section_policy', 'do_not_create_dedicated_brand_proof_or_why_choose_sections')}\n"
        
        state["brand_guardrail_context"] = summary

        # 4b. Store brand evidence cards and index
        try:
            from src.services.brand_evidence_service import build_brand_evidence_cards, build_brand_pages_index
            state["brand_evidence_cards"] = build_brand_evidence_cards(state)
            state["brand_pages_index"] = build_brand_pages_index(state)
        except Exception as e:
            logger.warning(f"Failed to build brand evidence cards or index: {e}")
            state["brand_evidence_cards"] = []
            state["brand_pages_index"] = {}

        # 4c. Store brand source chunks (Phase 1.7 Step 8)
        try:
            from src.services.brand_evidence_service import build_brand_source_chunks
            state["brand_source_chunks"] = build_brand_source_chunks(state)
            logger.info(f"[brand_source_chunks] compiled count={len(state.get('brand_source_chunks', []))}")
        except Exception as e:
            logger.warning(f"Failed to compile brand source chunks: {e}")
            state["brand_source_chunks"] = []

        # 4c.1. Store page-level grounded brand briefs (legacy diagnostics)
        try:
            from src.services.brand_evidence_service import build_brand_page_briefs, build_brand_page_narrative_briefs
            state["brand_page_briefs"] = build_brand_page_briefs(state)
            logger.info("[brand_page_briefs] compiled count=%s", len(state.get("brand_page_briefs", [])))
            state["brand_page_narrative_briefs"] = build_brand_page_narrative_briefs(state)
            logger.info("[brand_page_narrative_briefs] compiled count=%s", len(state.get("brand_page_narrative_briefs", [])))
            self._activate_brand_evidence_failure_mode_if_needed(state)
            self._persist_brand_page_knowledge_pack(state)
        except Exception as e:
            logger.warning(f"Failed to compile brand page briefs: {e}")
            state["brand_page_briefs"] = []
            state["brand_page_narrative_briefs"] = []
            self._activate_brand_evidence_failure_mode_if_needed(state)
            self._persist_brand_page_knowledge_pack(state)

        # 4d. Store brand evidence inventory (Phase 1.9 Step 4)
        try:
            from src.services.brand_evidence_service import build_brand_evidence_inventory
            state["brand_evidence_inventory"] = build_brand_evidence_inventory(state)
            logger.info(
                "[brand_evidence_inventory] services=%s projects=%s pricing=%s process=%s trust=%s",
                state["brand_evidence_inventory"].get("services_available"),
                state["brand_evidence_inventory"].get("projects_available"),
                state["brand_evidence_inventory"].get("pricing_available"),
                state["brand_evidence_inventory"].get("process_available"),
                state["brand_evidence_inventory"].get("trust_available"),
            )
        except Exception as e:
            logger.warning(f"Failed to build brand evidence inventory: {e}")
            state["brand_evidence_inventory"] = {
                "services_available": False,
                "projects_available": False,
                "pricing_available": False,
                "process_available": False,
                "trust_available": False,
                "explicit_geography": [],
                "service_page_urls": [],
                "project_page_urls": [],
                "pricing_page_urls": [],
                "process_page_urls": [],
                "trust_page_urls": [],
                "confidence": "low",
            }

        # 4e. Freeze source-qualified strategy boundaries after all page evidence
        # representations are available, then rebuild the contract/guardrails from
        # the same final evidence view.
        try:
            state["brand_evidence_boundaries"] = build_brand_evidence_boundaries(state)
            contract = build_brand_offer_contract(state)
            state["brand_offer_contract"] = contract
            guardrails = build_brand_generation_guardrails(state)
            state["brand_generation_guardrails"] = guardrails
            state["brand_guardrail_context"] = (
                "\n[BRAND GENERATION GUARDRAILS - DO NOT TREAT AS BRAND DESCRIPTION]\n"
                f"- Brand confidence: {guardrails.get('brand_confidence', 'low')}\n"
                f"- Brand usage mode: {guardrails.get('brand_usage_mode', 'soft_context_only')}\n"
                f"- Allowed brand claims: {', '.join(guardrails.get('allowed_brand_claims', []))}\n"
                f"- Allowed conversion actions: {', '.join(guardrails.get('allowed_conversion_actions', []))}\n"
                f"- Forbidden brand claims: {', '.join(guardrails.get('forbidden_brand_claims', []))}\n"
                f"- Brand section policy: {guardrails.get('brand_section_policy', 'do_not_create_dedicated_brand_proof_or_why_choose_sections')}\n"
            )
            boundaries = state["brand_evidence_boundaries"]
            logger.info(
                "[brand_evidence_boundaries] projects=%s testimonials=%s awards=%s "
                "certifications=%s partnerships=%s pricing=%s local_presence=%s",
                boundaries.get("projects"),
                boundaries.get("testimonials"),
                boundaries.get("awards"),
                boundaries.get("certifications"),
                boundaries.get("partnerships"),
                boundaries.get("brand_pricing"),
                boundaries.get("local_presence"),
            )
        except Exception as e:
            logger.warning("Failed to build final brand evidence boundaries: %s", e)
            state["brand_evidence_boundaries"] = {
                "services": False,
                "projects": False,
                "process": False,
                "testimonials": False,
                "awards": False,
                "certifications": False,
                "partnerships": False,
                "brand_pricing": False,
                "local_presence": False,
                "explicit_geography": [],
                "guarantees": False,
                "delivery_timelines": False,
                "evidence_sources": {},
            }
        self._activate_brand_evidence_failure_mode_if_needed(state)

        # 3b. Build Writing Brief & Brief Context (Phase 1.6)
        try:
            from src.services.brand_evidence_service import build_brand_writing_brief, format_brand_writing_brief_context
            state["brand_writing_brief"] = build_brand_writing_brief(state)
            state["brand_writing_brief_context"] = format_brand_writing_brief_context(state["brand_writing_brief"])
        except Exception as e:
            logger.warning(f"Brand writing brief generation failed: {e}.")
            state["brand_writing_brief"] = {}
            state["brand_writing_brief_context"] = ""
        self._activate_brand_evidence_failure_mode_if_needed(state)

        # Concise logging
        missing = contract.get("evidence_summary", {}).get("missing_evidence", [])
        logger.info(
            f"[brand_offer_contract] created=true "
            f"| confidence={contract['brand_identity'].get('confidence', 'low')} "
            f"| value_props_count={len(contract.get('value_propositions', []))} "
            f"| conversion_actions_count={len(contract.get('conversion_actions', []))} "
            f"| missing_evidence={missing[:3]}"
        )
        self._stamp_brand_evidence_snapshot(state, "initial_brand_discovery")
        
        return state

    def _is_usable_brand_narrative_brief(self, brief: Dict[str, Any]) -> bool:
        """A usable writer brief must contain observed page text, not only a link label."""
        if not isinstance(brief, dict):
            return False
        narrative = re.sub(r"\s+", " ", str(brief.get("narrative_brief") or "")).strip()
        if len(narrative) < 80:
            return False
        placeholder = (
            'This home page is titled "Homepage"' in narrative
            and "The page content says:" not in narrative
            and "does not provide explicit" in narrative
        )
        if placeholder:
            return False
        signals = brief.get("routing_signals") if isinstance(brief.get("routing_signals"), dict) else {}
        if any(signals.get(key) for key in ("services", "technologies", "projects", "process_steps", "explicit_geography")):
            return True
        if signals.get("has_pricing") or signals.get("has_trust"):
            return True
        return True

    def _has_usable_brand_page_evidence(self, state: Dict[str, Any]) -> bool:
        """Return True only when crawl produced enough page text for factual brand claims."""
        if not state.get("brand_url"):
            return True
        crawl_report = state.get("brand_crawl_report") or {}
        read_stats = crawl_report.get("page_read_stats") or []
        if any(int(stat.get("text_chars") or 0) >= 250 for stat in read_stats if isinstance(stat, dict)):
            return True
        briefs = state.get("brand_page_narrative_briefs") or []
        return any(self._is_usable_brand_narrative_brief(brief) for brief in briefs)

    def _activate_brand_evidence_failure_mode_if_needed(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        If crawling produced no usable page text, prevent link labels/placeholders
        from becoming writer-facing brand truth.
        """
        if not state.get("brand_url"):
            return state

        usable_briefs = [
            brief for brief in (state.get("brand_page_narrative_briefs") or [])
            if self._is_usable_brand_narrative_brief(brief)
        ]
        if usable_briefs:
            state["brand_page_narrative_briefs"] = usable_briefs
            state.pop("brand_evidence_failure_mode", None)
            return state

        state["brand_page_narrative_briefs"] = []
        state["brand_page_briefs"] = []
        state["brand_evidence_failure_mode"] = "no_usable_crawled_brand_pages"

        inventory = state.setdefault("brand_evidence_inventory", {})
        inventory.update(
            {
                "services_available": False,
                "projects_available": False,
                "pricing_available": False,
                "process_available": False,
                "trust_available": False,
                "explicit_geography": [],
                "confidence": "low",
            }
        )

        guardrails = state.setdefault("brand_generation_guardrails", {})
        guardrails.update(
            {
                "brand_confidence": "low",
                "brand_usage_mode": "no_usable_source_text",
                "allowed_brand_claims": [],
                "allowed_brand_capabilities": [],
                "allowed_conversion_actions": [],
                "brand_section_policy": "do_not_create_dedicated_brand_sections",
            }
        )

        writing_brief = state.get("brand_writing_brief")
        if isinstance(writing_brief, dict):
            writing_brief["evidence_confidence"] = "low"
            writing_brief["brand_usage_mode"] = "no_usable_source_text"
            writing_brief["brand_section_policy"] = "do_not_create_dedicated_brand_sections"
            writing_brief["allowed_services"] = []
            writing_brief["allowed_claims"] = []
            writing_brief["allowed_conversion_actions"] = []

        state["brand_guardrail_context"] = (
            "\n[BRAND GENERATION GUARDRAILS - DO NOT TREAT AS BRAND DESCRIPTION]\n"
            "- Brand confidence: low\n"
            "- Brand usage mode: no_usable_source_text\n"
            "- Allowed brand claims: None\n"
            "- Allowed conversion actions: None\n"
            "- Brand section policy: do_not_create_dedicated_brand_sections\n"
            "- The crawler did not collect usable brand page text. Do not create brand-owned service, process, proof, pricing, geography, or trust claims.\n"
        )

        logger.warning(
            "[brand_evidence_failure_mode] activated=no_usable_crawled_brand_pages brand_url=%s",
            state.get("brand_url"),
        )
        return state

    def _brand_ground_truth_catalog_lines(self, state: Dict[str, Any]) -> List[str]:
        from src.services.brand_evidence_service import format_brand_ground_truth_catalog_lines

        return format_brand_ground_truth_catalog_lines(state)

    def _record_ground_truth_consumption(self, state: Dict[str, Any], layer: str) -> Dict[str, Any]:
        """Step 3A-1: stamp + log that `layer` had the in-state ground truth available.

        Availability/logging only - this never changes prompts or decisions.
        """
        try:
            from src.services.brand_evidence_service import record_ground_truth_consumption

            record = record_ground_truth_consumption(state, layer)
            logger.info(
                "[ground_truth] %s_ground_truth_used=%s chars=%s",
                layer,
                str(record.get("used")).lower(),
                record.get("markdown_chars", 0),
            )
            return record
        except Exception as exc:
            logger.warning("[ground_truth] consumption record failed for %s: %s", layer, exc)
            return {"used": False, "markdown_chars": 0, "catalog_counts": {}}

    def _format_ground_truth_for_writer(self, state: Dict[str, Any], max_chars: int = 8000) -> str:
        """Return a clearly-delimited, length-bounded Brand Ground Truth block.

        Writer-only additive context (Step 3B parallel). Returns "" when no ground
        truth exists so the writer simply keeps the legacy knowledge pack.
        """
        ground_truth_md = state.get("brand_ground_truth")
        if not isinstance(ground_truth_md, str) or not ground_truth_md.strip():
            return ""
        body = ground_truth_md.strip()
        if len(body) > max_chars:
            body = body[:max_chars].rstrip() + "\n... [truncated]"
        return (
            "[BRAND GROUND TRUTH - SINGLE SOURCE OF TRUTH]\n"
            "Consolidated, page-traceable brand facts. Use ONLY facts shown here or in "
            "the page-by-page pack above. Do not invent services, projects, pricing, or "
            "locations that are not present.\n"
            f"{body}\n"
            "[END BRAND GROUND TRUTH]"
        )

    def _format_brand_page_knowledge_pack_for_prompt(self, state: Dict[str, Any], max_chars: Optional[int] = None) -> str:
        """Return the full cleaned page-by-page brand knowledge for every section prompt."""
        briefs = [
            brief for brief in (state.get("brand_page_narrative_briefs") or [])
            if self._is_usable_brand_narrative_brief(brief)
        ]
        lines: List[str] = []
        lines.append("[BRAND PAGE KNOWLEDGE PACK - PAGE BY PAGE]")
        lines.append("Use this as page-scoped context only. Do not invent facts not present here.")
        lines.extend(self._brand_ground_truth_catalog_lines(state))
        if state.get("brand_evidence_failure_mode"):
            lines.append(
                "BRAND EVIDENCE FAILURE MODE: no usable crawled brand page text was available. "
                "Do not describe brand services, processes, listings, pricing, geography, trust, guarantees, or local presence."
            )

        if not briefs:
            lines.append(
                "No usable crawled page narrative briefs were available. "
                "Keep brand mentions contextual and avoid dedicated brand-proof claims."
            )
        for idx, brief in enumerate(briefs, 1):
            narrative = re.sub(r"\s+", " ", str(brief.get("narrative_brief") or "")).strip()
            if not narrative:
                continue
            boundaries = brief.get("claim_boundaries") or []
            lines.extend(
                [
                     "",
                     f"## Page {idx}: {brief.get('page_title') or 'Brand page'}",
                     f"- URL: {brief.get('source_url') or brief.get('url') or ''}",
                     f"- Page type: {brief.get('page_type') or 'other'}",
                     "- What this page contains:",
                     narrative,
                ]
            )
            if boundaries:
                lines.append("- Claim boundaries:")
                lines.extend(f"  - {boundary}" for boundary in boundaries)

        context = "\n".join(lines).strip()
        if max_chars and len(context) > max_chars:
            context = context[:max_chars].rsplit("\n", 1)[0].strip()
            context += "\n\n[TRUNCATED: full page-by-page pack is saved in the output folder.]"
        return context

    def _persist_brand_page_knowledge_pack(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Save page narrative briefs to the current output directory and cache prompt context."""
        output_dir = state.get("output_dir") or self.work_dir
        try:
            os.makedirs(output_dir, exist_ok=True)
            pack_path = os.path.join(output_dir, "brand_page_knowledge_pack.md")
            brand_name = state.get("display_brand_name") or state.get("brand_name") or "Brand"
            crawl_report = state.get("brand_crawl_report") or {}
            briefs = [
                brief for brief in (state.get("brand_page_narrative_briefs") or [])
                if self._is_usable_brand_narrative_brief(brief)
            ]
            lines = [
                f"# Brand Page Knowledge Pack: {brand_name}",
                "",
                "This file is generated from crawled brand pages. It is page-scoped context for section writing.",
                "It should not be treated as permission to invent pricing, guarantees, timelines, local presence, testimonials, or project counts.",
                "",
                "## Crawl Summary",
                f"- Brand URL: {state.get('brand_url') or ''}",
                f"- Crawled URLs count: {len(crawl_report.get('crawled_urls') or [])}",
                f"- Usable page narrative briefs: {len(briefs)}",
            ]
            # 3E-2: surface the synced Brand Service Catalog (cards -> pack) so the
            # saved pack matches exactly what strategy/outline/writer prompts see.
            try:
                lines.extend(self._brand_ground_truth_catalog_lines(state))
            except Exception as catalog_error:
                logger.warning("[brand_service_catalog] failed to render in saved pack: %s", catalog_error)
            if not briefs:
                lines.extend(
                    [
                        "",
                        "## No Usable Page Briefs",
                        "The crawler did not collect enough page text to support strong brand claims.",
                        "Use the brand only as a light contextual reference unless another explicit source is provided.",
                    ]
                )
            for idx, brief in enumerate(briefs, 1):
                lines.extend(
                    [
                        "",
                        f"## Page {idx}: {brief.get('page_title') or 'Brand page'}",
                        f"- URL: {brief.get('source_url') or brief.get('url') or ''}",
                        f"- Page type: {brief.get('page_type') or 'other'}",
                        "",
                        "### What This Page Contains",
                        str(brief.get("narrative_brief") or "").strip(),
                    ]
                )
                boundaries = brief.get("claim_boundaries") or []
                if boundaries:
                    lines.extend(["", "### Claim Boundaries"])
                    lines.extend(f"- {boundary}" for boundary in boundaries)
            
            # Append crawl diagnostics strictly under "Diagnostics - Not Writer Context" at the bottom
            lines.extend(
                [
                    "",
                    "## Diagnostics - Not Writer Context",
                    "",
                    f"- Brand URL: {state.get('brand_url') or ''}",
                    f"- Crawled URLs count: {len(crawl_report.get('crawled_urls') or [])}",
                ]
            )
            crawled_urls = crawl_report.get("crawled_urls") or []
            if crawled_urls:
                lines.append("- Crawled URLs list:")
                for url in crawled_urls:
                    lines.append(f"  - {url}")
            if crawl_report.get("page_read_stats"):
                lines.append("- Page read stats:")
                for stat in crawl_report.get("page_read_stats", [])[:30]:
                    lines.append(
                        f"  - {stat.get('url')}: type={stat.get('page_type')}, "
                        f"text_chars={stat.get('text_chars', 0)}, sections={stat.get('semantic_sections_count', 0)}"
                    )
                    
            with open(pack_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines).strip() + "\n")
            state["brand_page_knowledge_pack_path"] = pack_path
            state["brand_page_knowledge_pack_context"] = self._format_brand_page_knowledge_pack_for_prompt(state)
            logger.info("[brand_page_knowledge_pack] saved=%s briefs=%s", pack_path, len(briefs))
        except Exception as e:
            logger.warning("[brand_page_knowledge_pack] failed to save: %s", e)
            state["brand_page_knowledge_pack_context"] = self._format_brand_page_knowledge_pack_for_prompt(state)
        # Step 1 of Brand Ground Truth consolidation: emit one evidence-rich,
        # page-by-page report. This is produced/saved only; it does not yet replace
        # what strategy/outline/writer/validator consume.
        self._persist_brand_ground_truth_report(state)
        return state

    def _persist_brand_ground_truth_report(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Save the consolidated single-source Brand Discovery report to the output dir."""
        output_dir = state.get("output_dir") or self.work_dir
        try:
            from src.services.brand_evidence_service import (
                build_brand_ground_truth_data,
                build_brand_ground_truth_report,
            )

            os.makedirs(output_dir, exist_ok=True)
            report_path = os.path.join(output_dir, "brand_ground_truth.md")
            report = build_brand_ground_truth_report(state)
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(report)
            state["brand_ground_truth_path"] = report_path
            # Step 3A-0: expose the single source of truth IN state (markdown for
            # prompts, structured data for code/validator). Built from the same
            # inputs as the report, so the two stay in sync. No layer consumes these
            # yet - this only makes them available for Step 3A-1.
            state["brand_ground_truth"] = report
            try:
                ground_truth_data = build_brand_ground_truth_data(state)
            except Exception as data_err:
                ground_truth_data = {}
                logger.warning("[brand_ground_truth_data] failed to build: %s", data_err)
            state["brand_ground_truth_data"] = ground_truth_data
            catalogs = ground_truth_data.get("catalogs") or {}
            logger.info(
                "[brand_ground_truth] saved=%s chars=%s state_keys=brand_ground_truth,brand_ground_truth_data "
                "pages=%s services=%s technologies=%s projects=%s pricing_offers=%s",
                report_path,
                len(report),
                ground_truth_data.get("pages_analyzed", 0),
                len(catalogs.get("services") or []),
                len(catalogs.get("technologies") or []),
                len(catalogs.get("projects") or []),
                len(catalogs.get("pricing_offers") or []),
            )
        except Exception as e:
            logger.warning("[brand_ground_truth] failed to save: %s", e)
        return state

    def _brand_evidence_source_fingerprint(self, state: Dict[str, Any]) -> str:
        """Hash the raw brand inputs that all derived evidence objects must reflect."""
        resources = []
        for item in state.get("internal_resources") or []:
            if not isinstance(item, dict):
                continue
            semantic_sections = []
            for section in item.get("semantic_sections") or []:
                if not isinstance(section, dict):
                    continue
                semantic_sections.append({
                    "heading": section.get("heading"),
                    "body_text": section.get("body_text"),
                    "url": section.get("url"),
                    "page_type": section.get("page_type"),
                })
            resources.append({
                "url": item.get("link") or item.get("url"),
                "title": item.get("title"),
                "page_type": item.get("page_type"),
                "page_text_full": item.get("page_text_full"),
                "page_text": item.get("page_text"),
                "text": item.get("text"),
                "semantic_sections": semantic_sections,
            })
        payload = {
            "brand_url": state.get("brand_url"),
            "brand_context": state.get("brand_context"),
            "resources": resources,
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()[:20]

    def _format_brand_guardrail_context(self, guardrails: Dict[str, Any]) -> str:
        """Render the compact diagnostic guardrail context from one current snapshot."""
        return (
            "\n[BRAND GENERATION GUARDRAILS - DO NOT TREAT AS BRAND DESCRIPTION]\n"
            f"- Brand confidence: {guardrails.get('brand_confidence', 'low')}\n"
            f"- Brand usage mode: {guardrails.get('brand_usage_mode', 'soft_context_only')}\n"
            f"- Allowed brand claims: {', '.join(guardrails.get('allowed_brand_claims', []))}\n"
            f"- Allowed conversion actions: {', '.join(guardrails.get('allowed_conversion_actions', []))}\n"
            f"- Forbidden brand claims: {', '.join(guardrails.get('forbidden_brand_claims', []))}\n"
            f"- Brand section policy: {guardrails.get('brand_section_policy', 'do_not_create_dedicated_brand_proof_or_why_choose_sections')}\n"
        )

    def _stamp_brand_evidence_snapshot(self, state: Dict[str, Any], reason: str) -> Dict[str, Any]:
        """Mark all derived evidence objects as belonging to the same raw-source revision."""
        fingerprint = self._brand_evidence_source_fingerprint(state)
        revision = int(state.get("brand_evidence_revision") or 0) + 1
        state["brand_evidence_revision"] = revision
        state["brand_evidence_source_fingerprint"] = fingerprint
        state["brand_evidence_derived_source_fingerprint"] = fingerprint
        for key in (
            "brand_page_knowledge_pack",
            "brand_evidence_inventory",
            "brand_evidence_boundaries",
            "brand_offer_contract",
            "brand_generation_guardrails",
            "brand_writing_brief",
        ):
            state[f"{key}_revision"] = revision
        history = list(state.get("brand_evidence_refresh_history") or [])
        history.append({
            "revision": revision,
            "reason": reason,
            "fingerprint": fingerprint,
            "resources": len(state.get("internal_resources") or []),
            "chunks": len(state.get("brand_source_chunks") or []),
            "narrative_briefs": len(state.get("brand_page_narrative_briefs") or []),
        })
        state["brand_evidence_refresh_history"] = history[-12:]
        return state

    async def _refresh_brand_derived_evidence_state(
        self,
        state: Dict[str, Any],
        *,
        reason: str,
        rebuild_evidence_map: bool = True,
    ) -> Dict[str, Any]:
        """Rebuild every derived brand object transactionally from current crawled pages."""
        from src.services.brand_evidence_service import (
            build_brand_evidence_boundaries,
            build_brand_evidence_cards,
            build_brand_evidence_inventory,
            build_brand_generation_guardrails,
            build_brand_offer_contract,
            build_brand_page_briefs,
            build_brand_page_narrative_briefs,
            build_brand_pages_index,
            build_brand_source_chunks,
            build_brand_writing_brief,
            format_brand_writing_brief_context,
        )

        working = dict(state)
        working["brand_evidence_refresh_history"] = list(
            state.get("brand_evidence_refresh_history") or []
        )
        if rebuild_evidence_map:
            working = await self.brand_evidence_service.run_brand_evidence_map(working)

        working["brand_evidence_cards"] = build_brand_evidence_cards(working)
        working["brand_pages_index"] = build_brand_pages_index(working)
        working["brand_source_chunks"] = build_brand_source_chunks(working)
        working["brand_page_briefs"] = build_brand_page_briefs(working)
        working["brand_page_narrative_briefs"] = build_brand_page_narrative_briefs(working)
        self._activate_brand_evidence_failure_mode_if_needed(working)
        self._persist_brand_page_knowledge_pack(working)
        working["brand_evidence_inventory"] = build_brand_evidence_inventory(working)
        working["brand_evidence_boundaries"] = build_brand_evidence_boundaries(working)
        working["brand_offer_contract"] = build_brand_offer_contract(working)
        working["brand_generation_guardrails"] = build_brand_generation_guardrails(working)
        working["brand_guardrail_context"] = self._format_brand_guardrail_context(
            working["brand_generation_guardrails"]
        )
        working["brand_writing_brief"] = build_brand_writing_brief(working)
        working["brand_writing_brief_context"] = format_brand_writing_brief_context(
            working["brand_writing_brief"]
        )
        self._activate_brand_evidence_failure_mode_if_needed(working)
        self._stamp_brand_evidence_snapshot(working, reason)

        if "brand_evidence_failure_mode" not in working:
            state.pop("brand_evidence_failure_mode", None)
        state.update(working)
        logger.info(
            "[brand_evidence_refresh] revision=%s reason=%s resources=%s chunks=%s "
            "narrative_briefs=%s projects=%s pricing=%s local_presence=%s",
            state.get("brand_evidence_revision"),
            reason,
            len(state.get("internal_resources") or []),
            len(state.get("brand_source_chunks") or []),
            len(state.get("brand_page_narrative_briefs") or []),
            state.get("brand_evidence_boundaries", {}).get("projects"),
            state.get("brand_evidence_boundaries", {}).get("brand_pricing"),
            state.get("brand_evidence_boundaries", {}).get("local_presence"),
        )
        return state

    async def _ensure_brand_evidence_state_current(
        self,
        state: Dict[str, Any],
        *,
        reason: str,
    ) -> Dict[str, Any]:
        """Refresh stale derived evidence before a downstream consumer can use it."""
        if not state.get("brand_url"):
            return state
        current = self._brand_evidence_source_fingerprint(state)
        derived = str(state.get("brand_evidence_derived_source_fingerprint") or "")
        required = (
            "brand_page_knowledge_pack_context",
            "brand_evidence_inventory",
            "brand_evidence_boundaries",
            "brand_offer_contract",
            "brand_generation_guardrails",
        )
        missing = [key for key in required if key not in state]
        if current == derived and not missing:
            return state
        logger.info(
            "[brand_evidence_stale] reason=%s current=%s derived=%s missing=%s",
            reason,
            current,
            derived or "none",
            missing,
        )
        return await self._refresh_brand_derived_evidence_state(
            state,
            reason=reason,
            rebuild_evidence_map=True,
        )


    def _extract_observed_pricing_signals(self, state: Dict[str, Any]) -> List[str]:
        """Extracts currency-grounded pricing patterns from SERP data (titles, snippets, meta)."""
        serp_data = state.get("serp_data", {})
        if not serp_data:
            return []

        found_mentions = []

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
            if not blob:
                continue
            for start, end, matched in _iter_observed_price_candidates(blob):
                if not _is_valid_observed_price_mention(blob, start, end, matched):
                    continue
                context = _observed_price_context(blob, start, end)
                if context:
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
        disable_repair = input_data.get("disable_outline_repair", False)
        if disable_repair:
            logger.warning("[outline_repair] STRUCTURAL REPAIR DISABLED for diagnostic run")
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

        buyer_journey_context = self._format_commercial_buyer_journey_context(state)
        if buyer_journey_context:
            state["commercial_buyer_journey_plan"] = self._build_commercial_buyer_journey_plan(state)

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
                brand_context=state.get("brand_context", "") + self._format_brand_evidence_inventory_context(state) + buyer_journey_context,
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
            
            try:
                compact_summary = ""
                guardrails_context = ""
            except Exception as e:
                logger.warning(f"Failed to build compact brand evidence summary or guardrails: {e}")
                compact_summary = ""
                guardrails_context = ""
            inventory_context = self._format_brand_evidence_inventory_context(state)
            self._record_ground_truth_consumption(state, "outline")
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
                    brand_context=h_brand_context + inventory_context + buyer_journey_context,
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
                if not disable_repair:
                    outline = self.outline_repair_service.promote_visitor_intents(
                        outline,
                        primary_keyword=state.get("primary_keyword", ""),
                        entity_phrase=entity_phrase,
                        serp_brief=state.get("serp_outline_brief")
                    )

                # TASK 3: FAQ De-duplication (safe/normalization operation: kept enabled)
                outline = self.outline_repair_service.dedupe_faq_against_h2(outline)
                # TASK 3b: FAQ Refill (restore minimum 4 FAQs after dedupe)
                if not disable_repair:
                    outline = self.outline_repair_service.refill_faq_after_dedupe(
                        outline,
                        entity_phrase=entity_phrase
                    )

                # TASK 3c: Deterministic FAQ Enrichment (Brand Utility)
                if not disable_repair:
                    if state.get("brand_generation_guardrails", {}).get("brand_section_policy") != "do_not_create_dedicated_brand_proof_or_why_choose_sections":
                        outline = self.outline_repair_service.enrich_brand_utility_faq(
                            outline,
                            serp_brief=state.get("serp_outline_brief", {}),
                            brand_context=state.get("brand_name", "") or state.get("display_brand_name", ""),
                            content_type=content_type,
                            entity_phrase=entity_phrase
                        )
                # (safe/normalization operation: kept enabled)
                outline = self.outline_repair_service.normalize_heading_only_section_types(outline)

                # Apply Anti-Echo and Strategic Map Repairs
                if not disable_repair:
                    outline = self.outline_repair_service.clean_echo_and_repetition(
                        outline, 
                        title=state.get("title", ""),
                        primary_keyword=state.get("primary_keyword", "")
                    )
                    outline = self.outline_repair_service.apply_strategic_map_and_roles(
                        outline,
                        primary_keyword=state.get("primary_keyword", ""),
                        content_type=content_type,
                        brand_name=state.get("brand_name", "") or state.get("display_brand_name", ""),
                        brand_evidence_inventory=self._brand_evidence_inventory_for_outline(state),
                    )

                # TASK 4: Conclusion Cleanup
                if not disable_repair:
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
                if not disable_repair:
                    if state.get("brand_generation_guardrails", {}).get("brand_section_policy") != "do_not_create_dedicated_brand_proof_or_why_choose_sections":
                        outline = self.outline_repair_service.enrich_brand_utility_faq(
                            outline,
                            serp_brief=state.get("serp_outline_brief", {}),
                            brand_context=state.get("brand_name", "") or state.get("display_brand_name", ""),
                            content_type=content_type,
                            entity_phrase=entity_phrase,
                        )
                outline = self.outline_repair_service.normalize_heading_only_section_types(outline)
                if not disable_repair:
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

        # Keep the commercial mix authoritative even in heading/content-stage
        # paths where the heavier validation block is intentionally relaxed.
        outline, dist_errors = self.validator.enforce_intent_distribution(
            outline,
            intent,
            content_type,
        )
        if dist_errors:
            logger.warning("[intent_distribution] Post-outline correction warnings: %s", " | ".join(dist_errors))

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

        # The raw outline promises determine which extra brand pages are needed.
        # Crawl and refresh evidence before any evidence-based heading downgrade,
        # otherwise the outline is judged against the older pre-outline snapshot.
        state["outline"] = outline
        state = await self._run_post_outline_brand_targeted_crawl(state, outline)
        outline = state.get("outline", outline)

        outline = self._normalize_outline_with_brand_evidence_inventory(outline, state)
        outline = self._ensure_commercial_buyer_journey_coverage(outline, state)
        for idx, sec in enumerate(outline):
            self.outline_gen._normalize_section(
                sec, idx, content_type, content_strategy, area
            )

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

    def _section_visibly_references_brand(self, section: Dict[str, Any], state: Dict[str, Any]) -> bool:
        brand_name = str(state.get("brand_name") or state.get("display_brand_name") or "").strip().lower()
        aliases = state.get("brand_aliases") or []
        refs = [brand_name] + [str(alias).strip().lower() for alias in aliases if str(alias).strip()]
        refs = [ref for ref in refs if ref]
        if not refs:
            return False
        visible_text = " ".join([
            str(section.get("heading_text") or ""),
            " ".join(self._subheading_text(item) for item in section.get("subheadings", []) or []),
        ]).lower()
        return any(ref in visible_text for ref in refs)

    def _brand_evidence_inventory_for_outline(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Return a compact inventory view without mutating canonical brand context."""
        inventory = state.get("brand_evidence_inventory")
        if not isinstance(inventory, dict):
            try:
                from src.services.brand_evidence_service import build_brand_evidence_inventory
                inventory = build_brand_evidence_inventory(state)
            except Exception:
                inventory = {}

        return {
            "services_available": bool(inventory.get("services_available")),
            "projects_available": bool(inventory.get("projects_available")),
            "pricing_available": bool(inventory.get("pricing_available")),
            "process_available": bool(inventory.get("process_available")),
            "trust_available": bool(inventory.get("trust_available")),
            "explicit_geography": [
                str(item).strip()
                for item in (inventory.get("explicit_geography") or [])
                if str(item).strip()
            ][:8],
            "confidence": inventory.get("confidence") or "low",
        }

    def _format_brand_evidence_inventory_context(self, state: Dict[str, Any]) -> str:
        inventory = self._brand_evidence_inventory_for_outline(state)
        return (
            "\n[BRAND EVIDENCE INVENTORY - OUTLINE GATE]\n"
            + json.dumps(inventory, ensure_ascii=False)
            + "\nRules:\n"
            "- Do not mention the brand in every section; generic headings should stay generic.\n"
            "- Brand-owned headings must be answerable from this inventory.\n"
            "- Do not create brand project headings unless projects_available is true.\n"
            "- Do not create Brand projects in a country/city unless explicit_geography supports it.\n"
            "- If projects exist but geography is unsupported, use Projects shown by Brand, not Projects by Brand in a location.\n"
            "- Do not create brand pricing/packages headings unless pricing_available is true.\n"
            "- Do not create dedicated brand-proof sections for low-confidence inventory.\n"
            "[END BRAND EVIDENCE INVENTORY]\n"
        )

    def _build_commercial_buyer_journey_plan(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Build a domain-neutral commercial decision map for outline planning."""
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return {}

        inventory = self._brand_evidence_inventory_for_outline(state)
        content_strategy = state.get("content_strategy", {}) if isinstance(state.get("content_strategy"), dict) else {}
        serp_outline_brief = state.get("serp_outline_brief", {}) if isinstance(state.get("serp_outline_brief"), dict) else {}
        seo_intelligence = state.get("seo_intelligence", {}) if isinstance(state.get("seo_intelligence"), dict) else {}
        structural = (
            seo_intelligence.get("market_analysis", {})
            .get("structural_intelligence", {})
            if isinstance(seo_intelligence.get("market_analysis", {}), dict)
            else {}
        )

        text_parts = [
            state.get("primary_keyword", ""),
            state.get("raw_title", ""),
            " ".join(str(item) for item in state.get("keywords", []) or []),
            content_strategy.get("market_angle", ""),
            content_strategy.get("primary_angle", ""),
            " ".join(str(item) for item in content_strategy.get("pain_point_focus", []) or []),
            " ".join(str(item) for item in serp_outline_brief.get("observed_topics", []) or []),
            " ".join(str(item) for item in serp_outline_brief.get("heading_candidates", []) or []),
        ]
        topic_blob = " ".join(str(part or "") for part in text_parts).casefold()

        def has_any(*terms: str) -> bool:
            return any(term.casefold() in topic_blob for term in terms if term)

        selected_roles: List[Dict[str, Any]] = [
            {
                "role": "intro_problem",
                "coverage_role": "introduction",
                "section_type": "introduction",
                "job": "Open from the reader's problem, risk, or desired outcome before any selling.",
                "brand_mode": "soft_intro_brand",
            },
            {
                "role": "service_scope",
                "coverage_role": "offer_clarity",
                "section_type": "offer",
                "job": "Explain what the buyer is evaluating or buying, including major option families when useful.",
                "brand_mode": "brand_light",
            },
            {
                "role": "evaluation_criteria",
                "coverage_role": "custom_domain_topic",
                "section_type": "core",
                "job": "Give practical decision criteria that help the reader compare providers, products, or options.",
                "brand_mode": "neutral_market",
            },
            {
                "role": "process",
                "coverage_role": "process_or_how",
                "section_type": "process",
                "job": "Reduce friction by showing the practical journey or implementation steps.",
                "brand_mode": "brand_owned" if inventory.get("process_available") else "neutral_market",
            },
            {
                "role": "faq_objections",
                "coverage_role": "faq",
                "section_type": "faq",
                "job": "Answer purchase objections and practical questions directly.",
                "brand_mode": "neutral_market",
            },
            {
                "role": "final_cta",
                "coverage_role": "conclusion",
                "section_type": "conclusion",
                "job": "Close with a confident next step after value and proof have been earned.",
                "brand_mode": "brand_cta",
            },
        ]

        if inventory.get("services_available") and inventory.get("confidence") != "low":
            selected_roles.insert(
                4,
                {
                    "role": "brand_fit",
                    "coverage_role": "differentiators",
                    "section_type": "differentiation",
                    "job": "Explain why the observed brand capabilities fit the buyer need, without generic superiority claims.",
                    "brand_mode": "brand_owned",
                },
            )

        if inventory.get("projects_available") or inventory.get("trust_available"):
            selected_roles.insert(
                5,
                {
                    "role": "proof",
                    "coverage_role": "proof",
                    "section_type": "proof",
                    "job": "Use observed projects, case studies, trust evidence, or source-backed examples.",
                    "brand_mode": "brand_owned",
                },
            )

        if inventory.get("process_available") or has_any("process", "workflow", "steps", "journey", "طريقة", "خطوات", "مراحل"):
            selected_roles.append(
                {
                    "role": "process",
                    "coverage_role": "process_or_how",
                    "section_type": "process",
                    "job": "Reduce friction by showing the practical journey or implementation steps.",
                    "brand_mode": "brand_owned" if inventory.get("process_available") else "neutral_market",
                }
            )

        optional_roles: List[Dict[str, str]] = [
            {
                "role": "features_included",
                "use_when": (
                    "The offer section does not already explain concrete inclusions, "
                    "deliverables, or capabilities as a distinct angle."
                ),
            },
            {
                "role": "comparison",
                "use_when": (
                    "There are genuinely different options or decision paths to compare. "
                    "Otherwise merge comparison guidance into evaluation criteria."
                ),
            },
        ]
        if has_any("security", "secure", "speed", "performance", "privacy", "compliance", "أمان", "حماية", "سرعة", "أداء"):
            optional_roles.append({
                "role": "security_performance",
                "use_when": "The topic or SERP suggests safety, reliability, speed, performance, privacy, or compliance affects the buying decision.",
            })
        if has_any("technology", "software", "integration", "automation", "platform", "tools", "تقنية", "تقنيات", "تكامل", "منصة", "برنامج"):
            optional_roles.append({
                "role": "technology_or_capability",
                "use_when": "Technology, integrations, tooling, or platform capability is a real decision factor.",
            })
        if has_any("roi", "return", "conversion", "growth", "revenue", "value", "هوية", "تحويل", "نمو", "عائد", "قيمة"):
            optional_roles.append({
                "role": "business_impact",
                "use_when": "The buyer cares about business value, conversion, trust, growth, operational efficiency, or brand impact.",
            })

        pricing_presence = structural.get("pricing_presence_ratio", 0) if isinstance(structural, dict) else 0
        if inventory.get("pricing_available") or pricing_presence > 0 or has_any("price", "pricing", "cost", "budget", "roi", "سعر", "أسعار", "تكلفة", "ميزانية", "عائد"):
            optional_roles.append({
                "role": "cost_value_roi",
                "use_when": "Discuss cost, value, budget, or ROI as market guidance. Brand prices/packages require explicit brand evidence.",
            })

        if state.get("area"):
            optional_roles.append({
                "role": "local_market_fit",
                "use_when": "Use the target area as buyer context only. Do not imply brand local presence without explicit brand geography evidence.",
            })

        disabled_claims = []
        if not inventory.get("pricing_available"):
            disabled_claims.append("brand pricing, package tiers, exact price ranges, or support plans")
        if not inventory.get("explicit_geography"):
            disabled_claims.append("brand local office, local market presence, or projects in the target area unless a project page explicitly says so")
        if not (inventory.get("projects_available") or inventory.get("trust_available")):
            disabled_claims.append("dedicated brand proof, case-study, project, testimonial, award, or certification claims")

        return {
            "purpose": "Map the buyer's commercial decision journey. These role names are not headings.",
            "article_rule": "Choose 5-8 visible H2 sections and merge roles naturally when needed; do not copy this list as headings.",
            "selected_roles": self._dedupe_commercial_plan_roles(selected_roles),
            "optional_roles": optional_roles,
            "disabled_claims": disabled_claims,
            "merge_guidance": [
                "problem_stakes can live inside the introduction.",
                "service_scope and features_included can be separate only when each has distinct content.",
                "brand_fit and proof must not repeat the same examples.",
                "comparison should stand alone only when options truly differ; otherwise embed it.",
                "cost_value_roi may be market guidance, not brand pricing, unless pricing_available is true.",
            ],
            "anti_bias_rules": [
                "Do not use industry-specific headings from another article as templates.",
                "Do not force security, technology, ROI, local, pricing, or proof sections unless they are relevant to this topic and evidence.",
                "Do not make every section about the brand.",
            ],
        }

    def _format_commercial_buyer_journey_context(self, state: Dict[str, Any]) -> str:
        plan = self._build_commercial_buyer_journey_plan(state)
        if not plan:
            return ""
        return (
            "\n[COMMERCIAL BUYER JOURNEY PLAN]\n"
            + json.dumps(plan, ensure_ascii=False)
            + "\nRules:\n"
            "- Treat this as a role map for purchase-decision coverage, not a heading template.\n"
            "- Role names must not appear as visible headings.\n"
            "- Choose and merge roles based on topic, SERP intent, brand evidence, and reader decision needs.\n"
            "- Competitor-style structures may inspire missing decision angles, but never copy their headings or unsupported claims.\n"
            "[END COMMERCIAL BUYER JOURNEY PLAN]\n"
        )

    def _dedupe_commercial_plan_roles(self, roles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Keep the role map stable when multiple signals ask for the same buyer-journey role."""
        deduped: List[Dict[str, Any]] = []
        seen = set()
        for role in roles or []:
            if not isinstance(role, dict):
                continue
            key = str(role.get("role") or "").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(role)
        return deduped

    def _commercial_plan_role_to_section_role(self, role_name: str) -> str:
        role_map = {
            "intro_problem": "intro",
            "service_scope": "service_explanation",
            "features_included": "features_included",
            "evaluation_criteria": "evaluation_criteria",
            "brand_fit": "brand_differentiator",
            "proof": "proof",
            "comparison": "comparison",
            "process": "process",
            "faq_objections": "faq",
            "final_cta": "cta",
            "cost_value_roi": "cost_value",
            "security_performance": "security_performance",
            "technology_or_capability": "technology_or_capability",
            "business_impact": "business_impact",
        }
        return role_map.get(role_name, role_name)

    def _commercial_optional_topic_factors(self, state: Dict[str, Any]) -> List[str]:
        """Infer optional standalone commercial sections without hardcoding an industry.

        Decision-review signals such as security, performance, technology, and
        business impact usually belong inside the evaluation criteria section.
        Keeping them out of auto-added H2s prevents duplicated "criteria"
        sections while the buyer journey plan can still mention them as angles.
        """
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return []

        content_strategy = state.get("content_strategy", {}) if isinstance(state.get("content_strategy"), dict) else {}
        serp_outline_brief = state.get("serp_outline_brief", {}) if isinstance(state.get("serp_outline_brief"), dict) else {}
        seo_intelligence = state.get("seo_intelligence", {}) if isinstance(state.get("seo_intelligence"), dict) else {}
        structural = (
            seo_intelligence.get("market_analysis", {})
            .get("structural_intelligence", {})
            if isinstance(seo_intelligence.get("market_analysis", {}), dict)
            else {}
        )

        text_parts = [
            state.get("primary_keyword", ""),
            state.get("raw_title", ""),
            " ".join(str(item) for item in state.get("keywords", []) or []),
            content_strategy.get("market_angle", ""),
            content_strategy.get("primary_angle", ""),
            " ".join(str(item) for item in content_strategy.get("pain_point_focus", []) or []),
            " ".join(str(item) for item in serp_outline_brief.get("observed_topics", []) or []),
            " ".join(str(item) for item in serp_outline_brief.get("heading_candidates", []) or []),
        ]
        blob = " ".join(str(part or "") for part in text_parts).casefold()

        def has_any(terms: List[str]) -> bool:
            return any(term.casefold() in blob for term in terms if term)

        factors: List[str] = []
        if has_any(["security", "secure", "speed", "performance", "privacy", "compliance", "أمان", "حماية", "سرعة", "أداء", "خصوصية", "امتثال"]):
            factors.append("security_performance")
        if has_any(["technology", "software", "integration", "automation", "platform", "tools", "تقنية", "تقنيات", "تكامل", "منصة", "برنامج", "برمجيات", "أدوات"]):
            factors.append("technology_or_capability")
        if has_any(["roi", "return", "conversion", "growth", "revenue", "value", "عائد", "قيمة", "نمو", "تحويل", "مبيعات", "ثقة", "هوية"]):
            factors.append("business_impact")

        pricing_presence = structural.get("pricing_presence_ratio", 0) if isinstance(structural, dict) else 0
        if pricing_presence > 0 or has_any(["price", "pricing", "cost", "budget", "investment", "roi", "سعر", "أسعار", "تكلفة", "ميزانية", "استثمار", "عائد"]):
            factors.append("cost_value")

        return list(dict.fromkeys(factors))

    def _merge_commercial_decision_review_sections(
        self,
        outline: List[Dict[str, Any]],
        state: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Merge overlapping commercial decision-review H2s into one criteria block."""
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return outline

        decision_roles = {
            "evaluation_criteria",
            "security_performance",
            "technology_or_capability",
            "business_impact",
        }
        sections = [dict(section) for section in outline or [] if isinstance(section, dict)]
        decision_indexes: List[int] = []
        for idx, section in enumerate(sections):
            if (section.get("heading_level") or "").upper() != "H2":
                continue
            role = str(
                section.get("commercial_section_role")
                or self._commercial_section_role_for_section(section, state, index=idx, total=len(sections))
            ).lower()
            if role in decision_roles:
                decision_indexes.append(idx)

        if len(decision_indexes) <= 1:
            return sections

        primary_idx = next(
            (
                idx for idx in decision_indexes
                if str(
                    sections[idx].get("commercial_section_role")
                    or self._commercial_section_role_for_section(sections[idx], state, index=idx, total=len(sections))
                ).lower() == "evaluation_criteria"
            ),
            decision_indexes[0],
        )
        primary = sections[primary_idx]
        primary["commercial_section_role"] = "evaluation_criteria"
        primary["coverage_role"] = "custom_domain_topic"
        primary["section_type"] = primary.get("section_type") or "core"

        subheadings = [
            text for text in (self._subheading_text(item) for item in primary.get("subheadings", []) or [])
            if text
        ]
        details = [
            str(item).strip()
            for item in primary.get("must_include_details", []) or []
            if str(item).strip()
        ]
        seen_subheadings = {text.casefold() for text in subheadings}
        seen_details = {text.casefold() for text in details}
        removed_indexes = set()

        for idx in decision_indexes:
            if idx == primary_idx:
                continue
            section = sections[idx]
            heading = str(section.get("heading_text") or "").strip()
            if heading and heading.casefold() not in seen_subheadings:
                subheadings.append(heading)
                seen_subheadings.add(heading.casefold())
            for item in section.get("subheadings", []) or []:
                text = self._subheading_text(item)
                if text and text.casefold() not in seen_subheadings:
                    subheadings.append(text)
                    seen_subheadings.add(text.casefold())
            if heading:
                detail = f"Cover '{heading}' as a concise decision factor, not as a separate repeated section."
                if detail.casefold() not in seen_details:
                    details.append(detail)
                    seen_details.add(detail.casefold())
            removed_indexes.add(idx)

        if subheadings:
            primary["subheadings"] = subheadings[:6]
        if details:
            primary["must_include_details"] = details[:8]

        merged = [section for idx, section in enumerate(sections) if idx not in removed_indexes]
        logger.info(
            "[commercial_role_dedupe] Merged %s decision-review sections into '%s'.",
            len(removed_indexes),
            primary.get("heading_text", ""),
        )
        return merged

    def _commercial_role_heading(self, role: str, state: Dict[str, Any]) -> str:
        """Create a domain-neutral heading for a missing commercial role."""
        lang = str(state.get("article_language") or "ar").lower()
        is_ar = lang.startswith("ar")
        primary_keyword = str(state.get("primary_keyword") or state.get("raw_title") or "الخدمة").strip()
        brand_name = str(state.get("brand_name") or state.get("display_brand_name") or "").strip()

        if is_ar:
            headings = {
                "service_explanation": f"{primary_keyword}: ما الخدمة أو الحل المناسب لاحتياجك؟",
                "features_included": "ما الذي يجب أن تشمله الخدمة قبل اتخاذ القرار؟",
                "evaluation_criteria": "معايير عملية لاختيار المزود أو الحل الأنسب",
                "brand_differentiator": f"كيف يقدّم {brand_name} قيمة عملية لهذا الاحتياج؟" if brand_name else "ما الذي يميز المزود المناسب لهذا الاحتياج؟",
                "proof": f"أدلة وأمثلة من أعمال {brand_name}" if brand_name else "أدلة وأمثلة تساعد على تقييم جودة التنفيذ",
                "comparison": "مقارنة بين الخيارات المتاحة حسب احتياج المشروع",
                "process": "خطوات الحصول على الخدمة من التقييم إلى التنفيذ",
                "cost_value": "التكلفة والقيمة المتوقعة قبل اتخاذ القرار",
                "security_performance": "الأمان والأداء كعاملين في قرار الشراء",
                "technology_or_capability": "القدرات التقنية والتكاملات التي تستحق المراجعة",
                "business_impact": "الأثر العملي والعائد المتوقع من الاستثمار",
                "faq": "أسئلة شائعة قبل التعاقد أو اتخاذ القرار",
                "cta": f"ابدأ الخطوة التالية مع {brand_name}" if brand_name else "ابدأ الخطوة التالية بثقة",
            }
        else:
            headings = {
                "service_explanation": f"{primary_keyword}: what does the right solution include?",
                "features_included": "What should be included before you decide?",
                "evaluation_criteria": "Practical criteria for choosing the right provider or option",
                "brand_differentiator": f"How {brand_name} fits this need in practical terms" if brand_name else "What makes a provider fit this need?",
                "proof": f"Evidence and examples from {brand_name}" if brand_name else "Evidence and examples for evaluating execution quality",
                "comparison": "Comparison of available options by project need",
                "process": "How the process works from evaluation to delivery",
                "cost_value": "Cost, value, and expected return before you decide",
                "security_performance": "Security and performance as buying-decision factors",
                "technology_or_capability": "Technical capabilities and integrations worth checking",
                "business_impact": "Practical business impact and expected return",
                "faq": "Common questions before you decide",
                "cta": f"Start the next step with {brand_name}" if brand_name else "Start the next step with confidence",
            }
        return headings.get(role, headings["evaluation_criteria"])

    def _commercial_role_section_type(self, role: str) -> str:
        return {
            "service_explanation": "offer",
            "features_included": "features",
            "evaluation_criteria": "core",
            "brand_differentiator": "differentiation",
            "proof": "proof",
            "comparison": "comparison",
            "process": "process",
            "cost_value": "pricing",
            "security_performance": "core",
            "technology_or_capability": "core",
            "business_impact": "core",
            "faq": "faq",
            "cta": "conclusion",
        }.get(role, "core")

    def _commercial_role_coverage(self, role: str) -> str:
        return {
            "service_explanation": "offer_clarity",
            "features_included": "features_or_included",
            "evaluation_criteria": "custom_domain_topic",
            "brand_differentiator": "differentiators",
            "proof": "proof",
            "comparison": "comparison",
            "process": "process_or_how",
            "cost_value": "pricing",
            "security_performance": "custom_domain_topic",
            "technology_or_capability": "custom_domain_topic",
            "business_impact": "custom_domain_topic",
            "faq": "faq",
            "cta": "conclusion",
        }.get(role, "custom_domain_topic")

    def _commercial_section_job(self, role: str) -> str:
        """Map legacy/current commercial roles to a compact buyer-journey job."""
        return {
            "service_explanation": "offer_scope",
            "features_included": "features_included",
            "brand_differentiator": "brand_fit",
            "proof": "proof",
            "comparison": "comparison",
            "process": "process",
            "cost_value": "cost_value",
            "faq": "faq",
            "cta": "cta",
            "intro": "offer_scope",
            "evaluation_criteria": "evaluation_criteria",
            "security_performance": "evaluation_criteria",
            "technology_or_capability": "evaluation_criteria",
            "business_impact": "evaluation_criteria",
        }.get(str(role or "").lower(), "features_included")

    def _commercial_buyer_stage(self, role: str) -> str:
        """Return the buyer-decision stage this section should serve."""
        role = str(role or "").lower()
        if role in {"intro", "service_explanation"}:
            return "awareness"
        if role in {
            "features_included",
            "evaluation_criteria",
            "security_performance",
            "technology_or_capability",
            "business_impact",
            "comparison",
            "cost_value",
        }:
            return "evaluation"
        if role in {"brand_differentiator", "proof"}:
            return "validation"
        if role in {"process", "faq", "cta"}:
            return "decision"
        return "awareness"

    def _commercial_buyer_question(self, role: str, section: Dict[str, Any]) -> str:
        """Stable buyer-question owner used to prevent duplicate section jobs."""
        role = str(role or "").lower()
        if role == "service_explanation":
            return "what_is_the_offer_or_service"
        if role == "features_included":
            return "what_is_included"
        if role in {"evaluation_criteria", "security_performance", "technology_or_capability", "business_impact"}:
            return "how_should_i_evaluate_the_option"
        if role == "brand_differentiator":
            return "why_this_provider_or_solution"
        if role == "proof":
            return "what_evidence_supports_the_claim"
        if role == "comparison":
            return "which_options_are_different"
        if role == "process":
            return "how_it_works"
        if role == "cost_value":
            return "what_cost_or_value_should_i_expect"
        if role == "faq":
            return "what_objections_remain"
        if role == "cta":
            return "what_next_step_should_i_take"
        if role == "intro":
            return "why_should_i_keep_reading"
        heading = re.sub(r"\s+", " ", str(section.get("heading_text") or "")).strip().casefold()
        return heading[:80] or "general_reader_question"

    def _section_allows_project_proof(self, section: Dict[str, Any], state: Dict[str, Any]) -> bool:
        role = str(section.get("commercial_section_role") or self._commercial_section_role_for_section(section, state)).lower()
        section_type = str(section.get("section_type") or "").lower()
        return role == "proof" or section_type in {"proof", "case_study", "case-study"}

    def _section_table_policy(self, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        section_type = str(section.get("section_type") or "").lower()
        role = str(section.get("commercial_section_role") or self._commercial_section_role_for_section(section, state)).lower()
        if section_type in {"introduction", "intro", "conclusion", "faq"} or role in {"intro", "cta", "faq", "process"}:
            return "forbidden"
        if section.get("requires_table"):
            return "required"
        if role in {"comparison", "cost_value", "features_included", "proof", "service_explanation"}:
            return "allowed"
        return "forbidden"

    def _build_section_intent_snapshot(self, section: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, str]:
        """Create the internal section job contract consumed by writer/gates/logs."""
        role = str(section.get("commercial_section_role") or self._commercial_section_role_for_section(section, state)).lower()
        brand_policy = str(section.get("brand_usage_policy") or self._brand_usage_policy_for_section(section, state)).lower()
        if role == "proof":
            evidence_expectation = "projects" if self._section_allows_project_proof(section, state) else "brand_pack"
        elif brand_policy in {"brand_owned", "brand_light", "brand_cta", "soft_intro_brand"}:
            evidence_expectation = "brand_pack"
        elif role in {"comparison", "cost_value", "evaluation_criteria"}:
            evidence_expectation = "market"
        else:
            evidence_expectation = "none"

        return {
            "buyer_question": self._commercial_buyer_question(role, section),
            "section_job": self._commercial_section_job(role),
            "buyer_stage": self._commercial_buyer_stage(role),
            "brand_usage_policy": brand_policy or "neutral_market",
            "evidence_expectation": evidence_expectation,
            "table_policy": self._section_table_policy(section, state),
            "project_usage": "proof_only" if role == "proof" else ("light" if role == "brand_differentiator" else "none"),
            "source_heading": str(section.get("heading_text") or "").strip(),
        }

    def _merge_duplicate_commercial_buyer_questions(
        self,
        outline: List[Dict[str, Any]],
        state: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Merge repeated commercial sections that answer the same buyer question."""
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return outline

        keepers: List[Dict[str, Any]] = []
        seen: Dict[str, Dict[str, Any]] = {}
        mergeable_questions = {
            "how_should_i_evaluate_the_option",
            "what_is_included",
            "which_options_are_different",
        }
        protected_roles = {"intro", "proof", "process", "faq", "cta", "brand_differentiator", "cost_value"}
        collisions: List[Dict[str, str]] = []

        for section in outline or []:
            role = str(section.get("commercial_section_role") or self._commercial_section_role_for_section(section, state)).lower()
            question = self._commercial_buyer_question(role, section)
            if question in seen and question in mergeable_questions and role not in protected_roles:
                primary = seen[question]
                heading = str(section.get("heading_text") or "").strip()
                if heading:
                    details = primary.setdefault("must_include_details", [])
                    note = f"Cover '{heading}' as a concise angle inside this section; do not repeat it as a separate H2."
                    if note not in details:
                        details.append(note)
                for sub in section.get("subheadings", []) or []:
                    sub_text = self._subheading_text(sub)
                    if sub_text:
                        primary.setdefault("subheadings", [])
                        if sub_text not in primary["subheadings"]:
                            primary["subheadings"].append(sub_text)
                collisions.append({
                    "removed_heading": heading,
                    "kept_heading": str(primary.get("heading_text") or ""),
                    "buyer_question": question,
                })
                continue
            seen.setdefault(question, section)
            keepers.append(section)

        if collisions:
            state.setdefault("commercial_role_collision_report", []).extend(collisions)
            logger.info("[commercial_role_collision] merged=%s details=%s", len(collisions), collisions[:5])
        return keepers

    def _ensure_article_table_plan(self, outline: List[Dict[str, Any]], state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Ensure commercial articles plan 1-2 useful tables before writing."""
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return outline
        if not state.get("include_tables", True):
            return outline

        for section in outline:
            table_type = str(section.get("table_type") or "").lower()
            explicit_project_table = table_type in {"project_evidence_table", "project_table"}
            if section.get("requires_table") and self._is_project_like_section(section) and (
                not explicit_project_table
                or not self._has_minimum_project_table_evidence(state, minimum=2)
            ):
                section["requires_table"] = False
                section["table_type"] = "project_proof_cards"
                logger.info(
                    "[TableAssigner] Using project proof cards instead of a default project table for section: %s",
                    section.get("heading_text", ""),
                )

        assigned = [
            section for section in outline
            if section.get("requires_table")
            and self._section_table_policy(section, state) != "forbidden"
        ]
        if len(assigned) > 2:
            for section in assigned[2:]:
                section["requires_table"] = False
                section["table_type"] = "none"
            assigned = assigned[:2]

        if assigned:
            for section in outline:
                section["section_intent_snapshot"] = self._build_section_intent_snapshot(section, state)
            return outline

        role_priority = ("comparison", "cost_value", "service_explanation", "features_included")
        fallback = None
        for role in role_priority:
            fallback = next(
                (
                    section for section in outline
                    if str(section.get("commercial_section_role") or "") == role
                    and self._section_table_policy(section, state) != "forbidden"
                ),
                None,
            )
            if fallback:
                break

        if fallback:
            role = str(fallback.get("commercial_section_role") or "")
            fallback["requires_table"] = True
            fallback["table_type"] = {
                "comparison": "decision_comparison",
                "cost_value": "cost_factors",
                "service_explanation": "offer_options",
                "features_included": "feature_checklist",
            }.get(role, "decision_comparison")
            logger.info(
                "[TableAssigner] Added mandatory article table target to section: %s",
                fallback.get("heading_text", ""),
            )

        for section in outline:
            section["section_intent_snapshot"] = self._build_section_intent_snapshot(section, state)
        return outline

    def _ensure_commercial_buyer_journey_coverage(
        self,
        outline: List[Dict[str, Any]],
        state: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Cover the buyer journey without turning its role map into a fixed H2 template."""
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return outline

        plan = self._build_commercial_buyer_journey_plan(state)
        selected_roles = self._dedupe_commercial_plan_roles(plan.get("selected_roles", []))
        required_roles = [
            self._commercial_plan_role_to_section_role(item.get("role", ""))
            for item in selected_roles
            if isinstance(item, dict)
        ]
        required_roles = [role for role in required_roles if role and role != "intro"]

        optional_roles = [
            role for role in self._commercial_optional_topic_factors(state)
            if role not in {"security_performance", "technology_or_capability", "business_impact"}
        ]
        for role in optional_roles:
            if role not in required_roles:
                required_roles.append(role)

        prepared = [dict(section) for section in outline]

        def role_for(section: Dict[str, Any]) -> str:
            return str(section.get("commercial_section_role") or self._commercial_section_role_for_section(section, state)).lower()

        for idx, section in enumerate(prepared):
            self._apply_commercial_section_role(section, state, idx, len(prepared))

        def merge_existing_section(
            primary: Dict[str, Any],
            secondary: Dict[str, Any],
            covered_role: str,
            reason: str,
        ) -> None:
            primary.setdefault("merged_coverage_roles", [])
            if covered_role not in primary["merged_coverage_roles"]:
                primary["merged_coverage_roles"].append(covered_role)
            primary.setdefault("must_include_details", [])
            secondary_heading = str(secondary.get("heading_text") or "").strip()
            if secondary_heading:
                note = (
                    f"Cover '{secondary_heading}' as a distinct angle inside this section; "
                    "do not repeat it as another H2."
                )
                if note not in primary["must_include_details"]:
                    primary["must_include_details"].append(note)
            primary.setdefault("subheadings", [])
            for subheading in secondary.get("subheadings", []) or []:
                text = self._subheading_text(subheading)
                if text and text not in primary["subheadings"]:
                    primary["subheadings"].append(text)
            state.setdefault("commercial_coverage_report", []).append({
                "role": covered_role,
                "action": "merged_existing",
                "reason": reason,
                "removed_section_id": secondary.get("section_id"),
                "target_section_id": primary.get("section_id"),
            })
            logger.info(
                "[commercial_coverage_gate] Merged existing role '%s' from '%s' into '%s' (%s).",
                covered_role,
                secondary.get("heading_text", ""),
                primary.get("heading_text", ""),
                reason,
            )

        service_section = next(
            (section for section in prepared if role_for(section) == "service_explanation"),
            None,
        )
        if service_section:
            service_blob = " ".join([
                str(service_section.get("heading_text") or ""),
                " ".join(
                    self._subheading_text(item)
                    for item in service_section.get("subheadings", []) or []
                ),
            ])
            includes_scope = bool(re.search(
                r"\b(?:include|includes|included|inclusions|what you get|deliverables|scope)\b|"
                r"(?:يشمل|تتضمن|يتضمن|ما تحصل عليه|نطاق الخدمة)",
                service_blob,
                re.IGNORECASE,
            ))
            if includes_scope:
                feature_sections = [
                    section for section in prepared
                    if role_for(section) == "features_included"
                ]
                for feature_section in feature_sections:
                    merge_existing_section(
                        service_section,
                        feature_section,
                        "features_included",
                        "offer_section_already_covers_inclusions",
                    )
                if feature_sections:
                    prepared = [
                        section for section in prepared
                        if section not in feature_sections
                    ]

        comparison_section = next(
            (section for section in prepared if role_for(section) == "comparison"),
            None,
        )
        if comparison_section:
            evaluation_sections = [
                section for section in prepared
                if role_for(section) == "evaluation_criteria"
            ]
            for evaluation_section in evaluation_sections:
                detail_count = len(evaluation_section.get("must_include_details", []) or [])
                subheading_count = len(evaluation_section.get("subheadings", []) or [])
                is_auto = str(evaluation_section.get("section_id") or "").startswith("sec_auto_")
                if is_auto or (detail_count < 2 and subheading_count < 2):
                    merge_existing_section(
                        comparison_section,
                        evaluation_section,
                        "evaluation_criteria",
                        "comparison_already_covers_decision_criteria",
                    )
                    prepared.remove(evaluation_section)

        prepared = self._merge_commercial_decision_review_sections(prepared, state)
        prepared = self._merge_duplicate_commercial_buyer_questions(prepared, state)

        for idx, section in enumerate(prepared):
            self._apply_commercial_section_role(section, state, idx, len(prepared))

        def has_role(role: str) -> bool:
            return any(role_for(section) == role for section in prepared)

        def merged_roles(section: Dict[str, Any]) -> set:
            return {
                str(value or "").lower()
                for value in section.get("merged_coverage_roles", []) or []
                if str(value or "").strip()
            }

        def role_is_already_covered(role: str) -> bool:
            if has_role(role):
                return True
            if any(role in merged_roles(section) for section in prepared):
                return True
            if role == "evaluation_criteria" and has_role("comparison"):
                comparison = next(
                    section for section in prepared
                    if role_for(section) == "comparison"
                )
                add_merged_role(
                    comparison,
                    role,
                    "Include concise decision criteria inside the comparison; "
                    "do not add a separate evaluation H2 unless it has a distinct evidence-backed gap.",
                )
                return True
            return False

        def h2_count() -> int:
            return sum(
                1
                for section in prepared
                if (section.get("heading_level") or "").upper() == "H2"
            )

        def add_merged_role(target: Dict[str, Any], role: str, note: str) -> None:
            target.setdefault("merged_coverage_roles", [])
            if role not in target["merged_coverage_roles"]:
                target["merged_coverage_roles"].append(role)
            target.setdefault("must_include_details", [])
            if note not in target["must_include_details"]:
                target["must_include_details"].append(note)
            state.setdefault("commercial_coverage_report", []).append({
                "role": role,
                "action": "merged",
                "target_section_id": target.get("section_id"),
                "target_heading": target.get("heading_text", ""),
            })
            logger.info(
                "[commercial_coverage_gate] Merged missing role '%s' into existing H2: %s",
                role,
                target.get("heading_text", ""),
            )

        def try_merge_role(role: str) -> bool:
            target_roles = {
                "features_included": ("service_explanation",),
                "evaluation_criteria": ("comparison", "features_included", "service_explanation"),
                "comparison": ("evaluation_criteria",),
                "brand_differentiator": ("proof", "service_explanation"),
                "proof": ("brand_differentiator",),
                "cost_value": ("comparison", "evaluation_criteria"),
            }.get(role, ())
            target = next(
                (
                    section
                    for target_role in target_roles
                    for section in prepared
                    if role_for(section) == target_role
                ),
                None,
            )
            if not target:
                return False
            notes = {
                "features_included": (
                    "Also explain the concrete inclusions or deliverables here; "
                    "do not create a second section that repeats the offer scope."
                ),
                "evaluation_criteria": (
                    "Include concise decision criteria within this section without "
                    "adding a separate evaluation H2."
                ),
                "comparison": (
                    "Compare only genuinely different options inside this section; "
                    "skip a standalone comparison when there is no useful contrast."
                ),
                "brand_differentiator": (
                    "Explain the source-backed brand fit here without repeating the same evidence."
                ),
                "proof": (
                    "Support this section with source-backed proof enabled by the brand evidence boundaries."
                ),
                "cost_value": (
                    "Include concise market-level cost or value guidance here; "
                    "do not imply brand pricing without explicit evidence."
                ),
            }
            add_merged_role(target, role, notes[role])
            return True

        def make_room_for_terminal_role(role: str) -> bool:
            if role not in {"faq", "cta"}:
                return False
            candidate = next(
                (
                    section for section in reversed(prepared)
                    if role_for(section) == "informational"
                    and (section.get("heading_level") or "").upper() == "H2"
                ),
                None,
            )
            target = next(
                (
                    section for section in prepared
                    if role_for(section) in {
                        "evaluation_criteria",
                        "features_included",
                        "service_explanation",
                        "comparison",
                        "brand_differentiator",
                        "proof",
                    }
                ),
                None,
            )
            if not candidate or not target or candidate is target:
                return False
            merge_existing_section(
                target,
                candidate,
                "informational",
                f"made_room_for_{role}",
            )
            prepared.remove(candidate)
            return True

        def insert_before_terminal(new_section: Dict[str, Any], role: str) -> None:
            if role == "cta":
                prepared.append(new_section)
                return
            if role == "faq":
                cta_idx = next(
                    (
                        idx for idx, section in enumerate(prepared)
                        if role_for(section) == "cta"
                    ),
                    len(prepared),
                )
                prepared.insert(cta_idx, new_section)
                return
            terminal_idx = next(
                (
                    idx for idx, section in enumerate(prepared)
                    if role_for(section) in {"faq", "cta"}
                ),
                len(prepared),
            )
            prepared.insert(terminal_idx, new_section)

        exact_pk_already_used = any(
            (section.get("heading_level") or "").upper() == "H2"
            and section.get("contains_exact_primary_keyword")
            for section in prepared
        )

        for role in required_roles:
            if role in {"security_performance", "technology_or_capability", "business_impact", "cost_value"}:
                if role not in optional_roles:
                    continue
            if role_is_already_covered(role):
                continue
            if try_merge_role(role):
                continue
            if h2_count() >= 8 and not make_room_for_terminal_role(role):
                state.setdefault("commercial_coverage_report", []).append({
                    "role": role,
                    "action": "skipped_max_h2",
                    "h2_count": h2_count(),
                })
                logger.info(
                    "[commercial_coverage_gate] Skipped role '%s'; outline already has %s H2 sections.",
                    role,
                    h2_count(),
                )
                continue
            section_type = self._commercial_role_section_type(role)
            heading_level = "H2"
            section = {
                "section_id": f"sec_auto_{role}",
                "heading_text": self._commercial_role_heading(role, state),
                "heading_level": heading_level,
                "section_type": section_type,
                "section_intent": "Commercial" if role not in {"faq"} else "Informational",
                "coverage_role": self._commercial_role_coverage(role),
                "visual_format": "table" if role == "comparison" else ("bulleted_list" if role in {"features_included", "process"} else "compact_narrative"),
                "subheadings": [],
                "commercial_section_role": role,
            }
            if role == "comparison":
                section["requires_table"] = True
                section["table_type"] = "decision_matrix"
            if role == "features_included":
                section["requires_list"] = True
            if role == "process":
                section["requires_list"] = True
            if role == "service_explanation" and not exact_pk_already_used:
                section["contains_exact_primary_keyword"] = True
                section["requires_primary_keyword"] = True
                exact_pk_already_used = True
            elif role in {"service_explanation", "cta"}:
                section["requires_primary_keyword"] = True

            insert_before_terminal(section, role)
            state.setdefault("commercial_coverage_report", []).append({
                "role": role,
                "action": "added",
                "section_id": section["section_id"],
                "heading": section["heading_text"],
            })
            logger.info("[commercial_coverage_gate] Added missing role '%s' as H2: %s", role, section["heading_text"])

        prepared = self._merge_commercial_decision_review_sections(prepared, state)

        for idx, section in enumerate(prepared):
            self._apply_commercial_section_role(section, state, idx, len(prepared))

        prepared = self._merge_duplicate_commercial_buyer_questions(prepared, state)

        for idx, section in enumerate(prepared):
            self._apply_commercial_section_role(section, state, idx, len(prepared))

        return prepared

    def _derive_outline_brand_evidence_requirements(
        self,
        outline: List[Dict[str, Any]],
        state: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Infer which brand evidence buckets the finalized outline will need."""
        brand_name = str(state.get("brand_name") or state.get("display_brand_name") or "").strip()
        focus_terms: List[str] = []
        requirements = {
            "needs_services": False,
            "needs_projects": False,
            "needs_process": False,
            "needs_pricing": False,
            "needs_technologies": False,
            "needs_trust": False,
            "needs_geography": False,
            "focus_terms": focus_terms,
        }

        def section_blob(section: Dict[str, Any]) -> str:
            return " ".join([
                str(section.get("heading_text") or ""),
                str(section.get("section_type") or ""),
                str(section.get("taxonomy_axis") or ""),
                str(section.get("content_goal") or ""),
                str(section.get("section_promise") or ""),
                " ".join(self._subheading_text(item) for item in section.get("subheadings", []) or []),
            ]).casefold()

        term_groups = {
            "needs_services": ["brand_offer", "services", "service", "offer", "solutions", "خدمات", "خدمة", "حلول"],
            "needs_projects": ["brand_projects", "project", "projects", "portfolio", "case study", "case-study", "proof", "client", "clients", "examples", "مشاريع", "مشروع", "أعمال", "اعمال", "نماذج", "عملاء"],
            "needs_process": ["brand_process", "process", "workflow", "steps", "stages", "how it works", "خطوات", "مراحل", "طريقة", "تنفيذ"],
            "needs_pricing": ["pricing", "price", "cost", "package", "packages", "plans", "أسعار", "اسعار", "تكلفة", "باقات", "باقة"],
            "needs_technologies": ["brand_features", "technology", "technologies", "tech", "stack", "tools", "systems", "تقنيات", "تكنولوجيا", "أدوات", "انظمة", "أنظمة"],
            "needs_trust": ["brand_support", "differentiation", "trust", "certified", "certification", "testimonials", "reviews", "لماذا", "ثقة", "اعتماد", "شهادات", "تقييمات"],
            "needs_geography": ["geography", "local", "location", "market", "saudi", "riyadh", "السعودية", "الرياض", "السوق"],
        }

        for section in outline or []:
            if not isinstance(section, dict):
                continue
            blob = section_blob(section)
            heading = str(section.get("heading_text") or "").strip()
            if heading:
                focus_terms.append(heading)
            if brand_name and brand_name.casefold() in blob:
                focus_terms.append(brand_name)
            for key, terms in term_groups.items():
                if any(term in blob for term in terms):
                    requirements[key] = True

        requirements["focus_terms"] = list(dict.fromkeys(term for term in focus_terms if term))[:12]
        return requirements

    async def _run_post_outline_brand_targeted_crawl(
        self,
        state: Dict[str, Any],
        outline: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """After outline creation, crawl extra brand pages for promised evidence."""
        if not state.get("brand_url") or state.get("content_type") != "brand_commercial":
            return state

        requirements = self._derive_outline_brand_evidence_requirements(outline, state)
        active_requirements = [
            key for key, enabled in requirements.items()
            if key.startswith("needs_") and enabled
        ]
        state["outline_evidence_requirements"] = requirements
        if not active_requirements:
            return state

        focus_parts = [
            state.get("brand_crawl_focus", ""),
            state.get("primary_keyword", ""),
            state.get("raw_title", ""),
            " ".join(requirements.get("focus_terms") or []),
            " ".join(key.replace("needs_", "") for key in active_requirements),
        ]
        state["brand_crawl_focus"] = " ".join(str(part or "") for part in focus_parts).strip()

        try:
            max_pages = int(state.get("brand_outline_crawl_max_pages") or state.get("brand_crawl_max_pages") or 8)
        except (TypeError, ValueError):
            max_pages = 8
        max_pages = max(1, min(max_pages, 15))

        try:
            before_urls = {
                str(item.get("link") or item.get("url") or "")
                for item in state.get("internal_resources", []) or []
                if isinstance(item, dict)
            }
            state = await self.brand_evidence_service.enrich_brand_internal_resources(state, max_pages=max_pages)
            after_urls = {
                str(item.get("link") or item.get("url") or "")
                for item in state.get("internal_resources", []) or []
                if isinstance(item, dict)
            }
            state["post_outline_brand_crawl_report"] = {
                "requirements": requirements,
                "new_urls": sorted(url for url in (after_urls - before_urls) if url),
                "max_pages": max_pages,
            }
            state = await self._refresh_brand_derived_evidence_state(
                state,
                reason="post_outline_targeted_crawl",
                rebuild_evidence_map=True,
            )
            logger.info(
                "[post_outline_brand_crawl] requirements=%s new_urls=%s chunks=%s briefs=%s "
                "narrative_briefs=%s revision=%s",
                active_requirements,
                len(state["post_outline_brand_crawl_report"]["new_urls"]),
                len(state.get("brand_source_chunks", [])),
                len(state.get("brand_page_briefs", [])),
                len(state.get("brand_page_narrative_briefs", [])),
                state.get("brand_evidence_revision"),
            )
        except Exception as e:
            logger.warning("[post_outline_brand_crawl] skipped due to error: %s", e)
        return state

    def _section_role_should_use_brand_evidence(self, section: Dict[str, Any], state: Dict[str, Any]) -> bool:
        """Brand-commercial sections that should be grounded in brand evidence."""
        if state.get("brand_evidence_failure_mode"):
            return False
        content_type = str(state.get("content_type") or section.get("content_type") or "").lower()
        if content_type != "brand_commercial":
            return False

        section_type = str(section.get("section_type") or "").lower()
        heading_level = str(section.get("heading_level") or "").upper()
        commercial_role = str(
            section.get("commercial_section_role")
            or self._commercial_section_role_for_section(section, state)
        ).lower()
        heading_references_brand = self._section_visibly_references_brand(section, state)
        if commercial_role in {"informational", "comparison", "cost_value", "faq"} and not heading_references_brand:
            return False
        if commercial_role in {"service_explanation", "features_included"} and not heading_references_brand:
            return False
        if commercial_role in {"brand_differentiator", "proof"}:
            return True
        if commercial_role == "process":
            return heading_references_brand
        if commercial_role in {"intro", "cta"}:
            return True

        if section_type in {"introduction", "intro", "conclusion"} or heading_level == "INTRO":
            return True
        if section_type in {"faq", "comparison", "pricing", "packages", "location"}:
            return self._section_visibly_references_brand(section, state)

        contract = section.get("section_contract") if isinstance(section.get("section_contract"), dict) else {}
        brand_policy = str(contract.get("brand_policy") or section.get("brand_policy") or "").lower()
        axis = str(section.get("taxonomy_axis") or contract.get("taxonomy_axis") or "").lower()
        if brand_policy == "commercial" or axis.startswith("brand_"):
            return True

        inventory = self._brand_evidence_inventory_for_outline(state)
        if section_type in {"offer", "services", "core_or_benefits"} and inventory.get("services_available"):
            return True
        if section_type in {"features", "differentiation", "differentiators", "brand_support", "brand"}:
            return any(inventory.get(key, False) for key in ("services_available", "projects_available", "process_available", "trust_available"))
        if section_type in {"process", "process_or_how"}:
            return bool(inventory.get("process_available") or inventory.get("services_available"))
        if section_type in {"proof", "case_study", "case-study"}:
            return bool(inventory.get("projects_available") or inventory.get("trust_available"))

        intent = str(section.get("section_intent") or "").lower()
        if intent in {"informational", "information", "info"}:
            return False

        if section_type in {"offer", "services", "core_or_benefits"}:
            return bool(inventory.get("services_available", True))
        if section_type in {"features", "differentiation", "differentiators", "brand_support", "brand"}:
            return any(inventory.get(key, False) for key in ("services_available", "projects_available", "process_available", "trust_available"))
        if section_type in {"process", "process_or_how"}:
            return bool(inventory.get("process_available") or inventory.get("services_available"))
        if section_type in {"proof", "case_study", "case-study"}:
            return bool(inventory.get("projects_available") or inventory.get("trust_available"))

        return "commercial" in intent and bool(inventory.get("services_available"))

    def _commercial_section_role_for_section(
        self,
        section: Dict[str, Any],
        state: Dict[str, Any],
        index: Optional[int] = None,
        total: Optional[int] = None,
    ) -> str:
        """Assign a domain-neutral commercial funnel role to a section."""
        section_type = str(section.get("section_type") or "").lower().strip()
        axis = str(section.get("taxonomy_axis") or "").lower().strip()
        coverage_role = str(section.get("coverage_role") or "").lower().strip()
        heading = " ".join(
            [
                str(section.get("heading_text") or ""),
                " ".join(self._subheading_text(item) for item in section.get("subheadings", []) or []),
            ]
        ).casefold()

        def has(*patterns: str) -> bool:
            return any(re.search(pattern, heading, re.IGNORECASE) for pattern in patterns)

        is_comparison_heading = has(
            r"\b(compare|comparison|versus|difference between| vs )\b",
            "\u0645\u0642\u0627\u0631\u0646",
            "\u0642\u0627\u0631\u0646",
            "\u062a\u0642\u0627\u0631\u0646",
            "\u0627\u0644\u0641\u0631\u0642 \u0628\u064a\u0646",
            "\u0645\u0642\u0627\u0628\u0644",
        )
        is_process_heading = has(
            r"\b(process|steps|stages|workflow|how it works|how does .+ work)\b",
            "\u062e\u0637\u0648\u0627\u062a",
            "\u0645\u0631\u0627\u062d\u0644",
            "\u0633\u064a\u0631 \u0627\u0644\u0639\u0645\u0644",
            "\u0622\u0644\u064a\u0629 \u0627\u0644\u0639\u0645\u0644",
            "\u0637\u0631\u064a\u0642\u0629 \u0627\u0644\u0639\u0645\u0644",
            "\u0643\u064a\u0641 (?:\u064a\u0639\u0645\u0644|\u062a\u0639\u0645\u0644|\u064a\u062a\u0645|\u062a\u062a\u0645)",
        )

        if section_type in {"introduction", "intro"} or str(section.get("heading_level") or "").upper() == "INTRO":
            return "intro"
        if section_type in {"conclusion"} or coverage_role == "conclusion" or has(
            r"\b(conclusion|final|start now|get started|contact|next steps?)\b",
            "\u062e\u0627\u062a\u0645",
            "\u0627\u0628\u062f\u0623",
            "\u062a\u0648\u0627\u0635\u0644",
            "\u0627\u0644\u062e\u0637\u0648\u0629 \u0627\u0644\u062a\u0627\u0644\u064a\u0629",
        ):
            return "cta"
        if section_type == "faq" or coverage_role == "faq":
            return "faq"
        if section_type in {"proof", "case_study", "case-study"} or coverage_role == "proof" or axis in {"brand_projects", "projects"}:
            return "proof"
        if section_type == "comparison" or coverage_role == "comparison" or axis == "comparison" or is_comparison_heading:
            return "comparison"
        if (
            section_type in {"process", "process_or_how", "how_it_works"}
            or coverage_role == "process_or_how"
            or is_process_heading
        ):
            return "process"
        if section_type == "core" and coverage_role == "custom_domain_topic" and has(
            r"\b(criteria|evaluate|evaluation|choose|choosing|checklist)\b",
            "\u0645\u0639\u0627\u064a\u064a\u0631",
            "\u062a\u0642\u064a\u064a\u0645",
            "\u0627\u062e\u062a\u064a\u0627\u0631",
            "\u062a\u062e\u062a\u0627\u0631",
        ):
            return "evaluation_criteria"
        if section_type in {"pricing", "packages"} or axis == "pricing" or has(
            r"\b(price|pricing|cost|budget|investment|value)\b",
            "\u0633\u0639\u0631",
            "\u0623\u0633\u0639\u0627\u0631",
            "\u062a\u0643\u0644\u0641",
            "\u0642\u064a\u0645\u0629",
            "\u0628\u0627\u0642",
        ):
            return "cost_value"
        if section_type == "core" and coverage_role == "custom_domain_topic" and has(
            r"\b(security|secure|speed|performance|privacy|compliance)\b",
            "\u0623\u0645\u0627\u0646",
            "\u062d\u0645\u0627\u064a\u0629",
            "\u0633\u0631\u0639\u0629",
            "\u0623\u062f\u0627\u0621",
            "\u062e\u0635\u0648\u0635\u064a\u0629",
        ):
            return "security_performance"
        if section_type == "core" and coverage_role == "custom_domain_topic" and has(
            r"\b(technology|technical|software|integration|automation|platform|tools?)\b",
            "\u062a\u0642\u0646\u064a",
            "\u062a\u0642\u0646\u064a\u0627\u062a",
            "\u062a\u0643\u0627\u0645\u0644",
            "\u0645\u0646\u0635\u0629",
            "\u0628\u0631\u0646\u0627\u0645\u062c",
            "\u0623\u062f\u0648\u0627\u062a",
        ):
            return "technology_or_capability"
        if section_type == "core" and coverage_role == "custom_domain_topic" and has(
            r"\b(roi|return|conversion|growth|revenue|business impact|value)\b",
            "\u0639\u0627\u0626\u062f",
            "\u0642\u064a\u0645\u0629",
            "\u0646\u0645\u0648",
            "\u062a\u062d\u0648\u064a\u0644",
            "\u0645\u0628\u064a\u0639\u0627\u062a",
        ):
            return "business_impact"
        if section_type in {"offer", "services", "service", "product"} or coverage_role == "offer_clarity":
            return "service_explanation"
        if section_type in {"differentiation", "differentiators", "why_choose_us", "brand_support", "brand"} or coverage_role == "differentiators" or has(
            r"\b(why choose|different|differentiator|advantage|advantages)\b",
            "\u064a\u0645\u064a\u0632",
            "\u0644\u0645\u0627\u0630\u0627",
            "\u0627\u0644\u0645\u0645\u064a\u0632",
            "\u0627\u0644\u062a\u0645\u064a\u0632",
        ):
            return "brand_differentiator" if self._section_visibly_references_brand(section, state) else "features_included"
        if section_type in {"features", "included", "key_features", "core_or_benefits"} or coverage_role == "features_or_included" or has(
            r"\b(features?|included|includes|capabilities|benefits)\b",
            "\u0645\u0632\u0627\u064a\u0627",
            "\u0645\u0645\u064a\u0632\u0627\u062a",
            "\u064a\u0634\u0645\u0644",
            "\u0627\u0644\u0645\u0636\u0645\u0646",
        ):
            return "features_included"
        if section_type in {"offer", "services", "service", "product"} or coverage_role == "offer_clarity" or has(
            r"\b(service|services|offer|product|solution|solutions|what is)\b",
            "\u062e\u062f\u0645\u0629",
            "\u062e\u062f\u0645\u0627\u062a",
            "\u062d\u0644\u0648\u0644",
            "\u0645\u0627 \u0647\u064a",
        ):
            return "service_explanation"
        if self._section_visibly_references_brand(section, state):
            return "brand_differentiator"
        return "informational"

    def _apply_commercial_section_role(
        self,
        section: Dict[str, Any],
        state: Dict[str, Any],
        index: int,
        total: int,
        role_override: Optional[str] = None,
    ) -> None:
        """Persist commercial funnel role and compatible legacy coverage_role."""
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return
        role = str(
            role_override
            or self._commercial_section_role_for_section(section, state, index=index, total=total)
        ).lower()
        section["commercial_section_role"] = role
        role_to_coverage = {
            "intro": "introduction",
            "service_explanation": "offer_clarity",
            "features_included": "features_or_included",
            "brand_differentiator": "differentiators",
            "proof": "proof",
            "comparison": "comparison",
            "process": "process_or_how",
            "faq": "faq",
            "cta": "conclusion",
            "cost_value": "pricing",
            "evaluation_criteria": "custom_domain_topic",
            "security_performance": "custom_domain_topic",
            "technology_or_capability": "custom_domain_topic",
            "business_impact": "custom_domain_topic",
        }
        if role in role_to_coverage:
            section["coverage_role"] = role_to_coverage[role]
        section["brand_usage_policy"] = self._brand_usage_policy_for_section(section, state)
        section["section_intent_snapshot"] = self._build_section_intent_snapshot(section, state)

    def _commercial_role_for_rewritten_heading(
        self,
        section: Dict[str, Any],
        state: Dict[str, Any],
        heading: str,
        index: int = 0,
        total: int = 1,
    ) -> str:
        """Classify a rewritten heading without letting stale role metadata win."""
        structural_type = str(section.get("section_type") or "").lower()
        if structural_type in {"introduction", "intro"} or str(section.get("heading_level") or "").upper() == "INTRO":
            return "intro"
        if structural_type == "faq":
            return "faq"
        if structural_type == "conclusion":
            return "cta"

        probe = dict(section)
        probe["heading_text"] = str(heading or "").strip()
        probe["section_type"] = "core"
        probe["coverage_role"] = "custom_domain_topic"
        for key in (
            "commercial_section_role",
            "section_intent_snapshot",
            "section_contract",
            "brand_usage_policy",
            "_visible_brand_reference",
        ):
            probe.pop(key, None)

        heading_blob = probe["heading_text"].casefold()
        if re.search(
            r"\b(projects?|portfolio|case stud(?:y|ies)|client examples?)\b|"
            r"\u0645\u0634\u0627\u0631\u064a\u0639|\u0646\u0645\u0627\u0630\u062c|\u0633\u0627\u0628\u0642\u0629 \u0623\u0639\u0645\u0627\u0644",
            heading_blob,
            re.IGNORECASE,
        ):
            probe["section_type"] = "proof"
            probe["taxonomy_axis"] = "brand_projects"
        else:
            probe["taxonomy_axis"] = self._generic_taxonomy_axis_for_section(probe)

        return self._commercial_section_role_for_section(
            probe,
            state,
            index=index,
            total=total,
        )

    def _section_type_for_commercial_role(self, role: str) -> str:
        """Return compatible structural metadata for a commercial role."""
        return {
            "intro": "introduction",
            "service_explanation": "offer",
            "features_included": "features",
            "brand_differentiator": "differentiation",
            "proof": "proof",
            "comparison": "comparison",
            "process": "process",
            "cost_value": "pricing",
            "faq": "faq",
            "cta": "conclusion",
        }.get(str(role or "").lower(), "core")

    def _taxonomy_axis_for_commercial_role(self, role: str) -> str:
        """Return a neutral taxonomy axis before brand-policy enforcement."""
        return {
            "service_explanation": "category_or_type",
            "features_included": "features",
            "brand_differentiator": "brand_support",
            "proof": "brand_projects",
            "comparison": "comparison",
            "process": "process",
            "cost_value": "pricing",
            "faq": "faq",
            "cta": "conclusion",
            "evaluation_criteria": "criteria",
            "security_performance": "criteria",
            "technology_or_capability": "criteria",
            "business_impact": "criteria",
        }.get(str(role or "").lower(), "criteria")

    def _sync_heading_role_contract(
        self,
        section: Dict[str, Any],
        state: Dict[str, Any],
        old_heading: str,
        *,
        outline: Optional[List[Dict[str, Any]]] = None,
        index: Optional[int] = None,
        existing_content: str = "",
        force: bool = False,
    ) -> Dict[str, Any]:
        """Atomically align heading-derived role, snapshot, and writer contract."""
        if str(state.get("content_type") or section.get("content_type") or "").lower() != "brand_commercial":
            return section.get("heading_contract_sync", {})
        new_heading = str(section.get("heading_text") or "").strip()
        old_heading = str(old_heading or "").strip()
        active_outline = outline if isinstance(outline, list) and outline else state.get("outline", [])
        if not isinstance(active_outline, list) or not active_outline:
            active_outline = [section]

        if index is None:
            index = 0
            section_id = section.get("section_id") or section.get("id")
            for candidate_index, candidate in enumerate(active_outline):
                if candidate is section or (
                    section_id
                    and (candidate.get("section_id") or candidate.get("id")) == section_id
                ):
                    index = candidate_index
                    break
        total = max(len(active_outline), 1)

        current_role = str(section.get("commercial_section_role") or "").lower()
        old_role = current_role or self._commercial_role_for_rewritten_heading(
            section,
            state,
            old_heading or new_heading,
            index=index,
            total=total,
        )
        new_role = self._commercial_role_for_rewritten_heading(
            section,
            state,
            new_heading,
            index=index,
            total=total,
        )
        heading_changed = old_heading != new_heading
        role_changed = old_role != new_role

        contract_heading = str((section.get("section_contract") or {}).get("source_heading") or "").strip()
        snapshot_heading = str((section.get("section_intent_snapshot") or {}).get("source_heading") or "").strip()
        metadata_stale = bool(
            (contract_heading and contract_heading != new_heading)
            or (snapshot_heading and snapshot_heading != new_heading)
            or (current_role and current_role != new_role)
        )
        if not (heading_changed or role_changed or metadata_stale or force):
            return section.get("heading_contract_sync", {})

        if role_changed:
            section["section_type"] = self._section_type_for_commercial_role(new_role)
            section["taxonomy_axis"] = self._taxonomy_axis_for_commercial_role(new_role)

        for key in (
            "section_contract",
            "section_intent_snapshot",
            "section_promise",
            "reader_takeaway",
            "depth_goal",
            "practical_decision_value",
            "semantic_goal",
            "decision_frame",
            "content_behavior",
            "execution_mode",
            "preferred_axis",
            "forbidden_taxonomy_axis",
            "must_include_details",
            "must_not_repeat",
        ):
            section.pop(key, None)

        if role_changed and str(section.get("table_type") or "").startswith("project") and new_role != "proof":
            section["requires_table"] = False
            section["table_type"] = "none"

        self._apply_commercial_section_role(
            section,
            state,
            index,
            total,
            role_override=new_role,
        )
        section["section_contract"] = self._build_section_contract(
            section,
            active_outline,
            index,
            state,
        )
        self._enrich_section_contract(section, active_outline, index, state)
        self._enforce_commercial_role_contract(section, state)
        section["section_intent_snapshot"] = self._build_section_intent_snapshot(section, state)

        body_rewrite_required = False
        if str(existing_content or "").strip() and (heading_changed or role_changed):
            role_report = self._evaluate_section_role_fulfillment(
                section,
                existing_content,
                state,
            )
            body_rewrite_required = bool(
                role_changed
                or role_report.get("fulfillment_status") in {"weak", "unsupported"}
            )

        sync_report = {
            "old_heading": old_heading,
            "new_heading": new_heading,
            "old_role": old_role,
            "new_role": new_role,
            "body_rewrite_required": body_rewrite_required,
        }
        section["heading_contract_sync"] = sync_report
        if body_rewrite_required:
            self._record_section_quality_issue(section, "heading_contract_body_rewrite_required")

        logger.info(
            "[heading_contract_sync] old_heading=%r new_heading=%r old_role=%s new_role=%s body_rewrite_required=%s",
            old_heading,
            new_heading,
            old_role,
            new_role,
            str(body_rewrite_required).lower(),
        )
        return sync_report

    def _ensure_heading_role_contract_current(
        self,
        section: Dict[str, Any],
        state: Dict[str, Any],
        outline: Optional[List[Dict[str, Any]]] = None,
        index: Optional[int] = None,
    ) -> None:
        """Refresh heading-derived metadata if any writer-facing layer is stale."""
        if str(state.get("content_type") or section.get("content_type") or "").lower() != "brand_commercial":
            return
        heading = str(section.get("heading_text") or "").strip()
        contract_heading = str((section.get("section_contract") or {}).get("source_heading") or "").strip()
        snapshot_heading = str((section.get("section_intent_snapshot") or {}).get("source_heading") or "").strip()
        fresh_role = self._commercial_role_for_rewritten_heading(
            section,
            state,
            heading,
            index=index or 0,
            total=max(len(outline or state.get("outline", []) or [section]), 1),
        )
        current_role = str(section.get("commercial_section_role") or "").lower()
        if (
            contract_heading != heading
            or snapshot_heading != heading
            or current_role != fresh_role
        ):
            self._sync_heading_role_contract(
                section,
                state,
                contract_heading or snapshot_heading or heading,
                outline=outline,
                index=index,
                force=True,
            )

    def _enforce_commercial_role_contract(self, section: Dict[str, Any], state: Dict[str, Any]) -> None:
        """Keep legacy brand_policy/taxonomy fields aligned with the section role."""
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return
        role = str(section.get("commercial_section_role") or self._commercial_section_role_for_section(section, state)).lower()
        visible_brand = self._section_visibly_references_brand(section, state)
        inventory = self._brand_evidence_inventory_for_outline(state)
        contract = section.get("section_contract") if isinstance(section.get("section_contract"), dict) else {}

        neutral_roles = {
            "informational", "comparison", "cost_value", "faq",
            "evaluation_criteria", "security_performance",
            "technology_or_capability", "business_impact",
        }
        light_roles = {"service_explanation", "features_included"}
        brand_owned_roles = {"brand_differentiator", "proof"}

        if role in neutral_roles and not visible_brand:
            section["brand_policy"] = "none"
            if role == "faq":
                section["taxonomy_axis"] = "faq"
            elif role == "comparison":
                section["taxonomy_axis"] = "comparison"
            elif role == "cost_value":
                section["taxonomy_axis"] = "pricing"
            elif str(section.get("taxonomy_axis") or "").startswith("brand_"):
                section["taxonomy_axis"] = self._generic_taxonomy_axis_for_section(section)
            contract["brand_policy"] = "none"
            contract["taxonomy_axis"] = section.get("taxonomy_axis") or self._generic_taxonomy_axis_for_section(section)
        elif role in light_roles and not visible_brand:
            section["brand_policy"] = "none"
            if str(section.get("taxonomy_axis") or "").startswith("brand_"):
                section["taxonomy_axis"] = self._generic_taxonomy_axis_for_section(section)
            contract["brand_policy"] = "none"
            contract["taxonomy_axis"] = section.get("taxonomy_axis") or self._generic_taxonomy_axis_for_section(section)
            section["execution_mode"] = "" if section.get("execution_mode") == "brand_service_catalog" else section.get("execution_mode", "")
        elif role == "process" and not visible_brand:
            section["brand_policy"] = "none"
            section["taxonomy_axis"] = "process"
            contract["brand_policy"] = "none"
            contract["taxonomy_axis"] = "process"
        elif role == "proof" and not (inventory.get("projects_available") or inventory.get("trust_available")):
            section["brand_policy"] = "none"
            section["taxonomy_axis"] = "criteria"
            contract["brand_policy"] = "none"
            contract["taxonomy_axis"] = "criteria"
        elif role in brand_owned_roles or role in {"intro", "cta"} or (visible_brand and role not in neutral_roles):
            section["brand_policy"] = "commercial"
            contract["brand_policy"] = "commercial"
            if role == "proof":
                section["taxonomy_axis"] = "brand_projects"
            elif role == "process":
                section["taxonomy_axis"] = "brand_process"
            elif role in {"service_explanation", "features_included"}:
                section["taxonomy_axis"] = "brand_offer"
            contract["taxonomy_axis"] = section.get("taxonomy_axis") or contract.get("taxonomy_axis")

        if contract:
            section["section_contract"] = contract

    def _brand_usage_policy_for_section(self, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        """Control how much the writer should use the brand in each section."""
        if state.get("brand_evidence_failure_mode"):
            return "no_brand_facts"
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return "neutral_market"

        section_type = str(section.get("section_type") or "").lower()
        heading_level = str(section.get("heading_level") or "").upper()
        coverage_role = str(section.get("coverage_role") or "").lower()
        commercial_role = str(section.get("commercial_section_role") or "").lower()
        if not commercial_role:
            commercial_role = self._commercial_section_role_for_section(section, state)
        heading_references_brand = self._section_visibly_references_brand(section, state)

        if commercial_role == "intro":
            return "soft_intro_brand"
        if commercial_role == "cta":
            return "brand_cta"
        if commercial_role in {
            "informational", "comparison", "cost_value", "faq",
            "evaluation_criteria", "security_performance",
            "technology_or_capability", "business_impact",
        } and not heading_references_brand:
            return "neutral_market"
        if commercial_role in {"service_explanation", "features_included"}:
            return "brand_light"
        if commercial_role == "process":
            return "brand_owned" if heading_references_brand else "neutral_market"
        if commercial_role in {"brand_differentiator", "proof"}:
            return "brand_owned"

        if section_type in {"introduction", "intro"} or heading_level == "INTRO":
            return "soft_intro_brand"
        if section_type == "conclusion" or coverage_role == "conclusion":
            return "brand_cta"
        if section_type in {"faq", "comparison", "pricing", "packages", "location"}:
            return "brand_owned" if heading_references_brand else "neutral_market"
        if section_type in {"proof", "case_study", "case-study"} or coverage_role == "proof":
            return "brand_owned"
        if section_type in {"differentiation", "differentiators", "why_choose_us"} or coverage_role == "differentiators":
            return "brand_owned"
        if section_type in {"process", "process_or_how", "how_it_works"} or coverage_role == "process_or_how":
            return "brand_owned" if heading_references_brand else "brand_light"
        if section_type in {"offer", "services", "features", "included", "key_features", "core_or_benefits"}:
            return "brand_light"
        if heading_references_brand:
            return "brand_owned"
        return "neutral_market"

    def _infer_brand_policy(self, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        brand_name = state.get("brand_name") or state.get("display_brand_name") or ""
        if not brand_name:
            return "none"
        if state.get("brand_evidence_failure_mode"):
            return "none"

        content_type = (state.get("content_type") or "").lower()
        intent = (state.get("intent") or "").lower()
        section_type = (section.get("section_type") or "").lower()
        if content_type == "brand_commercial":
            if (
                section_type in {"introduction", "conclusion"}
                or self._section_visibly_references_brand(section, state)
                or self._section_role_should_use_brand_evidence(section, state)
            ):
                return "commercial"
            return "none"
        if "commercial" in intent and self._section_visibly_references_brand(section, state):
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
            "source_heading": heading,
        }
        section["_visible_brand_reference"] = self._section_visibly_references_brand(section, state)
        return contract

    def _infer_taxonomy_axis(self, section: Dict[str, Any]) -> str:
        """Infer a broad editorial axis without making topic-specific assumptions."""
        section_type = str(section.get("section_type") or "").lower()
        heading_blob = " ".join([
            str(section.get("heading_text") or ""),
            " ".join(self._subheading_text(item) for item in section.get("subheadings", []) or []),
        ]).lower()
        clean_comparison_signal = bool(
            re.search(
                r"\b(compare|comparison|versus|difference between| vs )\b|"
                r"\u0645\u0642\u0627\u0631\u0646|\u0642\u0627\u0631\u0646|\u062a\u0642\u0627\u0631\u0646|"
                r"\u0627\u0644\u0641\u0631\u0642 \u0628\u064a\u0646|\u0645\u0642\u0627\u0628\u0644",
                heading_blob,
                re.IGNORECASE,
            )
        )
        clean_process_signal = bool(
            re.search(
                r"\b(process|steps|stages|workflow|how it works|how does .+ work)\b|"
                r"\u062e\u0637\u0648\u0627\u062a|\u0645\u0631\u0627\u062d\u0644|"
                r"\u0633\u064a\u0631 \u0627\u0644\u0639\u0645\u0644|\u0622\u0644\u064a\u0629 \u0627\u0644\u0639\u0645\u0644|"
                r"\u0637\u0631\u064a\u0642\u0629 \u0627\u0644\u0639\u0645\u0644|"
                r"\u0643\u064a\u0641 (?:\u064a\u0639\u0645\u0644|\u062a\u0639\u0645\u0644|\u064a\u062a\u0645|\u062a\u062a\u0645)",
                heading_blob,
                re.IGNORECASE,
            )
        )
        clean_evaluation_signal = bool(
            re.search(
                r"\b(criteria|evaluate|evaluation|choose|choosing|checklist)\b|"
                r"\u0645\u0639\u0627\u064a\u064a\u0631|\u062a\u0642\u064a\u064a\u0645|"
                r"\u0627\u062e\u062a\u064a\u0627\u0631|\u062a\u062e\u062a\u0627\u0631",
                heading_blob,
                re.IGNORECASE,
            )
        )

        if section_type == "faq":
            return "faq"
        if section_type == "conclusion":
            return "conclusion"
        brand_policy = str((section.get("section_contract") or {}).get("brand_policy") or section.get("brand_policy") or "").lower()
        is_brand_commercial = (
            str(section.get("content_type") or "").lower() == "brand_commercial"
            and (brand_policy == "commercial" or bool(section.get("_visible_brand_reference")))
        )
        if is_brand_commercial:
            if section_type in {"proof", "case_study"} or any(
                term in heading_blob
                for term in ("مشاريع", "نماذج", "أعمال", "سابقة", "portfolio", "projects", "case studies")
            ):
                return "brand_projects"
            if section_type in {"process", "process_or_how"} or any(
                term in heading_blob for term in ("خطوات", "مراحل", "تنفيذ", "طلب", "process", "steps")
            ):
                return "brand_process"
            if section_type in {"offer", "core_or_benefits", "services"} or any(
                term in heading_blob for term in ("خدمات", "الخدمات", "حلول", "options", "services", "offer")
            ):
                return "brand_offer"
            if section_type in {"features", "differentiation", "differentiators", "brand_support", "brand"}:
                return "brand_features" if section_type == "features" else "brand_support"
        if section_type == "comparison" or clean_comparison_signal:
            return "comparison"
        if section_type in {"process", "process_or_how", "how_it_works"} or clean_process_signal:
            return "process"
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
        if clean_evaluation_signal:
            return "criteria"
        if re.search(r"\bhow\b|\u0643\u064a\u0641", heading_blob, re.IGNORECASE):
            return "criteria"
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
            "brand_offer": [
                self._enrichment_text(state, "اشرح الخدمات أو الحلول التي يقدمها البراند فعليًا من أدلة الموقع المرصودة.", "Explain the services or solutions the brand actually provides from observed website evidence."),
                self._enrichment_text(state, "اكتب تحت كل خدمة بوصفها خدمة مقدمة من البراند، لا كمعيار لاختيار أي شركة.", "Write under each service as a brand-provided service, not as criteria for choosing any company."),
                self._enrichment_text(state, "استخدم أسماء الخدمات والقدرات المرصودة في صفحات البراند نفسها فقط، ولا تستعير أمثلة من صناعات أو مقالات أخرى.", "Use only the service/capability names observed on the brand pages themselves; do not borrow examples from other industries or prior articles."),
            ],
            "brand_features": [
                self._enrichment_text(state, "حوّل المميزات إلى قدرات تقنية وخدمات مرصودة لدى البراند، وليس نصائح عامة للبحث في السوق.", "Turn features into observed brand capabilities and services, not generic market-search advice."),
                self._enrichment_text(state, "اذكر التقنيات أو الأنظمة المرصودة بأسمائها عند توفرها.", "Mention observed technologies or systems by name when available."),
                self._enrichment_text(state, "تجنب صيغ مثل تأكد أو اسأل أو اختر؛ اشرح ما يتوفر فعليًا لدى البراند.", "Avoid phrasing like make sure, ask, or choose; explain what the brand actually provides."),
            ],
            "brand_projects": [
                self._enrichment_text(state, "اذكر أسماء المشاريع أو العملاء أو دراسات الحالة المرصودة صراحة في أدلة البراند.", "Mention the observed project, client, or case-study names explicitly found in brand evidence."),
                self._enrichment_text(state, "لا تحوّل هذا السكشن إلى معايير عامة لتقييم المشاريع.", "Do not turn this section into generic project-evaluation criteria."),
                self._enrichment_text(state, "إذا كان الموقع الجغرافي غير مدعوم، لا تربط المشاريع بالسعودية.", "If geography is unsupported, do not tie projects to Saudi Arabia."),
            ],
            "brand_process": [
                self._enrichment_text(state, "استخدم مراحل العمل المرصودة لدى البراند مثل Consultation & Planning وDesign & Development وExecution & Delivery عند توفرها.", "Use observed brand workflow stages such as Consultation & Planning, Design & Development, and Execution & Delivery when available."),
                self._enrichment_text(state, "اكتب العملية كطريقة تعامل مع البراند، لا كقائمة نصائح عامة لاختيار مزود خدمة.", "Write the process as the way to work with the brand, not as generic advice for selecting a provider."),
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

        brand_name = state.get("display_brand_name") or state.get("brand_name") or "the brand"
        brand_axis_defaults = {
            "brand_offer": {
                "section_promise": self._enrichment_text(
                    state,
                    f"عرض الخدمات والحلول التي يقدمها {brand_name} فعليًا والمرتبطة بعنوان السكشن.",
                    f"Show the actual services and solutions {brand_name} provides that match this section.",
                ),
                "reader_takeaway": self._enrichment_text(
                    state,
                    f"يفهم القارئ ما الذي يقدمه {brand_name} كخدمات محددة، لا مجرد معايير لاختيار أي شركة.",
                    f"The reader understands what {brand_name} specifically offers, not generic provider-selection criteria.",
                ),
                "depth_goal": self._enrichment_text(
                    state,
                    "حوّل كل H3 إلى شرح خدمة فعلية مدعومة بأدلة البراند.",
                    "Turn each H3 into a brand-provided service explanation grounded in brand evidence.",
                ),
                "practical_decision_value": self._enrichment_text(
                    state,
                    "يساعد السكشن القارئ على معرفة مدى ملاءمة خدمات البراند لاحتياجه.",
                    "This section helps the reader judge how the brand's services fit their need.",
                ),
            },
            "brand_features": {
                "section_promise": self._enrichment_text(
                    state,
                    f"شرح القدرات والمميزات التقنية المرصودة لدى {brand_name}.",
                    f"Explain the observed technical capabilities and features available from {brand_name}.",
                ),
                "reader_takeaway": self._enrichment_text(
                    state,
                    "يفهم القارئ القدرات التقنية المتوفرة لدى البراند بأسماء واضحة، لا نصائح عامة.",
                    "The reader understands the brand's concrete capabilities by name, not generic advice.",
                ),
                "depth_goal": self._enrichment_text(
                    state,
                    "استخدم أسماء الأدوات والأنظمة والخدمات المرصودة بدل عبارات تقنية عامة.",
                    "Use observed tools, systems, and service names instead of broad technical phrasing.",
                ),
            },
            "brand_projects": {
                "section_promise": self._enrichment_text(
                    state,
                    f"عرض أمثلة مشاريع أو عملاء مرصودة لدى {brand_name} عند توفرها.",
                    f"Show observed projects or client examples from {brand_name} when available.",
                ),
                "reader_takeaway": self._enrichment_text(
                    state,
                    "يرى القارئ أمثلة فعلية من الأدلة بدل قائمة معايير عامة.",
                    "The reader sees actual evidence examples instead of generic evaluation criteria.",
                ),
                "depth_goal": self._enrichment_text(
                    state,
                    "اذكر أسماء المشاريع المرصودة واربط كل مثال بما يوضحه عن قدرة البراند.",
                    "Mention observed project names and tie each example to what it demonstrates about the brand.",
                ),
            },
            "brand_process": {
                "section_promise": self._enrichment_text(
                    state,
                    f"شرح طريقة طلب وتنفيذ المشروع مع {brand_name} باستخدام مراحل العمل المرصودة.",
                    f"Explain how to request and execute a project with {brand_name} using observed workflow stages.",
                ),
                "reader_takeaway": self._enrichment_text(
                    state,
                    "يفهم القارئ خطوات التعامل مع البراند بترتيب عملي واضح.",
                    "The reader understands the practical sequence for working with the brand.",
                ),
            },
            "brand_support": {
                "section_promise": self._enrichment_text(
                    state,
                    f"توضيح ما يميز {brand_name} اعتمادًا على خدمات وتقنيات ومشاريع مرصودة.",
                    f"Explain what differentiates {brand_name} using observed services, technologies, and projects.",
                ),
                "reader_takeaway": self._enrichment_text(
                    state,
                    "يفهم القارئ أسباب الملاءمة من أدلة ملموسة، لا مدح عام.",
                    "The reader understands fit through concrete evidence, not generic praise.",
                ),
            },
        }
        if taxonomy_axis in brand_axis_defaults:
            defaults.update(brand_axis_defaults[taxonomy_axis])

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

        brand_mode_map = {
            "brand_offer": (
                "brand_service_catalog",
                "brand service clarity",
                "matching observed services to the reader's need",
                "Describe actual brand-provided services and capabilities from evidence; do not write generic provider-selection advice.",
            ),
            "brand_features": (
                "brand_evidence_application",
                "observed brand capabilities",
                "matching technical evidence to practical benefits",
                "Explain observed brand capabilities, tools, and systems by name; avoid generic criteria wording.",
            ),
            "brand_projects": (
                "brand_project_examples",
                "observed project proof",
                "using named projects or case evidence",
                "Use actual observed project/client names and snippets; do not substitute generic project-evaluation advice.",
            ),
            "brand_process": (
                "brand_process_delivery",
                "observed delivery workflow",
                "how the reader works with the brand",
                "Explain the brand's observed workflow stages as a practical collaboration path.",
            ),
            "brand_support": (
                "brand_evidence_application",
                "evidence-backed brand fit",
                "why the brand fits this need",
                "Ground differentiation in observed services, technologies, workflow, or projects.",
            ),
        }
        if taxonomy_axis in brand_mode_map:
            mode, semantic_goal, decision_frame, behavior = brand_mode_map[taxonomy_axis]
            section["execution_mode"] = mode
            section["semantic_goal"] = semantic_goal
            section["decision_frame"] = decision_frame
            section["content_behavior"] = behavior

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

        state = await self._run_post_outline_brand_targeted_crawl(state, outline)
        state = self._prepare_outline_for_content(state, outline, source="approved_outline")
        if state.get("workflow_logger"):
            state["workflow_logger"].log_event("approved_outline_load", {
                "sections": len(state.get("outline", [])),
                "content_only_mode": True,
            })
        return state

    def _fulfill_and_downgrade_heading(self, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        """Downgrade brand-owned headings that inventory says cannot be fulfilled."""
        heading = str(section.get("heading_text") or "").strip()
        heading_lower = heading.lower()
        lang = (state.get("article_language") or "ar").lower()
        brand_name = str(state.get("brand_name") or state.get("display_brand_name") or "").strip()
        brand_aliases = [str(a).strip() for a in (state.get("brand_aliases") or []) if str(a).strip()]
        brand_terms = [brand_name.lower()] + [alias.lower() for alias in brand_aliases]
        mentions_brand = any(term and term in heading_lower for term in brand_terms)
        content_type = str(state.get("content_type") or "").lower()
        role_requires_brand_evidence = self._section_role_should_use_brand_evidence(section, state)
        is_brand_owned = content_type == "brand_commercial" and (mentions_brand or role_requires_brand_evidence)
        inventory = self._brand_evidence_inventory_for_outline(state)
        section_type = str(section.get("section_type") or "").lower()

        def _generic_no_brand_evidence_heading() -> str:
            topic = state.get("primary_keyword") or state.get("raw_title") or (
                "\u0627\u0644\u062e\u064a\u0627\u0631 \u0627\u0644\u0645\u0646\u0627\u0633\u0628"
                if lang.startswith("ar")
                else "the available options"
            )
            process_terms = [
                "\u062e\u0637\u0648\u0627\u062a", "\u0645\u0631\u0627\u062d\u0644", "\u062f\u0644\u064a\u0644", "process", "steps", "how"
            ]
            service_terms = [
                "\u062e\u062f\u0645\u0627\u062a", "\u0627\u0644\u062e\u062f\u0645\u0627\u062a", "\u0646\u0637\u0627\u0642", "services", "service scope"
            ]
            if section_type in {"process", "process_or_how"} or any(term in heading_lower for term in process_terms):
                return (
                    f"\u062f\u0644\u064a\u0644 \u0627\u0644\u062e\u0637\u0648\u0627\u062a \u0627\u0644\u0639\u0645\u0644\u064a\u0629 \u0644\u0640 {topic}"
                    if lang.startswith("ar")
                    else f"Practical Steps for {topic}"
                )
            if section_type in {"offer", "services", "core_or_benefits"} or any(term in heading_lower for term in service_terms):
                return (
                    f"\u0645\u0639\u0627\u064a\u064a\u0631 \u062a\u0642\u064a\u064a\u0645 \u0627\u0644\u062e\u064a\u0627\u0631\u0627\u062a \u0627\u0644\u0645\u062a\u0627\u062d\u0629 \u0644\u0640 {topic}"
                    if lang.startswith("ar")
                    else f"How to Evaluate Available Options for {topic}"
                )

            cleaned = heading
            for term in [brand_name, *brand_aliases]:
                if not term:
                    continue
                cleaned = re.sub(r"\s+(?:by|via|through|from|with|at)\s+" + re.escape(term) + r"\b", "", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"\s*(?:\u0644\u062f\u0649|\u0639\u0628\u0631|\u0645\u0646 \u062e\u0644\u0627\u0644|\u0645\u0646|\u0639\u0646)\s*" + re.escape(term) + r"\b", "", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(re.escape(term), "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s+", " ", cleaned).strip(" -:\u2013\u2014")
            return cleaned or (
                f"\u0645\u0639\u0627\u064a\u064a\u0631 \u062a\u0642\u064a\u064a\u0645 \u0627\u0644\u062e\u064a\u0627\u0631\u0627\u062a \u0627\u0644\u0645\u062a\u0627\u062d\u0629 \u0644\u0640 {topic}"
                if lang.startswith("ar")
                else f"How to Evaluate Available Options for {topic}"
            )

        if state.get("brand_evidence_failure_mode") and mentions_brand:
            section["fulfillment_status"] = "unsupported"
            section["fulfillment_reason"] = "brand heading without usable crawled brand page text"
            section["brand_policy"] = "none"
            if isinstance(section.get("section_contract"), dict):
                section["section_contract"]["brand_policy"] = "none"
                if str(section["section_contract"].get("taxonomy_axis") or "").lower().startswith("brand_"):
                    section["section_contract"]["taxonomy_axis"] = self._generic_taxonomy_axis_for_section(section)
            downgraded = _generic_no_brand_evidence_heading()
            if downgraded != heading:
                logger.warning(
                    "[brand_evidence_failure_mode] Downgrading brand heading without usable evidence '%s' -> '%s'",
                    heading,
                    downgraded,
                )
            return downgraded

        def _cards_have(key: str) -> bool:
            for card in state.get("brand_evidence_cards", []) or []:
                if isinstance(card, dict) and not card.get("excluded_reason") and card.get(key):
                    return True
            return False

        def _available(inventory_key: str, card_key: str) -> bool:
            return bool(inventory.get(inventory_key)) or _cards_have(card_key)

        def _safe_brand_operational_heading() -> str:
            return (
                f"نطاق الخدمات المتاحة لدى {brand_name or 'البراند'} حسب احتياج المشروع"
                if lang.startswith("ar")
                else f"Service Scope Available From {brand_name or 'the Brand'}"
            )

        pricing_terms = [
            "باقات", "باقة", "أسعار", "اسعار", "تكلفة",
            "باقات", "باقة", "أسعار", "اسعار", "تكلفة",
            "pricing", "packages", "package", "plans", "cost",
        ]
        if is_brand_owned and any(term in heading_lower for term in pricing_terms) and not _available("pricing_available", "visible_pricing_or_packages"):
            section["fulfillment_status"] = "unsupported"
            section["fulfillment_reason"] = "brand pricing/packages heading without explicit brand pricing/package evidence"
            section["subheadings"] = []
            downgraded = _safe_brand_operational_heading()
            logger.warning("[brand_fulfillment] Downgrading unsupported brand pricing/packages heading '%s' -> '%s'", heading, downgraded)
            return downgraded

        explicit_geo = [str(item).casefold() for item in inventory.get("explicit_geography", [])]
        area = str(state.get("area") or "").strip()
        geo_candidates = [area] if area else []
        english_geo_tail = re.search(r"\b(?:in|across|within)\s+([A-Z][A-Za-z\s.'-]{2,80})$", heading)
        if english_geo_tail:
            geo_candidates.append(english_geo_tail.group(1).strip())
        unsupported_geo = [
            candidate for candidate in geo_candidates
            if candidate and candidate.casefold() not in explicit_geo
        ]
        if is_brand_owned and unsupported_geo and not inventory.get("explicit_geography"):
            cleaned = heading
            for candidate in unsupported_geo:
                cleaned = re.sub(r"\s+(?:in|across|within)\s+" + re.escape(candidate) + r"\b", "", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"\s+(?:\u0628\u0627\u0644|\u0628\u0640?|\u0628)\s*" + re.escape(candidate) + r"\b", "", cleaned, flags=re.IGNORECASE)
                if candidate.startswith("\u0627\u0644") and len(candidate) > 2:
                    bare_candidate = candidate[2:]
                    cleaned = re.sub(r"\s+(?:\u0628\u0627\u0644|\u0628)\s*" + re.escape(bare_candidate) + r"\b", "", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"\s+(?:في|داخل|عبر)\s*" + re.escape(candidate) + r"\b", "", cleaned, flags=re.IGNORECASE)
                # Arabic word boundaries are unreliable for prefixed forms such
                # as "بالسعودية"; run a punctuation-aware pass as well.
                candidate_pattern = re.escape(candidate)
                location_tail = r"(?=$|[\s:،,؛.!?])"
                cleaned = re.sub(r"\s+(?:in|across|within)\s+" + candidate_pattern + location_tail, "", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"\s+(?:\u0641\u064a|\u062f\u0627\u062e\u0644|\u0639\u0628\u0631)\s*" + candidate_pattern + location_tail, "", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"\s+(?:\u0628\u0627\u0644|\u0628\u0640?|\u0628)\s*" + candidate_pattern + location_tail, "", cleaned, flags=re.IGNORECASE)
                if candidate.startswith("\u0627\u0644") and len(candidate) > 2:
                    bare_candidate = candidate[2:]
                    cleaned = re.sub(r"\s+(?:\u0628\u0627\u0644|\u0628)\s*" + re.escape(bare_candidate) + location_tail, "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s+", " ", cleaned).strip(" -–—:")
            if cleaned and cleaned != heading:
                section["fulfillment_status"] = "weak"
                section["fulfillment_reason"] = "removed unsupported brand geography claim from heading"
                logger.info("[brand_fulfillment] Removed unsupported geography from brand heading '%s' -> '%s'", heading, cleaned)
                heading = cleaned
                heading_lower = heading.lower()

        project_terms = [
            "مشاريع", "سابقة أعمال", "أمثلة من أعمال",
            "أمثلة من مشاريع", "مشاريعنا", "سابقة أعمالنا",
            "مشاريع", "نماذج", "أعمال", "سابقة أعمال",
            "projects", "case studies", "portfolio", "our work", "our projects",
        ]
        if any(term in heading_lower for term in project_terms) and not _available("projects_available", "visible_project_or_case_study_examples"):
            logger.info("[HeadingDowngrader] Downgrading project-promising heading '%s' due to zero project evidence.", heading)
            section["fulfillment_status"] = "unsupported"
            section["fulfillment_reason"] = "project/case-study heading without observed project evidence"
            section["subheadings"] = []
            if lang == "ar":
                topic = state.get("primary_keyword") or state.get("raw_title") or "الخيار المناسب"
                return f"معايير اختيار وتقييم الخيارات المتاحة لـ {topic}"
            topic = state.get("primary_keyword") or state.get("raw_title") or "the available options"
            return f"How to Evaluate Available Options for {topic}"

        proof_like = (section.get("section_type") or "").lower() in {"proof", "case_study", "case-study", "differentiation"}
        if (
            is_brand_owned
            and proof_like
            and inventory.get("confidence") == "low"
            and not any(inventory.get(key) for key in ("projects_available", "trust_available", "pricing_available"))
        ):
            section["fulfillment_status"] = "weak"
            section["fulfillment_reason"] = "low-confidence inventory cannot support a dedicated brand-proof heading"
            section["subheadings"] = []
            return _safe_brand_operational_heading()

        return heading

    def _generic_taxonomy_axis_for_section(self, section: Dict[str, Any]) -> str:
        section_type = str(section.get("section_type") or "").lower()
        heading_blob = " ".join([
            str(section.get("heading_text") or ""),
            " ".join(self._subheading_text(item) for item in section.get("subheadings", []) or []),
        ]).lower()
        comparison_signal = bool(
            re.search(
                r"\b(compare|comparison|versus|difference between| vs )\b|"
                r"\u0645\u0642\u0627\u0631\u0646|\u0642\u0627\u0631\u0646|\u062a\u0642\u0627\u0631\u0646|"
                r"\u0627\u0644\u0641\u0631\u0642 \u0628\u064a\u0646|\u0645\u0642\u0627\u0628\u0644",
                heading_blob,
                re.IGNORECASE,
            )
        )
        process_signal = bool(
            re.search(
                r"\b(process|steps|stages|workflow|how it works|how does .+ work)\b|"
                r"\u062e\u0637\u0648\u0627\u062a|\u0645\u0631\u0627\u062d\u0644|"
                r"\u0633\u064a\u0631 \u0627\u0644\u0639\u0645\u0644|\u0622\u0644\u064a\u0629 \u0627\u0644\u0639\u0645\u0644|"
                r"\u0637\u0631\u064a\u0642\u0629 \u0627\u0644\u0639\u0645\u0644|"
                r"\u0643\u064a\u0641 (?:\u064a\u0639\u0645\u0644|\u062a\u0639\u0645\u0644|\u064a\u062a\u0645|\u062a\u062a\u0645)",
                heading_blob,
                re.IGNORECASE,
            )
        )
        evaluation_signal = bool(
            re.search(
                r"\b(criteria|evaluate|evaluation|choose|choosing|checklist)\b|"
                r"\u0645\u0639\u0627\u064a\u064a\u0631|\u062a\u0642\u064a\u064a\u0645|"
                r"\u0627\u062e\u062a\u064a\u0627\u0631|\u062a\u062e\u062a\u0627\u0631",
                heading_blob,
                re.IGNORECASE,
            )
        )
        if section_type == "faq":
            return "faq"
        if section_type == "conclusion":
            return "conclusion"
        if section_type == "comparison" or comparison_signal:
            return "comparison"
        if section_type in {"process", "process_or_how", "how_it_works"} or process_signal:
            return "process"
        if evaluation_signal:
            return "criteria"
        if re.search(r"\bhow\b|\u0643\u064a\u0641", heading_blob, re.IGNORECASE):
            return "criteria"
        if section_type == "process" or any(term in heading_blob for term in ("process", "steps", "how", "خطوات", "مراحل")):
            return "process"
        if section_type == "comparison" or any(term in heading_blob for term in ("comparison", "compare", "versus", " vs ", "مقارنة")):
            return "comparison"
        if section_type == "pricing" or any(term in heading_blob for term in ("pricing", "price", "cost", "budget", "أسعار", "تكلفة")):
            return "pricing"
        if section_type in {"offer", "services"}:
            return "category_or_type"
        return "criteria"

    def _normalize_outline_with_brand_evidence_inventory(
        self,
        outline: List[Dict[str, Any]],
        state: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Post-outline brand evidence gate based on the compact inventory."""
        if (
            str(state.get("content_type") or "").lower() != "brand_commercial"
            and not state.get("brand_evidence_failure_mode")
        ):
            return outline

        normalized: List[Dict[str, Any]] = []
        for section_index, raw_section in enumerate(outline):
            section = raw_section
            section_type = str(section.get("section_type") or "").lower()
            visible_brand = self._section_visibly_references_brand(section, state)
            should_use_brand_evidence = self._section_role_should_use_brand_evidence(section, state)

            if section_type == "faq" and not visible_brand:
                section["brand_policy"] = "none"
                section["taxonomy_axis"] = "faq"
                if isinstance(section.get("section_contract"), dict):
                    section["section_contract"]["brand_policy"] = "none"
                    section["section_contract"]["taxonomy_axis"] = "faq"
                normalized.append(section)
                continue

            if (
                not visible_brand
                and not should_use_brand_evidence
                and section_type not in {"introduction", "intro", "conclusion"}
            ):
                section["brand_policy"] = "none"
                current_axis = str(section.get("taxonomy_axis") or "").lower()
                if current_axis.startswith("brand_"):
                    section["taxonomy_axis"] = self._generic_taxonomy_axis_for_section(section)
                if isinstance(section.get("section_contract"), dict):
                    section["section_contract"]["brand_policy"] = "none"
                    contract_axis = str(section["section_contract"].get("taxonomy_axis") or "").lower()
                    if contract_axis.startswith("brand_"):
                        section["section_contract"]["taxonomy_axis"] = section.get("taxonomy_axis") or self._generic_taxonomy_axis_for_section(section)
                normalized.append(section)
                continue

            original_heading = str(section.get("heading_text") or "")
            section["heading_text"], claim_issues = self._sanitize_unsupported_brand_claims(
                original_heading,
                state,
                section=section,
                context="heading",
                brand_sensitive=self._section_visibly_references_brand(section, state),
            )
            for issue in claim_issues:
                self._record_section_quality_issue(section, f"unsupported_brand_claim_removed:{issue}")
            section["heading_text"] = self._fulfill_and_downgrade_heading(section, state)
            if section["heading_text"] != original_heading:
                self._sync_heading_role_contract(
                    section,
                    state,
                    original_heading,
                    outline=outline,
                    index=section_index,
                )
                visible_brand = self._section_visibly_references_brand(section, state)
                should_use_brand_evidence = self._section_role_should_use_brand_evidence(section, state)

            if state.get("brand_evidence_failure_mode"):
                section["brand_policy"] = "none"
                if str(section.get("taxonomy_axis") or "").lower().startswith("brand_"):
                    section["taxonomy_axis"] = self._generic_taxonomy_axis_for_section(section)
            elif visible_brand or should_use_brand_evidence or section_type in {"introduction", "intro", "conclusion"}:
                section["brand_policy"] = "commercial"
            else:
                section["brand_policy"] = "none"
                if str(section.get("taxonomy_axis") or "").lower().startswith("brand_"):
                    section["taxonomy_axis"] = self._generic_taxonomy_axis_for_section(section)

            if isinstance(section.get("section_contract"), dict):
                section["section_contract"]["brand_policy"] = section["brand_policy"]
                if section["brand_policy"] == "none" and str(section["section_contract"].get("taxonomy_axis") or "").lower().startswith("brand_"):
                    section["section_contract"]["taxonomy_axis"] = section.get("taxonomy_axis") or self._generic_taxonomy_axis_for_section(section)

            normalized.append(section)

        return normalized

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

            # Apply Heading Fulfillment & Downgrade Rule (Phase 1.7 Step 9)
            original_heading = str(section.get("heading_text") or "")
            section["heading_text"] = self._fulfill_and_downgrade_heading(section, state)
            if section["heading_text"] != original_heading:
                self._sync_heading_role_contract(
                    section,
                    state,
                    original_heading,
                    outline=[*safe_outline, section],
                    index=idx,
                )

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

        safe_outline = self._normalize_outline_with_brand_evidence_inventory(safe_outline, state)
        safe_outline = self._ensure_commercial_buyer_journey_coverage(safe_outline, state)

        for idx, section in enumerate(safe_outline):
            self._apply_commercial_section_role(section, state, idx, len(safe_outline))

        # Assign tables only when there is a real decision/use case for a table.
        tables_assigned = 0
        for priority_role in ("comparison", "proof", "cost_value", "features_included"):
            for section in safe_outline:
                if tables_assigned >= 2:
                    break
                if section.get("requires_table"):
                    continue
                if str(section.get("commercial_section_role") or "") != priority_role:
                    continue
                plan = self._table_plan_for_section(section, state)
                if not plan.get("requires_table"):
                    continue
                section["requires_table"] = True
                section["table_type"] = plan.get("table_type", "decision_matrix")
                tables_assigned += 1
                logger.info(
                    "[TableAssigner] Assigned %s table to section: %s",
                    section["table_type"],
                    section.get("heading_text"),
                )
            if tables_assigned >= 2:
                break

        safe_outline = self._ensure_article_table_plan(safe_outline, state)

        # Now, build and enrich the section contracts with the correct requires_table value already present
        for idx, section in enumerate(safe_outline):
            section["section_contract"] = self._build_section_contract(section, safe_outline, idx, state)
            self._enrich_section_contract(section, safe_outline, idx, state)
            self._enforce_commercial_role_contract(section, state)
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
        is_faq = (
            str(section.get("section_type") or "").lower() == "faq"
            or str(section.get("commercial_section_role") or "").lower() == "faq"
        )
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
                if is_faq and self._faq_heading_is_question(stripped):
                    kept.append(f"### {heading_text}")
                elif approved_h3 and normalized in approved_h3:
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

    def _evaluate_brand_owned_section_fulfillment(
        self,
        section: Dict[str, Any],
        content: str,
        state: Dict[str, Any],
    ) -> Dict[str, str]:
        """Soft deterministic fulfillment check for brand-owned sections."""
        from src.services.brand_evidence_service import evaluate_brand_section_fulfillment

        evaluation_section = section
        if not evaluation_section.get("taxonomy_axis"):
            evaluation_section = dict(section)
            evaluation_section["taxonomy_axis"] = self._infer_taxonomy_axis(evaluation_section)

        return evaluate_brand_section_fulfillment(
            section=evaluation_section,
            content=content,
            section_brand_understanding=evaluation_section.get("section_brand_understanding"),
            section_raw_brand_blocks=evaluation_section.get("section_raw_brand_blocks"),
            state=state,
        )

        contract = section.get("section_contract") or {}
        brand_policy = str(contract.get("brand_policy") or section.get("brand_policy") or "").lower()
        content_type = str(state.get("content_type") or "").lower()
        if content_type != "brand_commercial" and brand_policy != "commercial":
            return {"fulfillment_status": "satisfied", "fulfillment_reason": "not brand-owned"}

        heading = str(section.get("heading_text") or "")
        heading_lower = heading.lower()
        content_lower = (content or "").lower()
        axis = str(section.get("taxonomy_axis") or contract.get("taxonomy_axis") or self._infer_taxonomy_axis(section)).lower()
        cards = state.get("brand_evidence_cards") or []

        def _values(keys: List[str]) -> List[str]:
            out: List[str] = []
            for card in cards:
                if not isinstance(card, dict) or card.get("excluded_reason"):
                    continue
                for key in keys:
                    out.extend(str(item).strip() for item in card.get(key, []) if str(item).strip())
            return list(dict.fromkeys(out))

        services = _values(["visible_products_or_services", "visible_features_or_capabilities"])
        projects = _values(["visible_project_or_case_study_examples"])
        process_steps = _values(["visible_process_steps"])
        pricing = _values(["visible_pricing_or_packages"])
        geography = _values(["visible_geography"])

        def _mentioned(values: List[str]) -> bool:
            for value in values:
                folded = value.lower()
                if folded and folded in content_lower:
                    return True
            return False

        pricing_terms = ["باقات", "باقة", "أسعار", "اسعار", "تكلفة", "pricing", "packages", "package", "cost"]
        geo_terms = ["السعودية", "الرياض", "جدة", "saudi", "riyadh", "jeddah"]

        if any(term in heading_lower for term in pricing_terms) and not pricing:
            return {"fulfillment_status": "unsupported", "fulfillment_reason": "brand pricing/packages promised without brand pricing evidence"}
        if any(term in heading_lower for term in geo_terms) and not geography:
            return {"fulfillment_status": "unsupported", "fulfillment_reason": "brand geography promised without explicit brand geography evidence"}
        if axis == "brand_projects":
            if projects and _mentioned(projects):
                return {"fulfillment_status": "satisfied", "fulfillment_reason": "observed project evidence used"}
            return {"fulfillment_status": "unsupported" if projects else "weak", "fulfillment_reason": "project section did not surface observed project names"}
        if axis == "brand_offer":
            if services and _mentioned(services):
                return {"fulfillment_status": "satisfied", "fulfillment_reason": "observed service evidence used"}
            return {"fulfillment_status": "weak" if services else "unsupported", "fulfillment_reason": "service section lacks observed service/capability evidence"}
        if axis == "brand_process":
            if process_steps and _mentioned(process_steps):
                return {"fulfillment_status": "satisfied", "fulfillment_reason": "observed process evidence used"}
            return {"fulfillment_status": "weak" if process_steps else "unsupported", "fulfillment_reason": "process section lacks observed process evidence"}

        return {"fulfillment_status": "satisfied", "fulfillment_reason": "no strict brand-owned promise detected"}

    def _brand_project_names_for_policy(self, state: Dict[str, Any]) -> List[str]:
        """Collect observed project/client names for brand-use policy checks."""
        try:
            from src.services.brand_evidence_service import collect_observed_brand_project_names

            collected = collect_observed_brand_project_names(state=state)
            if collected:
                return collected[:20]
        except Exception:
            pass

        names: List[str] = []
        noise = {
            "screenshots", "technology stack", "technologies used", "scope of work",
            "services provided", "target", "b2c", "b2b", "name", "location",
            "sector", "objective", "brief", "publish date", "project", "projects",
        }
        for brief in state.get("brand_page_narrative_briefs", []) or []:
            if not isinstance(brief, dict):
                continue
            signals = brief.get("routing_signals") if isinstance(brief.get("routing_signals"), dict) else {}
            for value in signals.get("projects") or []:
                text = re.sub(r"\s+", " ", str(value or "")).strip(" .:-|")
                if text and text.casefold() not in noise:
                    names.append(text)
            title = re.sub(r"\s+", " ", str(brief.get("page_title") or "")).strip(" .:-|")
            title = re.sub(r"\s*-\s*(?:creative minds|brandco).*$", "", title, flags=re.IGNORECASE).strip()
            page_type = str(brief.get("page_type") or "").casefold()
            if page_type in {"portfolio", "projects", "case_study", "case-study"} and title and title.casefold() not in noise:
                names.append(title)
        seen = set()
        result: List[str] = []
        for name in names:
            key = name.casefold()
            if key in seen or len(name) < 3:
                continue
            seen.add(key)
            result.append(name)
        return result[:20]

    def _evaluate_brand_usage_policy_fulfillment(
        self,
        section: Dict[str, Any],
        content: str,
        state: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Validate that section output obeys neutral/light/owned brand-use policy."""
        policy = str(section.get("brand_usage_policy") or self._brand_usage_policy_for_section(section, state)).lower()
        if policy in {"brand_owned", "brand_cta", "soft_intro_brand", "no_brand_facts"}:
            return {"fulfillment_status": "satisfied", "fulfillment_reason": "brand usage policy satisfied"}

        text = content or ""
        brand_names = [
            state.get("display_brand_name"),
            state.get("brand_name"),
            state.get("official_brand_name"),
            *(state.get("brand_aliases") or []),
        ]
        brand_names = [str(item).strip() for item in brand_names if str(item or "").strip()]
        brand_mentions = 0
        folded_text = text.casefold()
        for name in dict.fromkeys(brand_names):
            if not name:
                continue
            brand_mentions += folded_text.count(name.casefold())

        project_names = self._brand_project_names_for_policy(state)
        project_mentions = [name for name in project_names if name.casefold() in folded_text]
        heading = str(section.get("heading_text") or "")
        project_heading = bool(
            re.search(r"\b(project|projects|portfolio|case stud)\b", heading, re.IGNORECASE)
            or re.search("\u0645\u0634\u0627\u0631\u064a\u0639|\u0646\u0645\u0627\u0630\u062c|\u0623\u0639\u0645\u0627\u0644", heading)
        )

        if policy == "neutral_market" and (brand_mentions or project_mentions):
            return {
                "fulfillment_status": "unsupported",
                "fulfillment_reason": "brand usage policy violation: neutral_market section contains brand or project facts",
                "brand_mentions": brand_mentions,
                "project_mentions": project_mentions,
            }
        if policy == "brand_light":
            if brand_mentions > 1:
                return {
                    "fulfillment_status": "weak",
                    "fulfillment_reason": "brand usage policy violation: brand_light section mentions the brand more than once",
                    "brand_mentions": brand_mentions,
                    "project_mentions": project_mentions,
                }
            if project_mentions and not project_heading:
                return {
                    "fulfillment_status": "unsupported",
                    "fulfillment_reason": "brand usage policy violation: brand_light section uses project examples outside proof",
                    "brand_mentions": brand_mentions,
                    "project_mentions": project_mentions,
                }
        return {"fulfillment_status": "satisfied", "fulfillment_reason": "brand usage policy satisfied"}

    def _stricter_fulfillment_report(self, primary: Dict[str, Any], secondary: Dict[str, Any]) -> Dict[str, Any]:
        """Return the stricter of two fulfillment reports."""
        rank = {"satisfied": 0, "weak": 1, "unsupported": 2}
        primary_status = str(primary.get("fulfillment_status") or "satisfied")
        secondary_status = str(secondary.get("fulfillment_status") or "satisfied")
        return secondary if rank.get(secondary_status, 0) > rank.get(primary_status, 0) else primary

    def _content_mentions_any_project_record(self, content: str, records: List[Dict[str, Any]]) -> bool:
        """Return True when content mentions any safe project record name or variant."""
        folded = str(content or "").casefold()
        for record in records or []:
            candidates = [record.get("name"), *(record.get("variants") or [])]
            for candidate in candidates:
                name = re.sub(r"\s+", " ", str(candidate or "")).strip()
                if name and name.casefold() in folded:
                    return True
        return False

    def _project_records_required_for_proof(self, records: List[Dict[str, Any]], state: Dict[str, Any], limit: int = 2) -> List[Dict[str, Any]]:
        """
        Pick the project records a proof section should not skip.

        Records are already sorted by target-area relevance in
        _project_records_from_narrative_pack. This helper keeps the rule small:
        require one of the strongest 1-2 examples, not every available project.
        """
        safe = [record for record in records or [] if str(record.get("name") or "").strip()]
        if not safe:
            return []
        return safe[: max(1, min(limit, len(safe)))]

    def _contains_professional_certification_claim(self, text: str) -> bool:
        """Distinguish professional credentials from ordinary workflow approval."""
        value = re.sub(r"\s+", " ", str(text or "")).strip()
        if not value:
            return False
        return bool(re.search(
            r"\b(?:certified|certification|accredited|accreditation|licensed)\b|"
            r"(?:"
            r"(?:\u0634\u0631\u0643\u0629|\u062c\u0647\u0629|\u0645\u0632\u0648\u062f|"
            r"\u0645\u0642\u062f\u0645 \u062e\u062f\u0645\u0629)\s+"
            r"\u0645\u0639\u062a\u0645\u062f(?:\u0629|\u0648\u0646|\u064a\u0646)?\b|"
            r"\u0645\u0639\u062a\u0645\u062f(?:\u0629|\u0648\u0646|\u064a\u0646)?\s+"
            r"(?:\u0645\u0646|\u0644\u062f\u0649|\u0628\u0648\u0627\u0633\u0637\u0629)\b|"
            r"\u062d\u0627\u0635\u0644(?:\u0629|\u0648\u0646|\u064a\u0646)?\s+\u0639\u0644\u0649\s+"
            r"(?:\u0634\u0647\u0627\u062f\u0629|\u0627\u0639\u062a\u0645\u0627\u062f)\b|"
            r"\u0627\u0639\u062a\u0645\u0627\u062f(?:\u0627\u062a)?\s+"
            r"(?:\u0645\u0646|\u0644\u062f\u0649|\u0628\u0648\u0627\u0633\u0637\u0629|"
            r"\u0645\u0647\u0646\u064a(?:\u0629)?|\u0631\u0633\u0645\u064a(?:\u0629)?|"
            r"\u062f\u0648\u0644\u064a(?:\u0629)?|ISO\b)|"
            r"\u0634\u0647\u0627\u062f(?:\u0629|\u0627\u062a)\s+"
            r"(?:\u0645\u0647\u0646\u064a(?:\u0629)?|\u0645\u0639\u062a\u0645\u062f(?:\u0629)?|ISO\b)|"
            r"\u0645\u0631\u062e\u0635(?:\u0629|\u0648\u0646|\u064a\u0646)?\s+"
            r"(?:\u0645\u0646|\u0644\u062f\u0649|\u0628\u0648\u0627\u0633\u0637\u0629)\b"
            r")",
            value,
            re.IGNORECASE,
        ))

    def _brand_claim_support_flags(self, state: Dict[str, Any]) -> Dict[str, bool]:
        """Return category-specific support flags from brand claim boundaries."""
        pack = self._positive_brand_pack_text(state)

        def has(pattern: str) -> bool:
            return bool(re.search(pattern, pack, re.IGNORECASE))

        # Step 3B: prefer unified ground-truth claim boundaries when present.
        try:
            from src.services.brand_evidence_service import (
                record_ground_truth_consumption,
                resolve_brand_claim_boundaries,
            )

            if isinstance(state.get("brand_ground_truth_data"), dict):
                claim_bounds = resolve_brand_claim_boundaries(state)
                record_ground_truth_consumption(state, "validator")
                local_presence = bool(claim_bounds.get("local_presence"))
                logger.info(
                    "[ground_truth] validator_claim_boundaries_used=true "
                    "pricing=%s local_presence=%s",
                    str(bool(claim_bounds.get("pricing_available"))).lower(),
                    str(local_presence).lower(),
                )
                return {
                    "pricing": bool(claim_bounds.get("pricing_available")),
                    "testimonial": bool(claim_bounds.get("testimonials")),
                    "certification": bool(claim_bounds.get("certifications")),
                    "award": bool(claim_bounds.get("awards")),
                    "local_presence": local_presence,
                    "local_support": local_presence and has(
                        r"\b(?:local support|local customer support|local technical support|on-site support)\b|"
                        r"(?:\u062f\u0639\u0645 \u0645\u062d\u0644\u064a|\u062f\u0639\u0645 \u0641\u0646\u064a \u0645\u062d\u0644\u064a|"
                        r"\u062f\u0639\u0645 \u0645\u064a\u062f\u0627\u0646\u064a)"
                    ),
                }
        except Exception:
            pass

        boundaries = state.get("brand_evidence_boundaries")
        has_explicit_boundaries = isinstance(boundaries, dict)
        if not isinstance(boundaries, dict):
            try:
                from src.services.brand_evidence_service import build_brand_evidence_boundaries
                boundaries = build_brand_evidence_boundaries(state)
            except Exception:
                boundaries = {}
        inventory = self._brand_evidence_inventory_for_outline(state)

        local_presence = bool(
            boundaries.get("local_presence")
            if has_explicit_boundaries
            else inventory.get("explicit_geography")
        )
        return {
            "pricing": bool(
                boundaries.get("brand_pricing")
                if has_explicit_boundaries
                else inventory.get("pricing_available")
            ),
            "testimonial": has(
                r"\b(?:testimonial|client review|customer review|client feedback|customer feedback)\b|"
                r"(?:\u0622\u0631\u0627\u0621 \u0627\u0644\u0639\u0645\u0644\u0627\u0621|"
                r"\u062a\u062c\u0627\u0631\u0628 \u0627\u0644\u0639\u0645\u0644\u0627\u0621|"
                r"\u0634\u0647\u0627\u062f\u0627\u062a \u0627\u0644\u0639\u0645\u0644\u0627\u0621)"
            ),
            "certification": bool(
                boundaries.get("certifications")
                if has_explicit_boundaries
                else self._contains_professional_certification_claim(pack)
            ),
            "award": has(
                r"\b(?:award-winning|awarded|won (?:an? )?award|recipient of)\b|"
                r"(?:\u062d\u0627\u0626\u0632 \u0639\u0644\u0649|\u062d\u0635\u0644 \u0639\u0644\u0649 \u062c\u0627\u0626\u0632\u0629|"
                r"\u062c\u0648\u0627\u0626\u0632 \u0645\u0648\u062b\u0642\u0629)"
            ),
            "local_presence": local_presence,
            "local_support": local_presence and has(
                r"\b(?:local support|local customer support|local technical support|on-site support)\b|"
                r"(?:\u062f\u0639\u0645 \u0645\u062d\u0644\u064a|\u062f\u0639\u0645 \u0641\u0646\u064a \u0645\u062d\u0644\u064a|"
                r"\u062f\u0639\u0645 \u0645\u064a\u062f\u0627\u0646\u064a)"
            ),
        }

    def _unsupported_brand_claim_categories(
        self,
        text: str,
        state: Dict[str, Any],
        *,
        brand_sensitive: bool = False,
    ) -> List[str]:
        """Classify unsupported brand claims without blocking neutral market guidance."""
        value = re.sub(r"\s+", " ", str(text or "")).strip()
        if not value:
            return []

        brand_context = brand_sensitive or self._brand_name_in_text(value, state) or bool(
            re.search(
                r"\b(?:our company|our brand|our packages|the company(?:'s)?|the brand(?:'s)?)\b|"
                r"(?:\u0634\u0631\u0643\u062a\u0646\u0627|\u0628\u0631\u0627\u0646\u062f\u0646\u0627|"
                r"\u0628\u0627\u0642\u0627\u062a\u0646\u0627|\u0627\u0644\u0634\u0631\u0643\u0629 \u062a\u0642\u062f\u0645|"
                r"\u0627\u0644\u0628\u0631\u0627\u0646\u062f \u064a\u0642\u062f\u0645)",
                value,
                re.IGNORECASE,
            )
        )
        if not brand_context:
            return []

        support = self._brand_claim_support_flags(state)
        issues: List[str] = []

        if re.search(
            r"\b(?:testimonials?|client experiences?|customer stories|client reviews?|customer reviews?)\b|"
            r"(?:\u0622\u0631\u0627\u0621 \u0627\u0644\u0639\u0645\u0644\u0627\u0621|"
            r"\u062a\u062c\u0627\u0631\u0628 \u0627\u0644\u0639\u0645\u0644\u0627\u0621|"
            r"\u0634\u0647\u0627\u062f\u0627\u062a \u0627\u0644\u0639\u0645\u0644\u0627\u0621)",
            value,
            re.IGNORECASE,
        ) and not support["testimonial"]:
            issues.append("testimonial")

        brand_pricing_claim = re.search(
            r"\b(?:our packages?|our prices?|company packages?|brand packages?|"
            r"offers? (?:a |its )?(?:package|plan)|packages? (?:start|include|available)|"
            r"prices? (?:start|begin)|pricing (?:starts?|is|at)|fees? (?:start|are)|"
            r"package (?:starts?|costs?|priced))\b|"
            r"(?:\u0628\u0627\u0642\u0627\u062a\u0646\u0627|\u0623\u0633\u0639\u0627\u0631\u0646\u0627|"
            r"\u0628\u0627\u0642\u0627\u062a \u0627\u0644\u0634\u0631\u0643\u0629|"
            r"\u0623\u0633\u0639\u0627\u0631 \u0627\u0644\u0634\u0631\u0643\u0629|"
            r"\u062a\u0642\u062f\u0645 .{0,60}\u0628\u0627\u0642\u0627\u062a|"
            r"\u062a\u0628\u062f\u0623 \u0627\u0644\u0623\u0633\u0639\u0627\u0631|"
            r"\u062a\u0628\u062f\u0623 \u0627\u0644\u0628\u0627\u0642\u0627\u062a|"
            r"\u0628\u0627\u0642\u0629 .{0,30}(?:\u0628\u0633\u0639\u0631|\u062a\u0643\u0644\u0641))",
            value,
            re.IGNORECASE,
        )
        heading_like_pricing = bool(
            len(value) <= 140
            and self._brand_name_in_text(value, state)
            and re.search(
                r"\b(?:pricing|prices?|packages?|plans?)\b|"
                r"(?:\u0623\u0633\u0639\u0627\u0631|\u0628\u0627\u0642\u0627\u062a|\u0628\u0627\u0642\u0629)",
                value,
                re.IGNORECASE,
            )
        )
        if (brand_pricing_claim or heading_like_pricing) and not support["pricing"]:
            issues.append("pricing")

        if self._contains_professional_certification_claim(value) and not support["certification"]:
            issues.append("certification")

        if re.search(
            r"\b(?:award-winning|awarded|won (?:an? )?award|industry awards?)\b|"
            r"(?:\u062d\u0627\u0626\u0632 \u0639\u0644\u0649|\u062d\u0635\u0644 \u0639\u0644\u0649 \u062c\u0627\u0626\u0632\u0629|"
            r"\u0627\u0644\u062d\u0627\u0626\u0632 \u0639\u0644\u0649 \u062c\u0648\u0627\u0626\u0632)",
            value,
            re.IGNORECASE,
        ) and not support["award"]:
            issues.append("award")

        if re.search(
            r"\b(?:local support|local customer support|local technical support|on-site support)\b|"
            r"(?:\u062f\u0639\u0645 \u0645\u062d\u0644\u064a|\u062f\u0639\u0645 \u0641\u0646\u064a \u0645\u062d\u0644\u064a|"
            r"\u062f\u0639\u0645 \u0645\u064a\u062f\u0627\u0646\u064a)",
            value,
            re.IGNORECASE,
        ) and not support["local_support"]:
            issues.append("local_support")

        if re.search(
            r"\b(?:local presence|local team|local office|local branch|local market expertise|"
            r"expertise in the local market|understands? the local market|"
            r"deep understanding of the local market|based in|headquartered in|office in|branch in)\b|"
            r"(?:\u062d\u0636\u0648\u0631 \u0645\u062d\u0644\u064a|\u0641\u0631\u064a\u0642 \u0645\u062d\u0644\u064a|"
            r"\u0645\u0643\u062a\u0628 \u0645\u062d\u0644\u064a|\u0641\u0631\u0639 \u0645\u062d\u0644\u064a|"
            r"\u062e\u0628\u0631\u0629 \u0641\u064a \u0627\u0644\u0633\u0648\u0642 \u0627\u0644\u0645\u062d\u0644\u064a|"
            r"\u0641\u0647\u0645 \u0627\u0644\u0633\u0648\u0642 \u0627\u0644\u0645\u062d\u0644\u064a|"
            r"\u064a\u0641\u0647\u0645 \u0627\u0644\u0633\u0648\u0642 \u0627\u0644\u0645\u062d\u0644\u064a|"
            r"\u0645\u0642\u0631\u0647\u0627 \u0641\u064a|\u0644\u062f\u064a\u0647\u0627 \u0645\u0643\u062a\u0628 \u0641\u064a|"
            r"\u0644\u062f\u064a\u0647\u0627 \u0641\u0631\u0639 \u0641\u064a)",
            value,
            re.IGNORECASE,
        ) and not support["local_presence"]:
            issues.append("local_presence")

        return list(dict.fromkeys(issues))

    def _sanitize_unsupported_brand_claims(
        self,
        text: str,
        state: Dict[str, Any],
        *,
        section: Optional[Dict[str, Any]] = None,
        context: str = "body",
        brand_sensitive: Optional[bool] = None,
    ) -> tuple[str, List[str]]:
        """Remove unsupported brand claims while preserving neutral market guidance."""
        value = str(text or "")
        if not value or str(state.get("content_type") or "").lower() != "brand_commercial":
            return value, []

        if brand_sensitive is None:
            policy = ""
            if section:
                policy = str(
                    section.get("brand_usage_policy")
                    or self._brand_usage_policy_for_section(section, state)
                ).lower()
            brand_sensitive = policy in {"brand_owned", "brand_cta", "soft_intro_brand"}

        heading_context = context in {"heading", "title", "h1", "meta_title"}

        def category_issues(fragment: str) -> List[str]:
            return self._unsupported_brand_claim_categories(
                fragment,
                state,
                brand_sensitive=bool(brand_sensitive),
            )

        if heading_context:
            issues = category_issues(value)
            if not issues:
                return value, []

            cleaned = value
            removal_patterns = {
                "testimonial": (
                    r"\b(?:and\s+)?(?:customer testimonials?|client testimonials?|client experiences?|"
                    r"customer stories|client reviews?|customer reviews?|testimonials?)\b|"
                    r"\s*(?:\u0648)?\s*(?:\u0622\u0631\u0627\u0621 \u0627\u0644\u0639\u0645\u0644\u0627\u0621|"
                    r"\u062a\u062c\u0627\u0631\u0628 \u0627\u0644\u0639\u0645\u0644\u0627\u0621|"
                    r"\u0634\u0647\u0627\u062f\u0627\u062a \u0627\u0644\u0639\u0645\u0644\u0627\u0621)"
                ),
                "pricing": (
                    r"\b(?:and\s+)?(?:pricing|prices?|packages?|plans?)\b|"
                    r"\s*(?:\u0648)?\s*(?:\u0627\u0644\u0623\u0633\u0639\u0627\u0631|"
                    r"\u0623\u0633\u0639\u0627\u0631|\u0627\u0644\u0628\u0627\u0642\u0627\u062a|"
                    r"\u0628\u0627\u0642\u0627\u062a|\u0628\u0627\u0642\u0629)"
                ),
                "certification": (
                    r"\b(?:and\s+)?(?:certified|certification|accredited|accreditation|licensed)\b|"
                    r"(?:"
                    r"(?:\u0634\u0631\u0643\u0629|\u062c\u0647\u0629|\u0645\u0632\u0648\u062f|"
                    r"\u0645\u0642\u062f\u0645 \u062e\u062f\u0645\u0629)\s+"
                    r"\u0645\u0639\u062a\u0645\u062f(?:\u0629|\u0648\u0646|\u064a\u0646)?|"
                    r"\u0645\u0639\u062a\u0645\u062f(?:\u0629|\u0648\u0646|\u064a\u0646)?\s+"
                    r"(?:\u0645\u0646|\u0644\u062f\u0649|\u0628\u0648\u0627\u0633\u0637\u0629)|"
                    r"\u062d\u0627\u0635\u0644(?:\u0629|\u0648\u0646|\u064a\u0646)?\s+\u0639\u0644\u0649\s+"
                    r"(?:\u0634\u0647\u0627\u062f\u0629|\u0627\u0639\u062a\u0645\u0627\u062f)|"
                    r"\u0627\u0639\u062a\u0645\u0627\u062f(?:\u0627\u062a)?\s+"
                    r"(?:\u0645\u0646|\u0644\u062f\u0649|\u0628\u0648\u0627\u0633\u0637\u0629|"
                    r"\u0645\u0647\u0646\u064a(?:\u0629)?|\u0631\u0633\u0645\u064a(?:\u0629)?|"
                    r"\u062f\u0648\u0644\u064a(?:\u0629)?|ISO\b)|"
                    r"\u0634\u0647\u0627\u062f(?:\u0629|\u0627\u062a)\s+"
                    r"(?:\u0645\u0647\u0646\u064a(?:\u0629)?|\u0645\u0639\u062a\u0645\u062f(?:\u0629)?|ISO\b)|"
                    r"\u0645\u0631\u062e\u0635(?:\u0629|\u0648\u0646|\u064a\u0646)?\s+"
                    r"(?:\u0645\u0646|\u0644\u062f\u0649|\u0628\u0648\u0627\u0633\u0637\u0629)"
                    r")"
                ),
                "award": (
                    r"\b(?:and\s+)?(?:award-winning|awarded|industry awards?)\b|"
                    r"\s*(?:\u0648)?\s*(?:\u062d\u0627\u0626\u0632 \u0639\u0644\u0649 \u062c\u0648\u0627\u0626\u0632|"
                    r"\u0627\u0644\u062d\u0627\u0626\u0632 \u0639\u0644\u0649 \u062c\u0648\u0627\u0626\u0632)"
                ),
                "local_support": (
                    r"\b(?:and\s+)?(?:local support|local customer support|local technical support|on-site support)\b|"
                    r"\s*(?:\u0648)?\s*(?:\u062f\u0639\u0645 \u0645\u062d\u0644\u064a|"
                    r"\u062f\u0639\u0645 \u0641\u0646\u064a \u0645\u062d\u0644\u064a|\u062f\u0639\u0645 \u0645\u064a\u062f\u0627\u0646\u064a)"
                ),
                "local_presence": (
                    r"\b(?:and\s+)?(?:local presence|local team|local office|local branch|"
                    r"local market expertise|expertise in the local market|understanding the local market)\b|"
                    r"\s*(?:\u0648)?\s*(?:\u062d\u0636\u0648\u0631 \u0645\u062d\u0644\u064a|"
                    r"\u0641\u0631\u064a\u0642 \u0645\u062d\u0644\u064a|\u0645\u0643\u062a\u0628 \u0645\u062d\u0644\u064a|"
                    r"\u0641\u0631\u0639 \u0645\u062d\u0644\u064a|\u062e\u0628\u0631\u0629 \u0641\u064a \u0627\u0644\u0633\u0648\u0642 \u0627\u0644\u0645\u062d\u0644\u064a|"
                    r"\u0641\u0647\u0645 \u0627\u0644\u0633\u0648\u0642 \u0627\u0644\u0645\u062d\u0644\u064a)"
                ),
            }
            for issue in issues:
                cleaned = re.sub(removal_patterns[issue], " ", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s+", " ", cleaned)
            cleaned = re.sub(r"\s*([|:,-])\s*(?=$|[|:,-])", " ", cleaned)
            cleaned = re.sub(r"\b(?:and|or)\s*$", "", cleaned, flags=re.IGNORECASE)
            cleaned = cleaned.strip(" -,:|")
            brand_name = str(state.get("display_brand_name") or state.get("brand_name") or "").strip()
            if not cleaned or cleaned.casefold() == brand_name.casefold():
                base = str(state.get("primary_keyword") or state.get("raw_title") or "").strip()
                cleaned = base or (
                    "\u0623\u062f\u0644\u0629 \u0648\u0642\u062f\u0631\u0627\u062a \u0645\u0648\u062b\u0642\u0629"
                    if str(state.get("article_language") or "").lower().startswith("ar")
                    else "Documented Capabilities and Evidence"
                )
                if context in {"title", "h1", "meta_title"} and brand_name and brand_name.casefold() not in cleaned.casefold():
                    cleaned = f"{cleaned} | {brand_name}"
            return cleaned, issues

        issues: List[str] = []
        sentence_split_re = re.compile(r"(?<=[.!?\u061f])\s+")
        processed_lines: List[str] = []
        for line in value.splitlines():
            if not line.strip():
                processed_lines.append("")
                continue
            heading_match = re.match(r"^(\s*#{1,6}\s+)(.*)$", line)
            if heading_match:
                clean_heading, heading_issues = self._sanitize_unsupported_brand_claims(
                    heading_match.group(2),
                    state,
                    section=section,
                    context="heading",
                    brand_sensitive=brand_sensitive,
                )
                issues.extend(heading_issues)
                processed_lines.append(heading_match.group(1) + clean_heading)
                continue

            kept_sentences: List[str] = []
            for sentence in sentence_split_re.split(line):
                sentence_issues = category_issues(sentence)
                if sentence_issues:
                    issues.extend(sentence_issues)
                    continue
                kept_sentences.append(sentence.strip())
            if kept_sentences:
                processed_lines.append(" ".join(kept_sentences))

        cleaned_lines: List[str] = []
        for line in processed_lines:
            if not line and (not cleaned_lines or not cleaned_lines[-1]):
                continue
            cleaned_lines.append(line)
        cleaned = "\n".join(cleaned_lines).strip()
        if context == "meta_description" and not cleaned and issues:
            topic = str(state.get("primary_keyword") or state.get("raw_title") or "").strip()
            is_ar = str(state.get("article_language") or "").lower().startswith("ar")
            if topic:
                cleaned = (
                    f"\u062f\u0644\u064a\u0644 \u0639\u0645\u0644\u064a \u062d\u0648\u0644 {topic} \u0644\u0641\u0647\u0645 \u0627\u0644\u062e\u064a\u0627\u0631\u0627\u062a \u0648\u0627\u0644\u0645\u0639\u0627\u064a\u064a\u0631 \u0642\u0628\u0644 \u0627\u062a\u062e\u0627\u0630 \u0627\u0644\u0642\u0631\u0627\u0631."
                    if is_ar
                    else f"A practical guide to {topic}, the available options, and the criteria to review before deciding."
                )
        return cleaned, list(dict.fromkeys(issues))

    def _pack_has_explicit_testimonial_evidence(self, state: Dict[str, Any]) -> bool:
        """Detect explicit testimonial/review evidence in the page knowledge pack only."""
        return self._brand_claim_support_flags(state)["testimonial"]

    def _downgrade_unsupported_testimonial_heading(self, section: Dict[str, Any], state: Dict[str, Any]) -> None:
        """Remove testimonial/client-experience wording when the pack does not support testimonials."""
        heading = str(section.get("heading_text") or "")
        if not heading or self._pack_has_explicit_testimonial_evidence(state):
            return
        if not (
            re.search(r"\b(testimonials?|reviews?|client experiences?|customer stories)\b", heading, re.IGNORECASE)
            or re.search(r"تجارب العملاء|آراء العملاء|تقييمات العملاء|شهادات العملاء", heading)
        ):
            return

        replacement = re.sub(r"\s*(?:و)?\s*(?:تجارب العملاء|آراء العملاء|تقييمات العملاء|شهادات العملاء)\s*", " ", heading)
        replacement = re.sub(r"\b(?:and\s+)?(?:testimonials?|reviews?|client experiences?|customer stories)\b", "", replacement, flags=re.IGNORECASE)
        replacement = re.sub(r"\s+", " ", replacement).strip(" -–:|")
        if not replacement:
            replacement = "نماذج من مشاريع البراند" if re.search(r"[\u0600-\u06FF]", heading) else "Observed brand project examples"
        section["heading_text"] = replacement
        section.setdefault("section_quality_issues", []).append("unsupported_testimonial_heading_downgraded")
        logger.info("[testimonial_heading_guard] Downgraded unsupported testimonial heading '%s' -> '%s'.", heading, replacement)

    def _record_section_quality_issue(self, section: Dict[str, Any], issue: str) -> None:
        issues = section.setdefault("section_quality_issues", [])
        if issue not in issues:
            issues.append(issue)

    def _is_faq_planning_text(self, text: str) -> bool:
        """Detect leaked writing instructions without treating normal advice as planning text."""
        value = re.sub(r"\s+", " ", str(text or "")).strip()
        if not value:
            return False
        if re.search(
            r"^\s*(?:\u0627\u0643\u062a\u0628|\u0627\u0630\u0643\u0631|"
            r"\u0642\u0645 \u0628(?:\u062a\u0648\u0636\u064a\u062d|\u0625\u0636\u0627\u0641\u0629|\u0643\u062a\u0627\u0628\u0629)|"
            r"\u0631\u0643\u0632 \u0639\u0644\u0649|\u062a\u062c\u0646\u0628 \u0630\u0643\u0631|"
            r"write|mention|writer should|section should|the section must|focus (?:this section )?on)\b",
            value,
            re.IGNORECASE,
        ):
            return True
        meta = re.search(
            r"\b(?:writer|section|outline|heading|instruction|prompt|article|draft|response|answer|content)\b|"
            r"\u0627\u0644\u0643\u0627\u062a\u0628|\u0647\u0630\u0627 \u0627\u0644\u0642\u0633\u0645|\u0627\u0644\u0633\u0643\u0634\u0646|"
            r"\u0627\u0644\u0639\u0646\u0648\u0627\u0646|\u0627\u0644\u0645\u062e\u0637\u0637|\u0627\u0644\u062a\u0639\u0644\u064a\u0645\u0627\u062a|"
            r"\u0627\u0644\u0628\u0631\u0648\u0645\u0628\u062a|\u0627\u0644\u0645\u0642\u0627\u0644|\u0627\u0644\u0625\u062c\u0627\u0628\u0629|\u0627\u0644\u0645\u062d\u062a\u0648\u0649",
            value,
            re.IGNORECASE,
        )
        directive = re.search(
            r"\b(?:should|must|include|avoid|focus|write|mention|explain|ensure|add)\b|"
            r"\u064a\u062c\u0628 \u0623\u0646|\u064a\u0646\u0628\u063a\u064a \u0623\u0646|\u0623\u0636\u0641|"
            r"\u0648\u0636\u062d|\u0627\u0630\u0643\u0631|\u062a\u062c\u0646\u0628|\u0631\u0643\u0632",
            value,
            re.IGNORECASE,
        )
        formatting = re.search(
            r"\b(?:h[1-6]|bullet points?|paragraphs?|word count|cta|format)\b|"
            r"\u0641\u0642\u0631\u0627\u062a|\u0646\u0642\u0627\u0637|\u0639\u062f\u062f \u0627\u0644\u0643\u0644\u0645\u0627\u062a|\u062a\u0646\u0633\u064a\u0642",
            value,
            re.IGNORECASE,
        )
        return bool((meta and directive) or (formatting and directive))

    def _faq_sensitive_topic(self, question: str) -> str:
        if self._contains_professional_certification_claim(question):
            return "certification"
        patterns = (
            ("pricing", r"\b(?:price|pricing|cost|fee|quote|packages?|plans?)\b|\u0633\u0639\u0631|\u0623\u0633\u0639\u0627\u0631|\u062a\u0643\u0644\u0641|\u062a\u0633\u0639\u064a\u0631|\u0628\u0627\u0642\u0627\u062a?"),
            ("timeline", r"\b(?:timeline|delivery time|turnaround|how long|duration)\b|\u0645\u062f\u0629 \u0627\u0644\u062a\u0646\u0641\u064a\u0630|\u0645\u062f\u0629 \u0627\u0644\u062a\u0633\u0644\u064a\u0645|\u0643\u0645 \u064a\u0633\u062a\u063a\u0631\u0642"),
            ("guarantee", r"\b(?:guarantee|guaranteed|warrant(?:y|ies))\b|\u0636\u0645\u0627\u0646|\u0645\u0636\u0645\u0648\u0646"),
            ("support", r"\b(?:technical support|customer support|maintenance|aftercare|after launch)\b|\u062f\u0639\u0645 \u0641\u0646\u064a|\u0635\u064a\u0627\u0646\u0629|\u062f\u0639\u0645 \u0628\u0639\u062f"),
            ("client_count", r"\b(?:number of clients|how many clients|client count|projects completed|how many projects)\b|\u0639\u062f\u062f \u0627\u0644\u0639\u0645\u0644\u0627\u0621|\u0643\u0645 \u0639\u0645\u064a\u0644|\u0639\u062f\u062f \u0627\u0644\u0645\u0634\u0627\u0631\u064a\u0639|\u0643\u0645 \u0645\u0634\u0631\u0648\u0639"),
            ("certification", r"\bawards?\b|\u062c\u0648\u0627\u0626\u0632?"),
            ("testimonial", r"\b(?:testimonials?|reviews?|ratings?|client feedback)\b|\u062a\u0642\u064a\u064a\u0645\u0627\u062a|\u0645\u0631\u0627\u062c\u0639\u0627\u062a|\u0622\u0631\u0627\u0621 \u0627\u0644\u0639\u0645\u0644\u0627\u0621|\u062a\u062c\u0627\u0631\u0628 \u0627\u0644\u0639\u0645\u0644\u0627\u0621"),
        )
        for topic, pattern in patterns:
            if re.search(pattern, str(question or ""), re.IGNORECASE):
                return topic
        return ""

    def _faq_question_references_brand(self, question: str, state: Dict[str, Any]) -> bool:
        return self._brand_name_in_text(question, state) or bool(
            re.search(
                r"\b(?:your company|your brand|do you|does the company|company's)\b|"
                r"\u0644\u062f\u064a\u0643\u0645|\u0639\u0646\u062f\u0643\u0645|\u0627\u0644\u0634\u0631\u0643\u0629|\u0627\u0644\u0628\u0631\u0627\u0646\u062f",
                str(question or ""),
                re.IGNORECASE,
            )
        )

    def _positive_brand_pack_text(self, state: Dict[str, Any]) -> str:
        pack = str(state.get("brand_page_knowledge_pack_context") or "")
        if not pack and state.get("brand_page_narrative_briefs"):
            pack = "\n".join(
                str(item.get("narrative_brief") or "")
                for item in state.get("brand_page_narrative_briefs") or []
                if isinstance(item, dict)
            )
        negative = re.compile(
            r"\b(?:no explicit|not observed|not stated|not found|without explicit|absent|unsupported)\b|"
            r"\u0644\u0627 \u064a\u0648\u062c\u062f|\u063a\u064a\u0631 \u0645\u0630\u0643\u0648\u0631|\u0644\u0645 \u064a\u0630\u0643\u0631|\u063a\u064a\u0631 \u0645\u062f\u0639\u0648\u0645",
            re.IGNORECASE,
        )
        return "\n".join(line for line in pack.splitlines() if line.strip() and not negative.search(line))

    def _faq_has_supporting_evidence(self, topic: str, state: Dict[str, Any]) -> bool:
        inventory = self._brand_evidence_inventory_for_outline(state)
        if topic == "pricing":
            return bool(inventory.get("pricing_available"))
        if topic == "certification":
            return self._brand_claim_support_flags(state)["certification"]
        pack = self._positive_brand_pack_text(state)
        patterns = {
            "timeline": r"\b(?:delivery timeline|turnaround|delivered within|delivery time)\b|\u0645\u062f\u0629 \u0627\u0644\u062a\u0633\u0644\u064a\u0645|\u0645\u062f\u0629 \u0627\u0644\u062a\u0646\u0641\u064a\u0630|\u062e\u0644\u0627\u0644 \d+",
            "guarantee": r"\b(?:guarantee|guaranteed|warranty)\b|\u0636\u0645\u0627\u0646|\u0645\u0636\u0645\u0648\u0646",
            "support": r"\b(?:technical support|customer support|maintenance|aftercare|post-launch support)\b|\u062f\u0639\u0645 \u0641\u0646\u064a|\u0635\u064a\u0627\u0646\u0629|\u062f\u0639\u0645 \u0628\u0639\u062f",
            "client_count": r"\b\d+\+?\s+(?:clients?|projects?)\b|\d+\+?\s*(?:\u0639\u0645\u064a\u0644|\u0645\u0634\u0631\u0648\u0639)",
            "testimonial": r"\b(?:testimonial|client review|customer review|client feedback)\b|\u0622\u0631\u0627\u0621 \u0627\u0644\u0639\u0645\u0644\u0627\u0621|\u062a\u062c\u0627\u0631\u0628 \u0627\u0644\u0639\u0645\u0644\u0627\u0621",
        }
        pattern = patterns.get(topic)
        return bool(pattern and re.search(pattern, pack, re.IGNORECASE))

    def _faq_market_guidance_replacement(self, topic: str, state: Dict[str, Any]) -> tuple[str, str]:
        is_ar = str(state.get("article_language") or "").lower().startswith("ar")
        if is_ar:
            replacements = {
                "pricing": ("\u0645\u0627 \u0627\u0644\u0639\u0648\u0627\u0645\u0644 \u0627\u0644\u062a\u064a \u062a\u0624\u062b\u0631 \u0639\u0644\u0649 \u0627\u0644\u062a\u0643\u0644\u0641\u0629\u061f", "\u062a\u062a\u0623\u062b\u0631 \u0627\u0644\u062a\u0643\u0644\u0641\u0629 \u0628\u0646\u0637\u0627\u0642 \u0627\u0644\u0639\u0645\u0644\u060c \u0648\u0627\u0644\u062a\u062e\u0635\u064a\u0635\u060c \u0648\u0627\u0644\u062a\u0643\u0627\u0645\u0644\u0627\u062a\u060c \u0648\u0627\u0644\u062f\u0639\u0645\u061b \u0644\u0630\u0644\u0643 \u064a\u0641\u0636\u0644 \u0645\u0642\u0627\u0631\u0646\u0629 \u0646\u0637\u0627\u0642 \u0645\u0643\u062a\u0648\u0628 \u0628\u062f\u0644\u0627 \u0645\u0646 \u0631\u0642\u0645 \u0639\u0627\u0645."),
                "timeline": ("\u0645\u0627 \u0627\u0644\u0639\u0648\u0627\u0645\u0644 \u0627\u0644\u062a\u064a \u062a\u0624\u062b\u0631 \u0639\u0644\u0649 \u0645\u062f\u0629 \u0627\u0644\u062a\u0646\u0641\u064a\u0630\u061f", "\u062a\u0639\u062a\u0645\u062f \u0627\u0644\u0645\u062f\u0629 \u0639\u0644\u0649 \u0627\u0644\u0646\u0637\u0627\u0642\u060c \u0648\u0633\u0631\u0639\u0629 \u0627\u0644\u0627\u0639\u062a\u0645\u0627\u062f\u060c \u0648\u0627\u0644\u062a\u0643\u0627\u0645\u0644\u0627\u062a\u060c \u0648\u062c\u0648\u0644\u0627\u062a \u0627\u0644\u0645\u0631\u0627\u062c\u0639\u0629\u060c \u0648\u064a\u0646\u0628\u063a\u064a \u062a\u062b\u0628\u064a\u062a\u0647\u0627 \u0641\u064a \u062e\u0637\u0629 \u0627\u0644\u0645\u0634\u0631\u0648\u0639."),
                "guarantee": ("\u0645\u0627 \u0627\u0644\u0630\u064a \u064a\u062c\u0628 \u062a\u0648\u0636\u064a\u062d\u0647 \u0628\u0634\u0623\u0646 \u0627\u0644\u0636\u0645\u0627\u0646\u061f", "\u0648\u0636\u062d \u0645\u0627 \u064a\u063a\u0637\u064a\u0647 \u0627\u0644\u0627\u062a\u0641\u0627\u0642\u060c \u0648\u0627\u0644\u0627\u0633\u062a\u062b\u0646\u0627\u0621\u0627\u062a\u060c \u0648\u0645\u0633\u0624\u0648\u0644\u064a\u0629 \u0627\u0644\u062a\u0639\u062f\u064a\u0644 \u0623\u0648 \u0627\u0644\u0635\u064a\u0627\u0646\u0629 \u0628\u0639\u062f \u0627\u0644\u062a\u0633\u0644\u064a\u0645."),
                "support": ("\u0645\u0627 \u0627\u0644\u0630\u064a \u064a\u062c\u0628 \u062a\u0648\u0636\u064a\u062d\u0647 \u0628\u0634\u0623\u0646 \u0627\u0644\u062f\u0639\u0645 \u0628\u0639\u062f \u0627\u0644\u062a\u0633\u0644\u064a\u0645\u061f", "\u0648\u0636\u062d \u0646\u0637\u0627\u0642 \u0627\u0644\u062f\u0639\u0645\u060c \u0648\u0642\u0646\u0648\u0627\u062a \u0627\u0644\u062a\u0648\u0627\u0635\u0644\u060c \u0648\u0645\u0627 \u064a\u0639\u062f \u0635\u064a\u0627\u0646\u0629 \u0636\u0645\u0646 \u0627\u0644\u0646\u0637\u0627\u0642 \u0623\u0648 \u0639\u0645\u0644\u0627 \u0625\u0636\u0627\u0641\u064a\u0627."),
                "client_count": ("\u0645\u0627 \u0627\u0644\u0623\u062f\u0644\u0629 \u0627\u0644\u062a\u064a \u062a\u0633\u0627\u0639\u062f \u0639\u0644\u0649 \u062a\u0642\u064a\u064a\u0645 \u062e\u0628\u0631\u0629 \u0627\u0644\u0645\u0632\u0648\u062f\u061f", "\u0642\u064a\u0645 \u0627\u0644\u062e\u0628\u0631\u0629 \u0645\u0646 \u062e\u0644\u0627\u0644 \u0623\u0645\u062b\u0644\u0629 \u0645\u0648\u062b\u0642\u0629\u060c \u0648\u0648\u0636\u0648\u062d \u0627\u0644\u0646\u0637\u0627\u0642\u060c \u0648\u062c\u0648\u062f\u0629 \u0627\u0644\u0645\u062e\u0631\u062c\u0627\u062a\u060c \u0644\u0627 \u0628\u0639\u062f\u062f \u0639\u0627\u0645 \u063a\u064a\u0631 \u0645\u0648\u062b\u0642."),
                "certification": ("\u0643\u064a\u0641 \u064a\u0645\u0643\u0646 \u0627\u0644\u062a\u062d\u0642\u0642 \u0645\u0646 \u0627\u0644\u0627\u0639\u062a\u0645\u0627\u062f\u0627\u062a\u061f", "\u062a\u062d\u0642\u0642 \u0645\u0646 \u0627\u0644\u0645\u0635\u062f\u0631 \u0627\u0644\u0631\u0633\u0645\u064a\u060c \u0648\u0627\u0644\u0635\u0644\u0627\u062d\u064a\u0629\u060c \u0648\u0639\u0644\u0627\u0642\u0629 \u0627\u0644\u0627\u0639\u062a\u0645\u0627\u062f \u0628\u0646\u0637\u0627\u0642 \u0627\u0644\u062e\u062f\u0645\u0629."),
                "testimonial": ("\u0643\u064a\u0641 \u064a\u0645\u0643\u0646 \u062a\u0642\u064a\u064a\u0645 \u062a\u062c\u0627\u0631\u0628 \u0627\u0644\u0639\u0645\u0644\u0627\u0621 \u0628\u0635\u0648\u0631\u0629 \u0645\u0648\u062b\u0648\u0642\u0629\u061f", "\u0627\u0628\u062d\u062b \u0639\u0646 \u062a\u062c\u0627\u0631\u0628 \u0645\u0646\u0633\u0648\u0628\u0629 \u0625\u0644\u0649 \u0645\u0635\u0627\u062f\u0631 \u0648\u0627\u0636\u062d\u0629 \u0648\u062a\u0641\u0627\u0635\u064a\u0644 \u0642\u0627\u0628\u0644\u0629 \u0644\u0644\u062a\u062d\u0642\u0642."),
            }
        else:
            replacements = {
                "pricing": ("What factors affect the cost?", "Cost depends on scope, customization, integrations, and support, so compare a written scope rather than a generic figure."),
                "timeline": ("What factors affect the implementation timeline?", "The timeline depends on scope, approval speed, integrations, and review rounds, and should be confirmed in a project plan."),
                "guarantee": ("What should be clarified about guarantees?", "Clarify coverage, exclusions, correction periods, and post-delivery responsibility."),
                "support": ("What should be clarified about support after delivery?", "Clarify support scope, contact channels, and which maintenance is included or separately billed."),
                "client_count": ("What evidence helps evaluate a provider's experience?", "Use documented examples, clear scope, and output quality rather than an unverified headline count."),
                "certification": ("How can certifications be verified?", "Check the issuing source, validity, and relevance to the service."),
                "testimonial": ("How can customer experiences be evaluated reliably?", "Look for attributable experiences with clear sources and verifiable details."),
            }
        return replacements.get(topic, ("What should be clarified before choosing?", "Compare the written scope, evidence, responsibilities, and exclusions before deciding."))

    def _faq_heading_is_question(self, heading: str) -> bool:
        value = re.sub(r"^\s*#{3,6}\s+", "", str(heading or "")).strip()
        if value.endswith(("?", "\u061f", "ØŸ")):
            return True
        return bool(
            re.match(
                r"^(?:what|why|how|when|where|who|which|is|are|can|does|do|should|"
                r"\u0645\u0627|\u0645\u0627\u0630\u0627|\u0644\u0645\u0627\u0630\u0627|\u0643\u064a\u0641|\u0645\u062a\u0649|\u0623\u064a\u0646|\u0647\u0644|\u0645\u0646|\u0623\u064a|\u0643\u0645)",
                value,
                re.IGNORECASE,
            )
        )

    def _sanitize_commercial_faq_content_legacy(self, content: str, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        """Remove leaked FAQ planning lines while preserving H3 question/answer blocks."""
        if not content or not self._is_commercial_faq_section(section, state):
            return content
        if not re.search(r"(?m)^#{3,6}\s+", content):
            return content

        lines = str(content).splitlines()
        first_h3_idx = next((idx for idx, line in enumerate(lines) if re.match(r"^\s*#{3,6}\s+", line)), None)
        if first_h3_idx is None:
            return content

        sanitized_lines: List[str] = []
        if first_h3_idx > 0:
            self._record_section_quality_issue(section, "faq_preamble_removed")

        blocks: List[List[str]] = []
        current: List[str] = []
        for line in lines[first_h3_idx:]:
            if re.match(r"^\s*#{3,6}\s+", line) and current:
                blocks.append(current)
                current = [line]
            else:
                current.append(line)
        if current:
            blocks.append(current)

        leak_pattern = re.compile(
            r"^\s*(?:ابدأ|وضح|قارن|اسأل|راجع|حدد|تأكد|start|clarify|compare|ask|review|define|check)\b",
            re.IGNORECASE,
        )
        seen_paragraphs = set()
        for block in blocks:
            if not block:
                continue
            heading = block[0]
            body_text = "\n".join(block[1:]).strip()
            paragraphs = [p.strip() for p in re.split(r"\n\s*\n", body_text) if p.strip()]
            kept: List[str] = []
            for idx, paragraph in enumerate(paragraphs):
                key = re.sub(r"\s+", " ", paragraph).casefold()
                if key in seen_paragraphs:
                    self._record_section_quality_issue(section, "faq_duplicate_answer_removed")
                    continue
                if idx > 0 and leak_pattern.search(paragraph):
                    self._record_section_quality_issue(section, "faq_repair_leak_removed")
                    continue
                seen_paragraphs.add(key)
                kept.append(paragraph)
            sanitized_lines.append(heading.strip())
            if kept:
                sanitized_lines.append("\n\n".join(kept))
            sanitized_lines.append("")

        return "\n".join(sanitized_lines).strip()

    def _sanitize_commercial_faq_content(self, content: str, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        """Sanitize FAQ blocks, remove planning leakage, and downgrade unsupported brand questions."""
        if not content or not self._is_commercial_faq_section(section, state):
            return content
        if not re.search(r"(?m)^#{3,6}\s+", content):
            self._record_section_quality_issue(section, "faq_missing_h3_blocks")
            return content

        lines = str(content).splitlines()
        first_h3_idx = next((idx for idx, line in enumerate(lines) if re.match(r"^\s*#{3,6}\s+", line)), None)
        if first_h3_idx is None:
            return content
        if first_h3_idx > 0:
            self._record_section_quality_issue(section, "faq_preamble_removed")

        blocks: List[List[str]] = []
        current: List[str] = []
        for line in lines[first_h3_idx:]:
            if re.match(r"^\s*#{3,6}\s+", line) and current:
                blocks.append(current)
                current = [line]
            else:
                current.append(line)
        if current:
            blocks.append(current)

        legacy_leak_pattern = re.compile(
            r"^\s*(?:Ø§Ø¨Ø¯Ø£|ÙˆØ¶Ø­|Ù‚Ø§Ø±Ù†|Ø§Ø³Ø£Ù„|Ø±Ø§Ø¬Ø¹|Ø­Ø¯Ø¯|ØªØ£ÙƒØ¯|"
            r"\u0627\u0628\u062f\u0623|\u0627\u0628\u062f\u0626|\u0648\u0636\u062d|\u0642\u0627\u0631\u0646|"
            r"\u0627\u0633\u0623\u0644|\u0627\u0633\u0626\u0644|\u0631\u0627\u062c\u0639|\u062d\u062f\u062f|\u062a\u0623\u0643\u062f|"
            r"start|clarify|compare|ask|review|define|check)\b",
            re.IGNORECASE,
        )
        seen_answers: set[str] = set()
        seen_questions: set[str] = set()
        sanitized_blocks: List[str] = []

        for block in blocks:
            heading = block[0].strip() if block else ""
            question = re.sub(r"^\s*#{3,6}\s+", "", heading).strip()
            if not self._faq_heading_is_question(heading):
                self._record_section_quality_issue(section, "faq_non_question_heading_removed")
                continue
            if not question.endswith(("?", "\u061f", "ØŸ")):
                question += "\u061f" if re.search(r"[\u0600-\u06FF]", question) else "?"
                heading = f"### {question}"
            question_key = re.sub(r"[^\w\u0600-\u06FF]+", " ", question).strip().casefold()
            if not question_key or question_key in seen_questions:
                self._record_section_quality_issue(section, "faq_duplicate_question_removed")
                continue

            body_text = "\n".join(block[1:]).strip()
            paragraphs = [item.strip() for item in re.split(r"\n\s*\n", body_text) if item.strip()]
            kept: List[str] = []
            for idx, paragraph in enumerate(paragraphs):
                answer_key = re.sub(r"\s+", " ", paragraph).strip().casefold()
                if not answer_key or answer_key in seen_answers:
                    self._record_section_quality_issue(section, "faq_duplicate_answer_removed")
                    continue
                if self._is_faq_planning_text(paragraph) or (kept and legacy_leak_pattern.search(paragraph)):
                    self._record_section_quality_issue(section, "faq_repair_leak_removed")
                    continue
                seen_answers.add(answer_key)
                kept.append(paragraph)

            topic = self._faq_sensitive_topic(question)
            brand_question = self._faq_question_references_brand(question, state)
            answer_mentions_brand = any(self._brand_name_in_text(item, state) for item in kept)
            if topic and (brand_question or answer_mentions_brand) and not self._faq_has_supporting_evidence(topic, state):
                replacement_question, replacement_answer = self._faq_market_guidance_replacement(topic, state)
                heading = f"### {replacement_question}"
                question_key = re.sub(r"[^\w\u0600-\u06FF]+", " ", replacement_question).strip().casefold()
                kept = [replacement_answer]
                self._record_section_quality_issue(section, f"faq_unsupported_brand_question_downgraded:{topic}")
                action = {"action": "downgraded_to_market_guidance", "topic": topic, "original_question": question}
                if action not in section.setdefault("faq_evidence_actions", []):
                    section["faq_evidence_actions"].append(action)
                logger.warning(
                    "[commercial_faq_evidence_gate] Downgraded unsupported brand FAQ topic=%s question='%s'.",
                    topic,
                    question,
                )

            if not kept:
                self._record_section_quality_issue(section, "faq_empty_answer_removed")
                continue
            if question_key in seen_questions:
                self._record_section_quality_issue(section, "faq_duplicate_question_removed")
                continue
            seen_questions.add(question_key)
            sanitized_blocks.append(f"{heading}\n" + "\n\n".join(kept))

        return "\n\n".join(sanitized_blocks).strip()

    def _evaluate_section_role_fulfillment(self, section: Dict[str, Any], content: str, state: Dict[str, Any]) -> Dict[str, Any]:
        """Small deterministic semantic gate for section job fulfillment."""
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return {"fulfillment_status": "satisfied", "fulfillment_reason": "non-commercial"}

        role = str(section.get("commercial_section_role") or "").lower()
        job = str((section.get("section_intent_snapshot") or {}).get("section_job") or "").lower()
        text = str(content or "")
        folded = text.casefold()
        heading = str(section.get("heading_text") or "")

        services_like = role in {"offer_scope", "features_included"} or job in {"offer_scope", "features_included"}
        if services_like:
            criteria_cues = re.findall(
                r"\b(compare|check|ask|verify|evaluate|criteria|choose|review)\b|قارن|تأكد|اسأل|راجع|قيّم|اختيار|معايير",
                text,
                re.IGNORECASE,
            )
            if len(criteria_cues) >= 3:
                return {
                    "fulfillment_status": "weak",
                    "fulfillment_reason": "role drift: services/features section reads like evaluation criteria",
                    "role_drift_cues": len(criteria_cues),
                }

        if self._is_project_like_section(section):
            records = self._project_records_from_narrative_pack(state, section, limit=5)
            required = self._project_records_required_for_proof(records, state)
            if required and not self._content_mentions_any_project_record(text, required):
                return {
                    "fulfillment_status": "unsupported",
                    "fulfillment_reason": "project proof missed target-relevant safe project records",
                    "required_project_names": [record.get("name") for record in required],
                }
            if (
                re.search(r"\b(testimonials?|reviews?|client experiences?|customer stories)\b", heading, re.IGNORECASE)
                or re.search(r"تجارب العملاء|آراء العملاء|تقييمات العملاء|شهادات العملاء", heading)
            ) and not self._pack_has_explicit_testimonial_evidence(state):
                return {
                    "fulfillment_status": "unsupported",
                    "fulfillment_reason": "testimonial/client-experience heading without explicit testimonial evidence",
                }

        if self._is_commercial_faq_section(section, state):
            faq_body_lines = [
                line.strip()
                for line in text.splitlines()
                if line.strip() and not re.match(r"^\s*#{3,6}\s+", line)
            ]
            if any(self._is_faq_planning_text(line) for line in faq_body_lines):
                return {
                    "fulfillment_status": "unsupported",
                    "fulfillment_reason": "faq repair leak remains in final content",
                }
            if re.search(r"faq_preamble_removed|faq_repair_leak_removed|faq_duplicate_answer_removed", " ".join(section.get("section_quality_issues", []))):
                return {
                    "fulfillment_status": "weak",
                    "fulfillment_reason": "faq repair leak cleaned from final content",
                }
            h3_count = len(re.findall(r"(?m)^#{3,6}\s+", text))
            if h3_count < 3:
                return {"fulfillment_status": "weak", "fulfillment_reason": "faq section has too few H3 question blocks"}
            for block in re.split(r"(?m)(?=^#{3,6}\s+)", text):
                if not re.match(r"^\s*#{3,6}\s+", block):
                    continue
                lines = block.splitlines()
                if not self._faq_heading_is_question(lines[0]) or not "\n".join(lines[1:]).strip():
                    return {
                        "fulfillment_status": "weak",
                        "fulfillment_reason": "faq section contains a malformed question or empty answer",
                    }

        if role == "comparison" or str(section.get("section_type") or "").lower() == "comparison":
            if not any(self._is_decision_useful_markdown_table(block) for _, _, block in self._extract_markdown_tables(text)):
                return {"fulfillment_status": "weak", "fulfillment_reason": "comparison section lacks a decision-useful table"}

        return {"fulfillment_status": "satisfied", "fulfillment_reason": "section role fulfilled"}

    def _content_has_markdown_table(self, content: str) -> bool:
        """Detect a valid markdown table with a header and separator row."""
        return any(self._is_valid_markdown_table(block) for _, _, block in self._extract_markdown_tables(content))

    def _is_project_like_section(self, section: Dict[str, Any]) -> bool:
        """Return True only for dedicated proof/portfolio sections (not differentiation headings)."""
        section_type = str(section.get("section_type") or "").lower()
        role = str(section.get("commercial_section_role") or self._commercial_section_role_for_section(section, {})).lower()
        axis = str(section.get("taxonomy_axis") or "").lower()
        return (
            role == "proof"
            or section_type in {"proof", "case_study", "case-study"}
            or axis in {"brand_projects", "projects"}
        )

    def _extract_markdown_tables(self, content: str) -> List[tuple]:
        """Return contiguous markdown-table-looking blocks as (start_line, end_line, block)."""
        if not content:
            return []
        lines = str(content).splitlines()
        tables: List[tuple] = []
        idx = 0
        while idx < len(lines):
            line = lines[idx]
            if "|" not in line or not line.strip().startswith("|"):
                idx += 1
                continue
            start = idx
            block_lines = []
            while idx < len(lines) and "|" in lines[idx] and lines[idx].strip().startswith("|"):
                block_lines.append(lines[idx])
                idx += 1
            if len(block_lines) >= 2:
                tables.append((start, idx, "\n".join(block_lines)))
        return tables

    def _is_markdown_separator_row(self, line: str) -> bool:
        text = str(line or "").strip()
        if not text.startswith("|"):
            return False
        cells = [cell.strip() for cell in text.strip("|").split("|")]
        return bool(cells) and all(re.fullmatch(r":?-{3,}:?", cell or "") for cell in cells)

    def _markdown_table_cells(self, line: str) -> List[str]:
        return [cell.strip() for cell in str(line or "").strip().strip("|").split("|")]

    def _is_valid_markdown_table(self, table: str) -> bool:
        lines = [line.strip() for line in str(table or "").splitlines() if line.strip()]
        if len(lines) < 3:
            return False
        if self._is_markdown_separator_row(lines[0]):
            return False
        if not self._is_markdown_separator_row(lines[1]):
            return False
        header_count = len(self._markdown_table_cells(lines[0]))
        sep_count = len(self._markdown_table_cells(lines[1]))
        if header_count < 2 or header_count != sep_count:
            return False
        return any(len(self._markdown_table_cells(row)) == header_count for row in lines[2:])

    def _is_decision_useful_markdown_table(self, table: str) -> bool:
        """Reject visually valid but empty/repetitive commercial tables."""
        if not self._is_valid_markdown_table(table):
            return False
        lines = [line.strip() for line in str(table or "").splitlines() if line.strip()]
        headers = [cell.casefold() for cell in self._markdown_table_cells(lines[0])]
        rows = [self._markdown_table_cells(line) for line in lines[2:]]
        rows = [row for row in rows if len(row) == len(headers)]
        if len(rows) < 2:
            return False

        placeholder_terms = {
            "", "-", "n/a", "none", "same", "similar", "option", "option 1", "option 2",
            "placeholder", "to be added", "not specified", "general",
        }
        flattened = [
            re.sub(r"\s+", " ", cell).strip().casefold()
            for row in rows
            for cell in row
        ]
        informative_cells = [
            cell for cell in flattened
            if cell not in placeholder_terms and len(cell) >= 3
        ]
        if len(informative_cells) < max(4, len(rows)):
            return False

        normalized_rows = {
            " | ".join(re.sub(r"\s+", " ", cell).strip().casefold() for cell in row)
            for row in rows
        }
        if len(normalized_rows) < 2:
            return False

        discriminating_columns = 0
        for idx in range(len(headers)):
            values = {
                re.sub(r"\s+", " ", row[idx]).strip().casefold()
                for row in rows
                if idx < len(row)
                and re.sub(r"\s+", " ", row[idx]).strip().casefold() not in placeholder_terms
            }
            if len(values) >= 2:
                discriminating_columns += 1
        return discriminating_columns >= 1

    def _count_valid_markdown_tables(self, content: str) -> int:
        return sum(1 for _, _, block in self._extract_markdown_tables(content) if self._is_valid_markdown_table(block))

    def _count_useful_markdown_tables(self, content: str) -> int:
        return sum(
            1
            for _, _, block in self._extract_markdown_tables(content)
            if self._is_decision_useful_markdown_table(block)
        )

    def _replace_first_markdown_table(self, content: str, replacement: str) -> str:
        tables = self._extract_markdown_tables(content)
        if not tables:
            return content
        start, end, _ = tables[0]
        lines = str(content or "").splitlines()
        new_lines = lines[:start] + str(replacement or "").splitlines() + lines[end:]
        return "\n".join(new_lines).strip()

    def _replace_first_markdown_table_region(self, content: str, replacement: str) -> str:
        """Replace the first table and nearby orphan pipe rows left by malformed project tables."""
        tables = self._extract_markdown_tables(content)
        if not tables:
            return content
        start, end, _ = tables[0]
        lines = str(content or "").splitlines()
        extended_end = end
        while extended_end < len(lines):
            stripped = lines[extended_end].strip()
            if not stripped:
                break
            if "|" in stripped and not stripped.startswith("#"):
                extended_end += 1
                continue
            break
        replacement_lines = str(replacement or "").splitlines() if str(replacement or "").strip() else []
        new_lines = lines[:start] + replacement_lines + lines[extended_end:]
        return "\n".join(new_lines).strip()

    def _limit_markdown_tables(self, content: str, max_tables: int = 2) -> str:
        """Keep the first valid tables and convert later table rows to bullets."""
        tables = self._extract_markdown_tables(content)
        if not tables:
            return content
        valid_seen = 0
        lines = str(content or "").splitlines()
        replacements: List[tuple] = []
        for start, end, block in tables:
            if not self._is_valid_markdown_table(block):
                continue
            valid_seen += 1
            if valid_seen <= max_tables:
                continue
            rows = [row for row in block.splitlines()[2:] if row.strip()]
            bullets = []
            for row in rows:
                cells = [cell for cell in self._markdown_table_cells(row) if cell]
                if cells:
                    bullets.append("- " + " - ".join(cells))
            replacements.append((start, end, bullets or []))
        for start, end, bullets in reversed(replacements):
            lines = lines[:start] + bullets + lines[end:]
        return "\n".join(lines).strip()

    _REPAIR_PLACEHOLDER_SUBSTRINGS: tuple = (
        "اكتب النتيجة المطلوبة",
        "حدد ما سيدخل في الخدمة",
        "حدد ما سيدخل",
        "define the outcome the reader expects",
        "separate included work from items that need separate approval",
    )

    def _line_has_repair_placeholder_leak(self, line: str) -> bool:
        """True when a line contains instructional repair/template text, not reader copy."""
        folded = re.sub(r"\s+", " ", str(line or "")).strip().casefold()
        if not folded:
            return False
        return any(token.casefold() in folded for token in self._REPAIR_PLACEHOLDER_SUBSTRINGS)

    def _content_has_repair_placeholder_leak(self, content: str) -> bool:
        return any(self._line_has_repair_placeholder_leak(line) for line in str(content or "").splitlines())

    def _strip_repair_placeholder_leaks(self, content: str) -> tuple:
        """Remove instructional placeholder lines; return (cleaned_content, changed)."""
        lines = str(content or "").splitlines()
        if not lines:
            return content, False
        kept: List[str] = []
        removed = False
        for line in lines:
            if self._line_has_repair_placeholder_leak(line):
                removed = True
                continue
            kept.append(line)
        cleaned = "\n".join(kept).strip()
        return cleaned, removed

    def _sanitize_repair_placeholder_leaks(
        self,
        content: str,
        section: Dict[str, Any],
    ) -> str:
        """Safe-repair: strip template leaks and flag; never inject replacement prose."""
        cleaned, removed = self._strip_repair_placeholder_leaks(content)
        if removed:
            self._record_section_quality_issue(section, "repair_placeholder_leak_removed")
            logger.info(
                "[repair_placeholder_gate] Removed instructional placeholder lines from section '%s'.",
                section.get("heading_text", ""),
            )
        if self._content_has_repair_placeholder_leak(cleaned):
            self._record_section_quality_issue(section, "repair_placeholder_leak")
        return cleaned

    def _section_body_integrity_issues(
        self,
        content: str,
        section: Dict[str, Any],
        state: Dict[str, Any],
    ) -> List[str]:
        """Blocking integrity checks after sanitization/normalization."""
        issues: List[str] = []
        text = str(content or "").strip()
        is_intro = self._is_commercial_intro_section(section, state)
        if not text and not is_intro:
            return ["section_body_empty"]

        for line in str(content or "").splitlines():
            match = re.match(r"^(\s*)(\d+)\.\s*(.*)$", line)
            if match and not str(match.group(3) or "").strip():
                issues.append("empty_numbered_list_item")
                break

        if self._is_commercial_process_section(section, state) and self._count_ordered_list_items(text) < 2:
            issues.append("process_section_insufficient_steps")

        if self._content_has_repair_placeholder_leak(text):
            issues.append("repair_placeholder_leak")

        return list(dict.fromkeys(issues))

    def _normalize_ordered_lists(self, content: str) -> str:
        """Remove empty ordered-list markers and renumber each list block from 1."""
        if not content:
            return content
        lines = str(content).splitlines()
        current = 1
        in_list = False
        out: List[str] = []
        for line in lines:
            match = re.match(r"^(\s*)(\d+)\.\s*(.*)$", line)
            if match:
                indent, _, body = match.groups()
                body = body.strip()
                if not body:
                    continue
                out.append(f"{indent}{current}. {body}")
                current += 1
                in_list = True
                continue
            if in_list and line.strip():
                out.append(line)
                continue
            in_list = False
            current = 1
            out.append(line)
        return "\n".join(out)

    def _text_contains_keyword(self, text: str, keyword: str) -> bool:
        """Loose exact-keyword check that tolerates markdown spacing/case."""
        if not text or not keyword:
            return False

        def normalize(value: str) -> str:
            return re.sub(r"\s+", " ", re.sub(r"[*_`]+", "", str(value or ""))).strip().casefold()

        return normalize(keyword) in normalize(text)

    def _is_commercial_intro_section(self, section: Dict[str, Any], state: Dict[str, Any]) -> bool:
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return False
        section_type = str(section.get("section_type") or "").lower()
        heading_level = str(section.get("heading_level") or "").upper()
        role = str(section.get("commercial_section_role") or "").lower()
        return section_type in {"introduction", "intro"} or heading_level == "INTRO" or role == "intro"

    def _brand_name_in_text(self, text: str, state: Dict[str, Any]) -> bool:
        folded = str(text or "").casefold()
        names = [
            state.get("display_brand_name"),
            state.get("brand_name"),
            state.get("official_brand_name"),
            *(state.get("brand_aliases") or []),
        ]
        return any(str(name or "").strip().casefold() in folded for name in names if str(name or "").strip())

    def _build_commercial_intro_hook(self, state: Dict[str, Any], content: str = "") -> str:
        """Build a neutral first paragraph hook that includes the primary keyword."""
        primary_keyword = str(
            state.get("primary_keyword")
            or state.get("raw_title")
            or state.get("input_data", {}).get("title")
            or ""
        ).strip()
        if not primary_keyword:
            return ""
        lang = str(state.get("article_language") or state.get("input_data", {}).get("article_language") or "").lower()
        is_ar = lang.startswith("ar") or bool(re.search(r"[\u0600-\u06FF]", primary_keyword + " " + str(content or "")))
        if is_ar:
            return (
                f"\u0627\u062e\u062a\u064a\u0627\u0631 {primary_keyword} \u0642\u062f \u064a\u0628\u062f\u0648 \u0642\u0631\u0627\u0631\u0627 \u0628\u0633\u064a\u0637\u0627\u060c "
                "\u0644\u0643\u0646 \u0627\u0644\u062a\u062d\u062f\u064a \u0627\u0644\u062d\u0642\u064a\u0642\u064a \u064a\u0638\u0647\u0631 \u0639\u0646\u062f\u0645\u0627 \u062a\u062d\u0627\u0648\u0644 \u0645\u0642\u0627\u0631\u0646\u0629 \u062e\u064a\u0627\u0631\u0627\u062a \u0643\u062b\u064a\u0631\u0629 "
                "\u0628\u0645\u0639\u0627\u064a\u064a\u0631 \u0648\u0627\u0636\u062d\u0629: \u0645\u0627 \u0627\u0644\u0630\u064a \u0633\u062a\u062d\u0635\u0644 \u0639\u0644\u064a\u0647\u061f \u0648\u0643\u064a\u0641 \u062a\u0642\u0644\u0644 \u0645\u062e\u0627\u0637\u0631\u0629 \u0627\u0644\u0627\u062e\u062a\u064a\u0627\u0631 \u0627\u0644\u062e\u0627\u0637\u0626\u061f "
                "\u0648\u0645\u0627 \u0627\u0644\u062f\u0644\u064a\u0644 \u0627\u0644\u0630\u064a \u064a\u062c\u0639\u0644 \u0627\u0644\u0642\u0631\u0627\u0631 \u0623\u0642\u0631\u0628 \u0644\u0646\u062a\u064a\u062c\u0629 \u0639\u0645\u0644\u064a\u0629\u061f"
            )
        return (
            f"Finding {primary_keyword} should not start with random browsing. "
            "It starts by clarifying the real need, comparing the options on practical grounds, "
            "and choosing the path that reduces risk and moves the reader toward a clear result."
        )

    def _fix_intro_spacing(self, text: str, state: Dict[str, Any]) -> str:
        primary_keyword = str(
            state.get("primary_keyword")
            or state.get("raw_title")
            or state.get("input_data", {}).get("title")
            or ""
        ).strip()
        if primary_keyword:
            text = str(text or "").replace(f"{primary_keyword}لا", f"{primary_keyword} لا")
        text = re.sub(r"([^\s])(\u0644\u0627\s+\u064a)", r"\1 \2", str(text or ""))
        return text

    def _normalize_intro_paragraph(self, paragraph: str) -> str:
        """Normalize an intro paragraph for stable duplicate detection."""
        normalized = str(paragraph or "")
        normalized = re.sub(r"\[([^\]]+)\]\((?:https?://|www\.)[^)]+\)", r"\1", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"(?:https?://|www\.)\S+", "", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"[*_`#>]+", " ", normalized)
        normalized = re.sub(r"[\u064B-\u065F\u0670]", "", normalized)
        normalized = re.sub(r"[^\w\u0600-\u06FF]+", " ", normalized, flags=re.UNICODE)
        return re.sub(r"\s+", " ", normalized).strip().casefold()

    def _is_intro_cta(self, paragraph: str, state: Dict[str, Any]) -> bool:
        """Return True when a paragraph asks the reader to take a practical next step."""
        text = str(paragraph or "")
        folded = text.casefold()
        brand_url = str(state.get("brand_url") or "").strip().casefold()
        has_link = bool(re.search(r"\[[^\]]+\]\((?:https?://|www\.)[^)]+\)", text, flags=re.IGNORECASE))
        has_url = bool(re.search(r"(?:https?://|www\.)\S+", text, flags=re.IGNORECASE))
        if has_link or has_url or (brand_url and brand_url in folded):
            return True

        cta_patterns = (
            r"(?:^|\s)(?:\u0631\u0627\u062c\u0639|\u0627\u0628\u062f\u0623|\u0627\u0628\u062f\u0626|\u062a\u0648\u0627\u0635\u0644|\u0632\u0631|\u0627\u0637\u0644\u0628|\u0627\u062d\u062c\u0632|\u0627\u0643\u062a\u0634\u0641|\u062a\u0639\u0631\u0641|\u0642\u0627\u0631\u0646)(?:\s|$)",
            r"\b(?:visit|review|explore|contact|start|request|book|call|compare)\b",
        )
        return any(re.search(pattern, folded, flags=re.IGNORECASE) for pattern in cta_patterns)

    def _is_intro_brand_bridge(self, paragraph: str, state: Dict[str, Any]) -> bool:
        """Identify a non-CTA paragraph that lightly connects the brand to the reader's need."""
        text = str(paragraph or "")
        if not self._brand_name_in_text(text, state) or self._is_intro_cta(text, state):
            return False

        bridge_patterns = (
            r"(?:\u062d\u0644|\u064a\u0633\u0627\u0639\u062f|\u062a\u0633\u0627\u0639\u062f|\u064a\u0642\u062f\u0645|\u062a\u0642\u062f\u0645|\u064a\u0648\u0641\u0631|\u062a\u0648\u0641\u0631|\u064a\u062f\u0639\u0645|\u062a\u062f\u0639\u0645|\u0627\u062d\u062a\u064a\u0627\u062c|\u062e\u062f\u0645\u0629|\u062f\u0648\u0631)",
            r"\b(?:solution|help|helps|provide|provides|offer|offers|support|supports|need|service|role)\b",
        )
        return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in bridge_patterns)

    def _dedupe_intro_paragraphs(self, paragraphs: List[str]) -> List[str]:
        """Remove repeated intro paragraphs while preserving their first occurrence."""
        seen: set[str] = set()
        deduped: List[str] = []
        for paragraph in paragraphs:
            key = self._normalize_intro_paragraph(paragraph)
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(paragraph.strip())
        return deduped

    def _inject_keyword_into_intro_hook(self, hook: str, primary_keyword: str, state: Dict[str, Any]) -> str:
        """Add a missing primary keyword without replacing an otherwise useful hook."""
        if not hook or not primary_keyword or self._text_contains_keyword(hook, primary_keyword):
            return hook
        lang = str(state.get("article_language") or state.get("input_data", {}).get("article_language") or "").lower()
        is_ar = lang.startswith("ar") or bool(re.search(r"[\u0600-\u06FF]", primary_keyword + " " + hook))
        if is_ar:
            return f"\u0639\u0646\u062f \u0627\u0644\u0628\u062d\u062b \u0639\u0646 {primary_keyword}\u060c {hook}"
        return f"When considering {primary_keyword}, {hook}"

    def _build_commercial_intro_brand_bridge(self, state: Dict[str, Any], content: str = "") -> str:
        brand_name = str(state.get("display_brand_name") or state.get("brand_name") or "").strip()
        if not brand_name:
            return ""
        lang = str(state.get("article_language") or state.get("input_data", {}).get("article_language") or "").lower()
        is_ar = lang.startswith("ar") or bool(re.search(r"[\u0600-\u06FF]", str(content or "")))
        if is_ar:
            return (
                f"\u0647\u0646\u0627 \u064a\u0645\u0643\u0646 \u0623\u0646 \u064a\u0638\u0647\u0631 \u062f\u0648\u0631 {brand_name} \u0628\u0634\u0643\u0644 \u062e\u0641\u064a\u0641\u060c "
                "\u0644\u0623\u0646 \u0635\u0641\u062d\u0627\u062a\u0647 \u0627\u0644\u0631\u0633\u0645\u064a\u0629 \u062a\u0633\u0627\u0639\u062f \u0627\u0644\u0642\u0627\u0631\u0626 \u0639\u0644\u0649 \u0641\u0647\u0645 \u0646\u0637\u0627\u0642 \u0627\u0644\u062e\u062f\u0645\u0629 \u0648\u0637\u0631\u064a\u0642\u0629 \u0627\u0644\u062a\u0639\u0627\u0645\u0644 \u0645\u0639\u0647\u0627 "
                "\u062f\u0648\u0646 \u0627\u0644\u062f\u062e\u0648\u0644 \u0641\u064a \u062a\u0641\u0627\u0635\u064a\u0644 \u062a\u0642\u0646\u064a\u0629 \u0645\u0643\u062b\u0641\u0629 \u0645\u0646\u0630 \u0627\u0644\u0628\u062f\u0627\u064a\u0629."
            )
        return (
            f"This is where {brand_name} can be introduced lightly: its official pages help the reader understand "
            "the service scope and next step without turning the introduction into a technical catalog."
        )

    def _build_commercial_intro_cta(self, state: Dict[str, Any], content: str = "") -> str:
        brand_name = str(state.get("display_brand_name") or state.get("brand_name") or "").strip()
        brand_url = str(state.get("brand_url") or "").strip()
        lang = str(state.get("article_language") or state.get("input_data", {}).get("article_language") or "").lower()
        is_ar = lang.startswith("ar") or bool(re.search(r"[\u0600-\u06FF]", str(content or "")))
        if is_ar:
            if brand_url:
                anchor = f"\u0627\u0644\u0645\u0648\u0642\u0639 \u0627\u0644\u0631\u0633\u0645\u064a \u0644\u0640 {brand_name}" if brand_name else "\u0627\u0644\u0645\u0648\u0642\u0639 \u0627\u0644\u0631\u0633\u0645\u064a"
                return (
                    "\u0648\u0644\u0643\u064a \u062a\u0628\u062f\u0623 \u0628\u062e\u0637\u0648\u0629 \u0639\u0645\u0644\u064a\u0629\u060c "
                    f"\u064a\u0645\u0643\u0646\u0643 \u0645\u0631\u0627\u062c\u0639\u0629 [{anchor}]({brand_url}) "
                    "\u062b\u0645 \u0645\u0642\u0627\u0631\u0646\u0629 \u0645\u0627 \u064a\u0638\u0647\u0631 \u0641\u064a\u0647 \u0645\u0639 \u0627\u062d\u062a\u064a\u0627\u062c\u0643 \u0648\u0623\u0648\u0644\u0648\u064a\u0627\u062a\u0643."
                )
            return "\u0648\u0644\u0643\u064a \u062a\u0628\u062f\u0623 \u0628\u062e\u0637\u0648\u0629 \u0639\u0645\u0644\u064a\u0629\u060c \u0642\u0627\u0631\u0646 \u0645\u0627 \u064a\u0638\u0647\u0631 \u0641\u064a \u0635\u0641\u062d\u0627\u062a \u0627\u0644\u0645\u0632\u0648\u062f \u0645\u0639 \u0627\u062d\u062a\u064a\u0627\u062c\u0643 \u0648\u0623\u0648\u0644\u0648\u064a\u0627\u062a\u0643."
        if brand_url:
            anchor = f"{brand_name} official website" if brand_name else "the official website"
            return f"To start practically, review [{anchor}]({brand_url}) and compare what it offers with your needs and priorities."
        return "To start practically, compare the provider's official service details with your needs and priorities."

    def _ensure_commercial_intro_contract(
        self,
        content: str,
        section: Dict[str, Any],
        state: Dict[str, Any],
    ) -> str:
        """Ensure commercial intros use hook, light brand bridge, and soft CTA."""
        if not content or not self._is_commercial_intro_section(section, state):
            return content
        content = self._fix_intro_spacing(content, state)
        primary_keyword = str(
            state.get("primary_keyword")
            or state.get("raw_title")
            or state.get("input_data", {}).get("title")
            or ""
        ).strip()
        if not primary_keyword:
            return content

        paragraphs = [
            self._fix_intro_spacing(paragraph.strip(), state)
            for paragraph in re.split(r"\n\s*\n", str(content).strip())
            if paragraph.strip()
        ]
        if not paragraphs:
            return content
        original_paragraphs = list(paragraphs)
        paragraphs = self._dedupe_intro_paragraphs(paragraphs)
        fallback_keys = set(section.get("_intro_fallback_paragraph_keys") or [])

        hook_was_fallback = False
        hook = next(
            (
                paragraph
                for paragraph in paragraphs
                if not self._is_intro_cta(paragraph, state)
                and not self._is_intro_brand_bridge(paragraph, state)
                and not self._brand_name_in_text(paragraph, state)
                and len(paragraph.split()) >= 12
            ),
            "",
        )
        if not hook:
            hook = self._build_commercial_intro_hook(state, content)
            hook_was_fallback = bool(hook)
        hook = self._inject_keyword_into_intro_hook(hook, primary_keyword, state)
        if hook_was_fallback and hook:
            fallback_keys.add(self._normalize_intro_paragraph(hook))

        brand_bridge = next(
            (
                paragraph
                for paragraph in paragraphs
                if self._is_intro_brand_bridge(paragraph, state) and len(paragraph.split()) <= 80
            ),
            "",
        )
        if not brand_bridge:
            brand_bridge = self._build_commercial_intro_brand_bridge(state, content)
            if brand_bridge:
                fallback_keys.add(self._normalize_intro_paragraph(brand_bridge))

        cta = next((paragraph for paragraph in paragraphs if self._is_intro_cta(paragraph, state)), "")
        if not cta:
            cta = self._build_commercial_intro_cta(state, content)
            if cta:
                fallback_keys.add(self._normalize_intro_paragraph(cta))

        final_paragraphs = self._dedupe_intro_paragraphs([hook, brand_bridge, cta])
        fallback_builders = (
            self._build_commercial_intro_hook(state, content),
            self._build_commercial_intro_brand_bridge(state, content),
            self._build_commercial_intro_cta(state, content),
        )
        for fallback in fallback_builders:
            if len(final_paragraphs) >= 3:
                break
            if fallback:
                final_paragraphs = self._dedupe_intro_paragraphs([*final_paragraphs, fallback])
        final_paragraphs = final_paragraphs[:3]
        section["_intro_fallback_paragraph_keys"] = sorted(fallback_keys)

        first = final_paragraphs[0] if final_paragraphs else ""
        first_has_keyword = self._text_contains_keyword(first, primary_keyword)
        first_mentions_brand = self._brand_name_in_text(first, state)
        changed = [self._normalize_intro_paragraph(p) for p in original_paragraphs] != [
            self._normalize_intro_paragraph(p) for p in final_paragraphs
        ]
        if len(final_paragraphs) == 3 and changed:
            logger.info(
                "[commercial_intro_contract] Enforced intro contract. keyword_present=%s "
                "brand_in_first_paragraph=%s bridge_detected=%s cta_detected=%s",
                first_has_keyword,
                first_mentions_brand,
                any(self._is_intro_brand_bridge(paragraph, state) for paragraph in original_paragraphs),
                any(self._is_intro_cta(paragraph, state) for paragraph in original_paragraphs),
            )
        return "\n\n".join(paragraph.strip() for paragraph in final_paragraphs if paragraph.strip()).strip()

    def _finalize_commercial_intro_surgically(
        self,
        content: str,
        section: Dict[str, Any],
        state: Dict[str, Any],
    ) -> tuple[str, Dict[str, Any]]:
        """Minimally normalize a final intro without generating missing paragraphs."""
        original = str(content or "").strip()
        report = {
            "status": "pass",
            "changed": False,
            "issues": [],
            "paragraph_count": 0,
            "keyword_in_first_paragraph": False,
            "brand_bridge_present": False,
            "soft_cta_present": False,
        }
        if not self._is_commercial_intro_section(section, state):
            return original, report

        primary_keyword = str(
            state.get("primary_keyword")
            or state.get("raw_title")
            or state.get("input_data", {}).get("title")
            or ""
        ).strip()
        normalized_content = self._fix_intro_spacing(original, state)
        paragraphs = self._dedupe_intro_paragraphs([
            self._fix_intro_spacing(paragraph.strip(), state)
            for paragraph in re.split(r"\n\s*\n", normalized_content)
            if paragraph.strip()
        ])
        fallback_keys = set(section.get("_intro_fallback_paragraph_keys") or [])
        original_paragraphs = list(paragraphs)
        paragraphs = [
            paragraph
            for paragraph in paragraphs
            if self._normalize_intro_paragraph(paragraph) not in fallback_keys
        ]
        removed_fallbacks = len(original_paragraphs) != len(paragraphs)

        hook = next(
            (
                paragraph
                for paragraph in paragraphs
                if not self._brand_name_in_text(paragraph, state)
                and not self._is_intro_cta(paragraph, state)
                and len(paragraph.split()) >= 8
            ),
            "",
        )
        if not hook:
            report["issues"].append("intro_missing_hook")
        elif primary_keyword:
            hook = self._inject_keyword_into_intro_hook(hook, primary_keyword, state)

        bridge = ""
        bridge_claim_issues: List[str] = []
        unsafe_bridge_keys: set[str] = set()
        for paragraph in paragraphs:
            if not self._is_intro_brand_bridge(paragraph, state) or len(paragraph.split()) > 80:
                continue
            cleaned_bridge, claim_issues = self._sanitize_unsupported_brand_claims(
                paragraph,
                state,
                section=section,
                context="body",
                brand_sensitive=True,
            )
            if claim_issues:
                bridge_claim_issues.extend(claim_issues)
                unsafe_bridge_keys.add(self._normalize_intro_paragraph(paragraph))
            if cleaned_bridge.strip() and self._is_intro_brand_bridge(cleaned_bridge, state):
                bridge = cleaned_bridge.strip()
                break
        if not bridge:
            report["issues"].append(
                "intro_brand_bridge_unsupported"
                if bridge_claim_issues
                else "intro_missing_brand_bridge"
            )

        cta_candidates = [
            paragraph for paragraph in paragraphs
            if self._is_intro_cta(paragraph, state)
        ]
        cta = cta_candidates[0].strip() if cta_candidates else ""
        brand_url = str(state.get("brand_url") or "").strip()
        if not cta:
            report["issues"].append("intro_missing_soft_cta")
        elif brand_url and brand_url.casefold() not in cta.casefold():
            report["issues"].append("intro_cta_missing_brand_url")

        if hook and primary_keyword and not self._text_contains_keyword(hook, primary_keyword):
            report["issues"].append("intro_keyword_patch_failed")
        if hook and self._brand_name_in_text(hook, state):
            report["issues"].append("intro_hook_mentions_brand")

        if report["issues"]:
            minimally_repaired = [
                paragraph
                for paragraph in paragraphs
                if self._normalize_intro_paragraph(paragraph) not in unsafe_bridge_keys
            ]
            if hook:
                hook_key = next(
                    (
                        idx for idx, paragraph in enumerate(minimally_repaired)
                        if not self._brand_name_in_text(paragraph, state)
                        and not self._is_intro_cta(paragraph, state)
                        and len(paragraph.split()) >= 8
                    ),
                    None,
                )
                if hook_key is not None:
                    minimally_repaired[hook_key] = hook
            result = "\n\n".join(minimally_repaired).strip()
            report.update({
                "status": "needs_revision",
                "changed": result != original or removed_fallbacks,
                "paragraph_count": len(minimally_repaired),
                "keyword_in_first_paragraph": bool(
                    minimally_repaired
                    and primary_keyword
                    and self._text_contains_keyword(minimally_repaired[0], primary_keyword)
                ),
                "brand_bridge_present": bool(bridge),
                "soft_cta_present": bool(cta),
            })
            return result, report

        final_paragraphs = [hook, bridge, cta]
        result = "\n\n".join(final_paragraphs).strip()
        report.update({
            "status": "repaired" if result != original else "pass",
            "changed": result != original or removed_fallbacks,
            "paragraph_count": 3,
            "keyword_in_first_paragraph": self._text_contains_keyword(hook, primary_keyword),
            "brand_bridge_present": True,
            "soft_cta_present": True,
        })
        return result, report

    def _record_final_intro_report(
        self,
        state: Dict[str, Any],
        section: Optional[Dict[str, Any]],
        report: Dict[str, Any],
    ) -> None:
        """Persist final intro status and prevent false-success output."""
        state["final_intro_quality_report"] = dict(report or {})
        if section is not None:
            section["final_intro_quality_report"] = dict(report or {})

        if report.get("status") != "needs_revision":
            if section is not None:
                section["section_quality_issues"] = [
                    issue
                    for issue in section.get("section_quality_issues", [])
                    if not str(issue).startswith("intro_final_enforcement_failed:")
                ]
            state["final_quality_warnings"] = [
                warning
                for warning in state.get("final_quality_warnings", [])
                if not str(warning).startswith("intro_final_enforcement_failed:")
            ]
            if (
                state.get("final_status") == "needs_revision"
                and not state.get("final_quality_warnings")
            ):
                state.pop("final_status", None)
            logger.info(
                "[final_intro_enforcement] status=%s changed=%s keyword=%s bridge=%s cta=%s",
                report.get("status"),
                str(bool(report.get("changed"))).lower(),
                str(bool(report.get("keyword_in_first_paragraph"))).lower(),
                str(bool(report.get("brand_bridge_present"))).lower(),
                str(bool(report.get("soft_cta_present"))).lower(),
            )
            return

        issue = "intro_final_enforcement_failed:" + ",".join(report.get("issues") or ["unknown"])
        if section is not None:
            self._record_section_quality_issue(section, issue)
        warnings = state.setdefault("final_quality_warnings", [])
        if issue not in warnings:
            warnings.append(issue)
        state["final_status"] = "needs_revision"
        logger.warning("[final_intro_enforcement] status=needs_revision issues=%s", report.get("issues"))

    def _finalize_intro_sections_for_output(
        self,
        state: Dict[str, Any],
        sections: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Apply the surgical final intro invariant to assembled section objects."""
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return {"status": "pass", "changed": False, "issues": []}

        intro = next(
            (
                section for section in sections or []
                if self._is_commercial_intro_section(section, state)
            ),
            None,
        )
        if intro is None:
            report = {
                "status": "needs_revision",
                "changed": False,
                "issues": ["intro_section_missing"],
                "paragraph_count": 0,
                "keyword_in_first_paragraph": False,
                "brand_bridge_present": False,
                "soft_cta_present": False,
            }
            self._record_final_intro_report(state, None, report)
            return report

        repaired, report = self._finalize_commercial_intro_surgically(
            intro.get("generated_content", ""),
            intro,
            state,
        )
        intro["generated_content"] = repaired
        self._record_final_intro_report(state, intro, report)
        return report

    def _is_commercial_process_section(self, section: Dict[str, Any], state: Dict[str, Any]) -> bool:
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return False
        role = str(section.get("commercial_section_role") or self._commercial_section_role_for_section(section, state)).lower()
        section_type = str(section.get("section_type") or "").lower()
        return role == "process" or section_type in {"process", "process_or_how", "how_it_works"}

    def _is_commercial_faq_section(self, section: Dict[str, Any], state: Dict[str, Any]) -> bool:
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return False
        role = str(section.get("commercial_section_role") or self._commercial_section_role_for_section(section, state)).lower()
        return role == "faq" or str(section.get("section_type") or "").lower() == "faq"

    def _is_commercial_cta_section(self, section: Dict[str, Any], state: Dict[str, Any]) -> bool:
        if str(state.get("content_type") or "").lower() != "brand_commercial":
            return False
        role = str(section.get("commercial_section_role") or self._commercial_section_role_for_section(section, state)).lower()
        return role == "cta" or str(section.get("section_type") or "").lower() == "conclusion"

    def _count_ordered_list_items(self, content: str) -> int:
        return len(re.findall(r"(?m)^\s*\d+\.\s+\S+", str(content or "")))

    def _ensure_commercial_process_depth(self, content: str, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        """Flag short process sections without injecting instructional fallback prose."""
        if not content or not self._is_commercial_process_section(section, state):
            return content
        if self._count_ordered_list_items(content) >= 4:
            return content
        self._record_section_quality_issue(section, "process_section_insufficient_steps")
        logger.info(
            "[commercial_process_gate] Short process section '%s' flagged; instructional fallback suppressed.",
            section.get("heading_text", ""),
        )
        return content

    # def _ensure_commercial_faq_depth(self, content: str, section: Dict[str, Any], state: Dict[str, Any]) -> str:
    #     """Ensure commercial FAQ sections answer practical objections, not generic filler."""
    #     if not content or not self._is_commercial_faq_section(section, state):
    #         return content
    #     content = self._sanitize_commercial_faq_content(content, section, state)
    #     question_count = len(re.findall(r"(?m)^#{3,6}\s+", content)) + len(re.findall(r"[\?\u061f]", content))
    #     if question_count >= 4:
    #         return self._sanitize_commercial_faq_content(content, section, state)

    #     keyword = str(state.get("primary_keyword") or state.get("raw_title") or "this option").strip()
    #     lang = str(state.get("article_language") or section.get("article_language") or "").lower()
    #     is_ar = lang.startswith("ar") or bool(re.search(r"[\u0600-\u06FF]", content + keyword))
    #     if is_ar:
    #         addition = "\n\n".join(
    #             [
    #                 f"### \u0647\u0644 {keyword} \u0645\u0646\u0627\u0633\u0628 \u0644\u0627\u062d\u062a\u064a\u0627\u062c\u064a \u0627\u0644\u062d\u0627\u0644\u064a\u061f\n\u0627\u0628\u062f\u0623 \u0628\u0645\u0637\u0627\u0628\u0642\u0629 \u0627\u0644\u0646\u0637\u0627\u0642 \u0645\u0639 \u0627\u0644\u0647\u062f\u0641 \u0648\u062d\u062c\u0645 \u0627\u0644\u062a\u0639\u0642\u064a\u062f \u0648\u0645\u0627 \u062a\u062d\u062a\u0627\u062c \u0625\u0644\u0649 \u0642\u064a\u0627\u0633\u0647 \u0628\u0639\u062f \u0627\u0644\u0628\u062f\u0621.",
    #                 "### \u0645\u0627 \u0627\u0644\u0630\u064a \u064a\u062c\u0628 \u062a\u0648\u0636\u064a\u062d\u0647 \u0642\u0628\u0644 \u0627\u0644\u0627\u062a\u0641\u0627\u0642\u061f\n\u0648\u0636\u062d \u0627\u0644\u0645\u0634\u0645\u0648\u0644 \u0648\u0627\u0644\u0627\u0633\u062a\u062b\u0646\u0627\u0621\u0627\u062a \u0648\u0637\u0631\u064a\u0642\u0629 \u0627\u0644\u062a\u0633\u0644\u064a\u0645 \u0648\u0646\u0642\u0627\u0637 \u0627\u0644\u0645\u0631\u0627\u062c\u0639\u0629 \u0642\u0628\u0644 \u0623\u064a \u0627\u0644\u062a\u0632\u0627\u0645.",
    #                 "### \u0643\u064a\u0641 \u0623\u0642\u0627\u0631\u0646 \u0627\u0644\u062a\u0643\u0644\u0641\u0629 \u0628\u0627\u0644\u0642\u064a\u0645\u0629\u061f\n\u0642\u0627\u0631\u0646 \u0645\u0627 \u0633\u062a\u062d\u0635\u0644 \u0639\u0644\u064a\u0647 \u0648\u0645\u062f\u0649 \u0642\u0627\u0628\u0644\u064a\u062a\u0647 \u0644\u0644\u062a\u0637\u0648\u064a\u0631 \u0628\u062f\u0644 \u0627\u0644\u0627\u0643\u062a\u0641\u0627\u0621 \u0628\u0631\u0642\u0645 \u0623\u0648\u0644\u064a.",
    #                 "### \u0645\u0627\u0630\u0627 \u064a\u062d\u062f\u062b \u0628\u0639\u062f \u0628\u062f\u0621 \u0627\u0644\u062a\u0646\u0641\u064a\u0630\u061f\n\u0627\u0633\u0623\u0644 \u0639\u0646 \u0627\u0644\u0645\u0631\u0627\u062d\u0644 \u0648\u0622\u0644\u064a\u0629 \u0627\u0644\u062a\u0648\u0627\u0635\u0644 \u0648\u0645\u0627 \u0627\u0644\u0630\u064a \u064a\u0639\u062a\u0628\u0631 \u062a\u0633\u0644\u064a\u0645\u064b\u0627 \u0645\u0643\u062a\u0645\u0644\u064b\u0627.",
    #             ]
    #         )
    #     else:
    #         addition = "\n\n".join(
    #             [
    #                 f"### Is {keyword} right for my current need?\nMatch the scope to the outcome, complexity, and result you need to measure.",
    #                 "### What should be clarified before agreement?\nClarify inclusions, exclusions, delivery method, and review points before committing.",
    #                 "### How should I compare cost with value?\nCompare what you receive and how adaptable it is, not only the first quoted figure.",
    #                 "### What happens after implementation starts?\nAsk about milestones, communication, review ownership, and what counts as final delivery.",
    #             ]
    #         )
    #     logger.info("[commercial_faq_gate] Added objection-driven FAQ prompts to '%s'.", section.get("heading_text", ""))
    #     return self._sanitize_commercial_faq_content(((content or "").strip() + "\n\n" + addition).strip(), section, state)

    def _ensure_commercial_faq_depth(self, content: str, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        """Ensure commercial FAQ sections answer practical objections, not generic filler."""
        if not content or not self._is_commercial_faq_section(section, state):
            return content
        return self._sanitize_commercial_faq_content(content, section, state)


    def _ensure_commercial_conclusion_cta(self, content: str, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        """Flag missing conclusion CTA without injecting replacement copy."""
        if not content or not self._is_commercial_cta_section(section, state):
            return content
        brand_url = str(state.get("brand_url") or "").strip()
        if not brand_url or brand_url in content:
            return content
        self._record_section_quality_issue(section, "conclusion_missing_brand_url_cta")
        logger.info(
            "[commercial_cta_gate] Missing conclusion CTA for '%s' (auto-inject suppressed).",
            section.get("heading_text", ""),
        )
        return content

    def _apply_commercial_section_quality_gates(
        self,
        content: str,
        section: Dict[str, Any],
        state: Dict[str, Any],
        *,
        skip_intro_contract: bool = False,
    ) -> str:
        """Apply deterministic commercial quality gates consistently after writing."""
        content = self._ensure_project_proof_format(content, section, state)
        content = self._ensure_required_table_content(content, section, state)
        if not skip_intro_contract:
            content = self._ensure_commercial_intro_contract(content, section, state)
        content = self._ensure_commercial_faq_depth(content, section, state)
        content = self._ensure_commercial_conclusion_cta(content, section, state)
        content, claim_issues = self._sanitize_unsupported_brand_claims(
            content,
            state,
            section=section,
            context="body",
        )
        for issue in claim_issues:
            self._record_section_quality_issue(section, f"unsupported_brand_claim_removed:{issue}")
        content = self._normalize_ordered_lists(content)
        content = self._ensure_commercial_process_depth(content, section, state)
        content = self._normalize_ordered_lists(content)
        content = self._sanitize_repair_placeholder_leaks(content, section)
        for issue in self._section_body_integrity_issues(content, section, state):
            self._record_section_quality_issue(section, issue)
        return content

    def _has_minimum_project_table_evidence(self, state: Dict[str, Any], minimum: int = 2) -> bool:
        """Return True only when safe project records are available, not loose routing labels."""
        return len(self._project_records_from_narrative_pack(state, limit=minimum)) >= minimum

    def _legacy_project_records_from_narrative_pack(
        self,
        state: Dict[str, Any],
        section: Optional[Dict[str, Any]] = None,
        limit: int = 6,
    ) -> List[Dict[str, Any]]:
        """Return safe project records from page-scoped narrative briefs for proof rendering.

        Loose routing signals such as ``routing_signals.projects`` are intentionally
        not treated as project truth. They are useful for routing/debug only and can
        contain labels, tools, or metadata fragments.
        """
        source_briefs = []
        if section and isinstance(section.get("section_page_narrative_briefs"), list):
            source_briefs.extend(section.get("section_page_narrative_briefs") or [])
        source_briefs.extend(state.get("brand_page_narrative_briefs") or [])

        noisy_names = {
            "screenshots", "technology stack", "technologies used", "scope of work",
            "services provided", "target", "b2c", "b2b", "name", "location",
            "sector", "objective", "brief", "publish date", "quality assurance",
            "real estatetarget", "real estate target", "management", "deliverables",
            "design services", "websites", "website", "mobile app", "web app", "all",
            "portfolio", "projects", "case study", "case studies",
            "seo", "advertising b2b", "b2c branding", "adobe photoshop",
            "adobe creative cloud", "figma", "react", "react js", "node js",
            "tailwind", "tailwind css", "swift", "java", "laravel", "aws",
        }
        noisy_detail_terms = {
            "screenshots", "technology stack", "technologies used", "scope of work",
            "services provided", "target", "b2c", "b2b", "name", "location",
            "sector", "objective", "brief", "publish date", "quality assurance",
            "real estatetarget", "real estate target", "management", "deliverables",
            "portfolio", "projects", "case study", "case studies", "all",
        }

        def brand_names() -> List[str]:
            names = [
                state.get("display_brand_name"),
                state.get("brand_name"),
                state.get("official_brand_name"),
                *(state.get("brand_aliases") or []),
            ]
            return [str(name).strip().casefold() for name in names if str(name or "").strip()]

        def clean_record_name(value: str) -> str:
            name = re.sub(r"\s+", " ", str(value or "")).strip(" .:-|")
            parts = re.split(r"\s+-\s+", name)
            if len(parts) > 1:
                suffix = parts[-1].strip().casefold()
                if (
                    suffix in brand_names()
                    or "brand" in suffix
                    or "company" in suffix
                    or suffix in {"official", "portfolio", "projects", "case study"}
                ):
                    name = " - ".join(parts[:-1]).strip(" .:-|")
            name = re.sub(
                r"\b(?:websites?|mobile app|mob app|web app|mobile application|web application|design services|seo|all)\b$",
                "",
                name,
                flags=re.IGNORECASE,
            )
            name = re.sub(r"\s+", " ", name).strip(" .:-|")
            folded = name.casefold()
            folded_no_space = re.sub(r"\s+", "", folded)
            noisy_no_space = {re.sub(r"\s+", "", item) for item in noisy_names}
            if folded in noisy_names or folded_no_space in noisy_no_space:
                return ""
            if re.search(
                r"\b(?:screenshots?|technology stack|scope of work|services provided|publish date|objective|audience|target audience)\b",
                name,
                re.IGNORECASE,
            ):
                return ""
            if len(name) < 2 or len(name) > 90:
                return ""
            return name

        def family_key(value: str) -> str:
            key = clean_record_name(value).casefold()
            key = re.sub(
                r"\b(?:mob|mobile|web|website|app|application|platform|ios|android)\b",
                " ",
                key,
                flags=re.IGNORECASE,
            )
            key = re.sub(r"[^\w\s]", " ", key)
            return re.sub(r"\s+", " ", key).strip()

        def clean_record_location(value: str) -> str:
            location = re.sub(r"\s+", " ", str(value or "")).strip(" .:-|")
            if not location or location == "-":
                return ""
            if location.casefold() in {"ing", "in", "on", "at", "location", "project", "sector", "target", "b2b", "b2c"}:
                return ""
            if len(location) > 80:
                return ""
            if re.search(
                r"\b(?:content|application|web application|design|service|services|technology|tools|stack|audience|sector|target|branding|positioning|seo|ui|ux|b2b|b2c)\b",
                location,
                re.IGNORECASE,
            ):
                return ""
            return location

        def location_from_narrative(value: str) -> str:
            text = re.sub(r"\s+", " ", str(value or "")).strip()
            patterns = [
                r"\bLocation\s*:\s*(.{2,80}?)(?=\s+(?:Sector|Audience|Expertise|Services|Project|Technologies|Brief)\s*:|\s+(?:Sector|Audience|Expertise|Services|Project|Technologies|Brief)\b|$)",
                r"\bproject\s+in\s+(.{2,80}?)(?=\s+(?:within|featuring|with|for|sector|and)\b|[.;]|$)",
                r"\bin\s+(.{2,80}?)(?=\s+(?:within|featuring|with|for|sector|and)\b|[.;]|$)",
            ]
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    cleaned = clean_record_location(match.group(1))
                    if cleaned:
                        return cleaned
            return ""

        def sector_from_narrative(value: str) -> str:
            text = re.sub(r"\s+", " ", str(value or "")).strip()
            match = re.search(
                r"\bSector\s*:\s*(.{2,80}?)(?=\s+(?:Audience|Expertise|Services|Project|Technologies|Brief)\s*:|\s+(?:Audience|Expertise|Services|Project|Technologies|Brief)\b|$)",
                text,
                re.IGNORECASE,
            )
            if not match:
                match = re.search(r"\bwithin\s+the\s+(.{2,60}?)\s+sector\b", text, re.IGNORECASE)
            if match:
                sector = re.sub(r"\s+", " ", match.group(1)).strip(" .:-|")
                if sector and len(sector) <= 60:
                    return sector
            return ""

        def page_looks_like_project_source(brief: Dict[str, Any]) -> bool:
            page_type = str(brief.get("page_type") or "").casefold()
            url = str(brief.get("source_url") or brief.get("url") or "").casefold()
            title = str(brief.get("page_title") or "")
            narrative = str(brief.get("narrative_brief") or brief.get("grounded_summary") or "")
            return (
                page_type in {"portfolio", "projects", "case_study", "case-study", "portfolio_listing"}
                or any(segment in url for segment in ("/portfolio", "/project", "/projects", "/case"))
                or bool(re.search(r"\b(project|portfolio|case study)\b", f"{title} {narrative}", re.IGNORECASE))
            )

        def clean_details(items: Any) -> List[str]:
            details = []
            for item in items or []:
                value = re.sub(r"\s+", " ", str(item)).strip(" .:-|")
                folded = value.casefold()
                if not value or folded in noisy_detail_terms:
                    continue
                if re.search(r"\b(?:screenshots?|brief|scope of work|technologies used|services provided)\b", value, re.IGNORECASE):
                    continue
                details.append(value)
            return list(dict.fromkeys(details))

        def add_record(record: Dict[str, Any]) -> None:
            raw_name = re.sub(r"\s+", " ", str(record.get("name") or "")).strip(" .:-|")
            name = clean_record_name(raw_name)
            if not name:
                return
            location = clean_record_location(str(record.get("location") or ""))
            if location:
                leading_location = re.split(r"\s*,\s*", location)[0]
                if leading_location and re.search(rf"\b{re.escape(leading_location)}$", name, re.IGNORECASE):
                    candidate_name = clean_record_name(re.sub(rf"\b{re.escape(leading_location)}$", "", name, flags=re.IGNORECASE))
                    if candidate_name and candidate_name.casefold() not in {"project", "client", "case", "portfolio"}:
                        name = candidate_name
            services = clean_details(record.get("services") or [])
            technologies = clean_details(record.get("technologies") or [])
            sector = re.sub(r"\s+", " ", str(record.get("sector") or "")).strip(" .:-|")
            if sector.casefold() in noisy_names:
                sector = ""
            category = re.sub(r"\s+", " ", str(record.get("category") or "")).strip(" .:-|")
            if name.casefold().endswith("target") and not (services or technologies or location or sector):
                return
            if not any([location, sector, services, technologies, category]):
                return

            key = family_key(name) or name.casefold()
            existing = seen.get(key)
            if existing:
                existing.setdefault("variants", [])
                variant = raw_name
                if variant and variant != existing.get("name") and variant not in existing["variants"]:
                    existing["variants"].append(variant)
                if location and not existing.get("location"):
                    existing["location"] = location
                if sector and not existing.get("sector"):
                    existing["sector"] = sector
                if category and not existing.get("category"):
                    existing["category"] = category
                existing["services"] = list(dict.fromkeys([*(existing.get("services") or []), *services]))
                existing["technologies"] = list(dict.fromkeys([*(existing.get("technologies") or []), *technologies]))
                return

            record_out = {
                "name": name,
                "location": location,
                "sector": sector,
                "category": category,
                "services": services,
                "technologies": technologies,
                "variants": [raw_name] if raw_name and raw_name != name else [],
            }
            seen[key] = record_out
            records.append(record_out)

        records: List[Dict[str, Any]] = []
        seen: Dict[str, Dict[str, Any]] = {}
        for brief in source_briefs:
            if not isinstance(brief, dict):
                continue
            signals = brief.get("routing_signals") if isinstance(brief.get("routing_signals"), dict) else {}
            for record in signals.get("project_records") or []:
                if not isinstance(record, dict):
                    continue
                add_record(record)

            if signals.get("project_records"):
                continue
            narrative = str(brief.get("narrative_brief") or brief.get("grounded_summary") or "")
            if not page_looks_like_project_source(brief):
                continue
            project_names = [clean_record_name(str(brief.get("page_title") or ""))]
            location = location_from_narrative(narrative)
            if not location:
                for item in signals.get("project_locations") or signals.get("explicit_geography") or []:
                    location = clean_record_location(str(item))
                    if location:
                        break
            sector = sector_from_narrative(narrative)
            for name in project_names:
                if not name:
                    continue
                add_record(
                    {
                        "name": name,
                        "location": location,
                        "sector": sector,
                        "services": signals.get("services") or [],
                        "technologies": signals.get("technologies") or [],
                    }
                )

        try:
            from src.services.brand_evidence_service import _area_relevance_score_for_text

            records.sort(
                key=lambda record: (
                    -_area_relevance_score_for_text(
                        " ".join(
                            [
                                str(record.get("name") or ""),
                                str(record.get("location") or ""),
                                str(record.get("sector") or ""),
                                " ".join(record.get("services") or []),
                                " ".join(record.get("technologies") or []),
                            ]
                        ),
                        state,
                    ),
                    str(record.get("name") or "").casefold(),
                )
            )
        except Exception:
            records.sort(key=lambda record: str(record.get("name") or "").casefold())

        return records[:limit]

    def _project_records_from_narrative_pack(
        self,
        state: Dict[str, Any],
        section: Optional[Dict[str, Any]] = None,
        limit: int = 6,
    ) -> List[Dict[str, Any]]:
        """Use the single knowledge-pack project truth path for proof rendering."""
        from src.services.brand_evidence_service import (
            build_safe_project_records_from_knowledge_pack,
        )

        return build_safe_project_records_from_knowledge_pack(
            state,
            section=section,
            limit=limit,
        )

    def _build_project_proof_cards(
        self,
        records: List[Dict[str, Any]],
        state: Dict[str, Any],
        section: Optional[Dict[str, Any]] = None,
        limit: int = 4,
    ) -> str:
        """Render safe project proof as compact narrative bullets instead of a default table."""
        if not records:
            return ""
        lang = str(state.get("article_language") or (section or {}).get("article_language") or "").lower()
        heading = str((section or {}).get("heading_text") or "")
        is_ar = lang.startswith("ar") or bool(re.search(r"[\u0600-\u06FF]", heading))
        lines: List[str] = []
        for record in records[:limit]:
            name = str(record.get("name") or "").strip()
            if not name:
                continue
            detail_parts: List[str] = []
            location = str(record.get("location") or "").strip()
            sector = str(record.get("sector") or "").strip()
            category = str(record.get("category") or "").strip()
            services = [str(item).strip() for item in (record.get("services") or []) if str(item).strip()][:3]
            technologies = [str(item).strip() for item in (record.get("technologies") or []) if str(item).strip()][:3]
            variants = [str(item).strip() for item in (record.get("variants") or []) if str(item).strip()][:2]
            if is_ar:
                if location:
                    detail_parts.append(f"موقعه المرصود: {location}")
                if sector:
                    detail_parts.append(f"القطاع: {sector}")
                if category:
                    detail_parts.append(f"نوع العمل: {category}")
                if variants:
                    detail_parts.append("تشمل النسخ المرصودة: " + "، ".join(variants))
                if services:
                    detail_parts.append("الخدمات المرصودة: " + "، ".join(services))
                if technologies:
                    detail_parts.append("التقنيات/الأدوات المرصودة: " + "، ".join(technologies))
                details = "؛ ".join(detail_parts) if detail_parts else "ورد كمثال مشروع في صفحات البراند."
                lines.append(f"- **{name}**: {details}.")
            else:
                if location:
                    detail_parts.append(f"observed location: {location}")
                if sector:
                    detail_parts.append(f"sector: {sector}")
                if category:
                    detail_parts.append(f"work type: {category}")
                if variants:
                    detail_parts.append("observed variants: " + ", ".join(variants))
                if services:
                    detail_parts.append("observed services: " + ", ".join(services))
                if technologies:
                    detail_parts.append("observed technologies/tools: " + ", ".join(technologies))
                details = "; ".join(detail_parts) if detail_parts else "listed as a project example in the brand pages"
                lines.append(f"- **{name}**: {details}.")
        return "\n".join(lines).strip()

    def _table_plan_for_section(self, section: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        """Plan useful tables and avoid placeholder visual tables."""
        section_type = str(section.get("section_type") or "").lower()
        role = str(section.get("commercial_section_role") or "").lower()
        axis = str(section.get("taxonomy_axis") or "").lower()
        visual_format = str(section.get("visual_format") or "").lower()
        if section_type in {"introduction", "intro", "conclusion", "faq"} or role in {"intro", "cta", "faq"}:
            return {"requires_table": False, "table_type": "none"}
        if self._is_project_like_section(section):
            return {"requires_table": False, "table_type": "project_proof_cards"}
        if role == "comparison" or section_type == "comparison" or axis == "comparison":
            return {"requires_table": True, "table_type": "decision_comparison"}
        if role == "cost_value":
            return {"requires_table": visual_format == "table", "table_type": "cost_factors"}
        if role == "features_included" and visual_format == "table":
            return {"requires_table": True, "table_type": "feature_checklist"}
        return {"requires_table": False, "table_type": "none"}

    def _build_required_section_table(self, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        """Create a compact fallback table when a required visual table is missing."""
        lang = str(state.get("article_language") or section.get("article_language") or "").lower()
        heading = str(section.get("heading_text") or "")
        section_type = str(section.get("section_type") or "").lower()
        axis = str(section.get("taxonomy_axis") or "").lower()
        if section_type == "comparison":
            axis = "comparison"
        is_ar = lang.startswith("ar") or bool(re.search(r"[\u0600-\u06FF]", heading))
        role = str(section.get("commercial_section_role") or "").lower()
        table_type = str(section.get("table_type") or "").lower()
        project_like_section = (
            role == "proof"
            or section_type in {"proof", "case_study", "case-study"}
            or axis in {"brand_projects", "projects"}
            or bool(re.search(r"\b(project|projects|portfolio|case study|case studies)\b", heading, re.IGNORECASE))
            or bool(re.search(r"[\u0645][\u0634][\u0627][\u0631][\u064a][\u0639]|[\u0646][\u0645][\u0627][\u0630][\u062c]|[\u0623][\u0639][\u0645][\u0627][\u0644]", heading))
        )

        if project_like_section:
            records = self._project_records_from_narrative_pack(state, section, limit=5)
            explicit_project_table = table_type in {"project_evidence_table", "project_table"}
            if len(records) < 2 or not explicit_project_table:
                return self._build_project_proof_cards(records, state, section, limit=5)
            rows = []
            for record in records:
                detail_values = list(dict.fromkeys(
                    [
                        *(str(item).strip() for item in (record.get("services") or []) if str(item).strip()),
                        *(str(item).strip() for item in (record.get("technologies") or []) if str(item).strip()),
                    ]
                ))[:4]
                rows.append(
                    "| {name} | {location} | {sector} | {details} |".format(
                        name=record.get("name") or "-",
                        location=record.get("location") or "-",
                        sector=record.get("sector") or "-",
                        details=", ".join(detail_values) or "-",
                    )
                )
            if is_ar:
                return "\n".join(
                    [
                        "| \u0627\u0644\u0645\u0634\u0631\u0648\u0639 | \u0627\u0644\u0645\u0648\u0642\u0639 \u0627\u0644\u0645\u0631\u0635\u0648\u062f | \u0627\u0644\u0642\u0637\u0627\u0639 | \u062e\u062f\u0645\u0627\u062a/\u062a\u0642\u0646\u064a\u0627\u062a \u0645\u0631\u0635\u0648\u062f\u0629 |",
                        "|---|---|---|---|",
                        *rows,
                    ]
                )
            return "\n".join(
                [
                    "| Project | Observed location | Sector | Observed services/technologies |",
                    "|---|---|---|---|",
                    *rows,
                ]
            )

        if role == "cost_value" or table_type == "cost_factors" or section_type in {"pricing", "packages"} or axis == "pricing":
            if is_ar:
                return "\n".join(
                    [
                        "| \u0639\u0627\u0645\u0644 \u0627\u0644\u0642\u064a\u0645\u0629 | \u0643\u064a\u0641 \u064a\u0624\u062b\u0631 \u0639\u0644\u0649 \u0627\u0644\u0642\u0631\u0627\u0631 | \u0645\u0627 \u064a\u062c\u0628 \u062a\u0648\u0636\u064a\u062d\u0647 |",
                        "|---|---|---|",
                        "| \u0646\u0637\u0627\u0642 \u0627\u0644\u0627\u062d\u062a\u064a\u0627\u062c | \u064a\u062d\u062f\u062f \u062d\u062c\u0645 \u0627\u0644\u0639\u0645\u0644 \u0648\u0645\u0633\u062a\u0648\u0649 \u0627\u0644\u062a\u062e\u0635\u064a\u0635 | \u0645\u0627 \u0627\u0644\u0645\u0634\u0645\u0648\u0644 \u0648\u0645\u0627 \u0627\u0644\u062e\u0627\u0631\u062c \u0639\u0646 \u0627\u0644\u0646\u0637\u0627\u0642 |",
                        "| \u0645\u0631\u062d\u0644\u0629 \u0627\u0644\u062a\u0637\u0628\u064a\u0642 | \u062a\u0624\u062b\u0631 \u0639\u0644\u0649 \u0627\u0644\u0648\u0642\u062a \u0648\u0627\u0644\u062a\u0639\u0642\u064a\u062f | \u062e\u0637\u0648\u0627\u062a \u0627\u0644\u062a\u0633\u0644\u064a\u0645 \u0648\u0645\u0646 \u064a\u0639\u062a\u0645\u062f \u0643\u0644 \u0645\u0631\u062d\u0644\u0629 |",
                        "| \u0627\u0644\u062f\u0639\u0645 \u0628\u0639\u062f \u0627\u0644\u0625\u0637\u0644\u0627\u0642 | \u064a\u062d\u062f\u062f \u0627\u0644\u0642\u064a\u0645\u0629 \u0628\u0639\u062f \u0627\u0644\u062a\u0646\u0641\u064a\u0630 | \u0645\u062f\u0649 \u0627\u0644\u0645\u062a\u0627\u0628\u0639\u0629 \u0648\u062d\u062f\u0648\u062f\u0647\u0627 |",
                    ]
                )
            return "\n".join(
                [
                    "| Value factor | How it affects the decision | What to clarify |",
                    "|---|---|---|",
                    "| Scope | Defines effort and customization | What is included and excluded |",
                    "| Delivery stage | Affects time and complexity | Milestones and approvals |",
                    "| After-launch support | Shapes long-term value | Follow-up scope and boundaries |",
                ]
            )

        if role == "features_included" or table_type == "feature_checklist":
            if is_ar:
                return "\n".join(
                    [
                        "| \u0627\u0644\u0639\u0646\u0635\u0631 | \u0645\u0627 \u064a\u062d\u0635\u0644 \u0639\u0644\u064a\u0647 \u0627\u0644\u0642\u0627\u0631\u0626 | \u0643\u064a\u0641 \u064a\u0633\u0627\u0639\u062f \u0627\u0644\u0642\u0631\u0627\u0631 |",
                        "|---|---|---|",
                        "| \u0627\u0644\u0646\u0637\u0627\u0642 | \u062a\u062d\u062f\u064a\u062f \u0645\u0627 \u062a\u0634\u0645\u0644\u0647 \u0627\u0644\u062e\u062f\u0645\u0629 | \u064a\u0645\u0646\u0639 \u062a\u0648\u0642\u0639\u0627\u062a \u063a\u064a\u0631 \u0648\u0627\u0636\u062d\u0629 |",
                        "| \u0627\u0644\u062a\u0646\u0641\u064a\u0630 | \u062e\u0637\u0648\u0627\u062a \u0639\u0645\u0644 \u0642\u0627\u0628\u0644\u0629 \u0644\u0644\u0645\u0631\u0627\u062c\u0639\u0629 | \u064a\u0633\u0647\u0644 \u0645\u062a\u0627\u0628\u0639\u0629 \u0627\u0644\u062a\u0642\u062f\u0645 |",
                        "| \u0627\u0644\u0642\u0627\u0628\u0644\u064a\u0629 \u0644\u0644\u062a\u0637\u0648\u064a\u0631 | \u0645\u0631\u0648\u0646\u0629 \u0644\u0644\u062a\u0639\u062f\u064a\u0644 \u0623\u0648 \u0627\u0644\u062a\u0648\u0633\u0639 | \u064a\u062f\u0639\u0645 \u0642\u0631\u0627\u0631\u064b\u0627 \u0623\u0637\u0648\u0644 \u0645\u062f\u0649 |",
                    ]
                )
            return "\n".join(
                [
                    "| Element | What the reader gets | How it helps the decision |",
                    "|---|---|---|",
                    "| Scope | Clear included work | Prevents unclear expectations |",
                    "| Delivery | Reviewable execution steps | Makes progress easier to track |",
                    "| Scalability | Room to adapt or expand | Supports a longer-term decision |",
                ]
            )

        option_labels = [
            re.sub(r"\s+", " ", str(item)).split(":", 1)[0].strip(" .:-|")
            for item in (section.get("subheadings") or [])
            if str(item).strip()
        ]
        option_labels = [label for label in option_labels if label]
        if len(option_labels) >= 2:
            left, right = option_labels[0], option_labels[1]
            if is_ar:
                return "\n".join(
                    [
                        f"| \u0648\u062c\u0647 \u0627\u0644\u0645\u0642\u0627\u0631\u0646\u0629 | {left} | {right} | \u0645\u0627 \u062a\u0641\u062d\u0635\u0647 \u0642\u0628\u0644 \u0627\u0644\u0642\u0631\u0627\u0631 |",
                        "|---|---|---|---|",
                        "| \u0627\u0644\u0647\u062f\u0641 | \u064a\u0646\u0627\u0633\u0628 \u0627\u062d\u062a\u064a\u0627\u062c\u064b\u0627 \u0645\u062d\u062f\u062f\u064b\u0627 | \u064a\u0646\u0627\u0633\u0628 \u0627\u062d\u062a\u064a\u0627\u062c\u064b\u0627 \u0623\u0648\u0633\u0639 \u0623\u0648 \u0623\u0643\u062b\u0631 \u062a\u062e\u0635\u064a\u0635\u064b\u0627 | \u0645\u0627 \u064a\u062a\u062d\u0642\u0642 \u0628\u0639\u062f \u0627\u0644\u062a\u0646\u0641\u064a\u0630 |",
                        "| \u0627\u0644\u062a\u062e\u0635\u064a\u0635 | \u0623\u0633\u0647\u0644 \u0641\u064a \u0627\u0644\u0645\u0642\u0627\u0631\u0646\u0629 | \u064a\u062d\u062a\u0627\u062c \u0646\u0637\u0627\u0642\u064b\u0627 \u0648\u0645\u0631\u0627\u062c\u0639\u0629 \u0623\u0648\u0636\u062d | \u062d\u062f\u0648\u062f \u0627\u0644\u062a\u0639\u062f\u064a\u0644 \u0648\u0627\u0644\u062a\u0648\u0633\u0639 |",
                        "| \u0627\u0644\u0623\u0646\u0633\u0628 \u0644\u0647 | \u0627\u062d\u062a\u064a\u0627\u062c \u0648\u0627\u0636\u062d \u0648\u0645\u0628\u0627\u0634\u0631 | \u0645\u0634\u0631\u0648\u0639 \u064a\u062d\u062a\u0627\u062c \u0645\u0631\u0648\u0646\u0629 \u0623\u0643\u0628\u0631 | \u0627\u0644\u0645\u0648\u0627\u0631\u062f \u0648\u0627\u0644\u0623\u0648\u0644\u0648\u064a\u0627\u062a |",
                    ]
                )
            return "\n".join(
                [
                    f"| Comparison point | {left} | {right} | What to check |",
                    "|---|---|---|---|",
                    "| Goal | Fits a clear, narrower need | Fits a broader or more customized need | Desired outcome |",
                    "| Customization | Easier to compare | Needs clearer scope and review | Boundaries for change and growth |",
                    "| Best fit | Direct and well-defined need | Project with more moving parts | Resources and priorities |",
                ]
            )

        if section_type == "comparison" or axis == "comparison" or role == "comparison":
            return ""

        topic_blob = " ".join(
            str(value or "")
            for value in [
                heading,
                state.get("primary_keyword"),
                state.get("raw_title"),
                " ".join(section.get("subheadings") or []),
            ]
        ).casefold()
        # Keep fallback tables domain-neutral. Industry-specific segmentation belongs
        # in SERP/strategy evidence, not in hardcoded production fallback logic.
        real_estate_like = False
        location_like = (
            section_type == "location"
            or axis == "location_area"
            or bool(re.search(r"\b(location|area|district|region|zone|city|neighborhood)\b", topic_blob, re.IGNORECASE))
        )
        if section_type == "comparison" or axis == "comparison":
            location_like = False
        understanding = section.get("section_brand_understanding") if isinstance(section.get("section_brand_understanding"), dict) else {}
        noisy_project_names = {
            "screenshots", "technology stack", "technologies used", "scope of work",
            "services provided", "target", "b2c", "b2b", "name", "location",
            "sector", "objective", "brief", "publish date", "real estate target",
            "technology & tools used", "quality assurance",
        }

        def clean_project_name(value: str) -> str:
            name = re.sub(r"\s+", " ", str(value or "")).strip(" .:-|")
            name = re.sub(r"\s*-\s*(?:creative minds|brandco).*$", "", name, flags=re.IGNORECASE).strip()
            name = re.sub(
                r"\b(?:websites?|mobile app|web app|design services|seo|all)\b",
                " ",
                name,
                flags=re.IGNORECASE,
            )
            name = re.sub(r"\s+", " ", name).strip(" .:-|")
            folded_name = name.casefold()
            folded_no_space = re.sub(r"\s+", "", folded_name)
            noisy_no_space = {re.sub(r"\s+", "", item) for item in noisy_project_names}
            if folded_name in noisy_project_names or folded_no_space in noisy_no_space:
                return ""
            return name

        def clean_location_value(value: str) -> str:
            location = re.sub(r"\s+", " ", str(value or "")).strip(" .:-|")
            if not location or location == "-":
                return "-"
            if location.casefold() in {"ing", "in", "on", "at", "location", "project", "sector", "target", "b2b", "b2c"}:
                return "-"
            if len(location) > 60:
                return "-"
            if re.search(
                r"\b(?:content|application|web application|design|service|services|technology|tools|stack|audience|sector|target|branding|positioning|seo|ui|ux|b2b|b2c)\b",
                location,
                re.IGNORECASE,
            ):
                return "-"
            return location

        def extract_project_location_from_narrative(narrative: str) -> str:
            text = re.sub(r"\s+", " ", str(narrative or "")).strip()
            patterns = [
                r"\bproject\s+in\s+([A-Z][A-Za-z .'-]{2,45}(?:,\s*[A-Z][A-Za-z .'-]{2,45})?)\b",
                r"\bin\s+([A-Z][A-Za-z .'-]{2,45}(?:,\s*[A-Z][A-Za-z .'-]{2,45})?)\s+(?:within|featuring|with|for|sector|and)\b",
                r"(?:\u0641\u064a)\s+([\u0600-\u06FF\s]{2,45})(?:\s+\u0645\u0639|\s+\u0644|\s+\u0648|[.،])",
            ]
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if not match:
                    continue
                candidate = re.split(
                    r"\b(?:within|featuring|with|for|sector|services|technology|target|audience)\b|[.،؛]",
                    match.group(1),
                    maxsplit=1,
                    flags=re.IGNORECASE,
                )[0]
                cleaned = clean_location_value(candidate)
                if cleaned != "-":
                    return cleaned
            return "-"

        def valid_project_record(record: Dict[str, Any]) -> bool:
            name = re.sub(r"\s+", " ", str(record.get("name") or "")).strip(" .:-|")
            if not name or name.casefold() in noisy_project_names:
                return False
            if re.search(r"(?:target|scope|stack|screenshots?)$", name, re.IGNORECASE):
                return False
            details_values = [
                str(record.get("location") or "").strip(),
                str(record.get("sector") or "").strip(),
                *(str(item).strip() for item in (record.get("services") or []) if str(item).strip()),
                *(str(item).strip() for item in (record.get("technologies") or []) if str(item).strip()),
            ]
            clean_details = [
                value for value in details_values
                if value and value != "-" and value.casefold() not in noisy_project_names
            ]
            return bool(clean_details)

        # Get selected narrative briefs from section or state
        selected_briefs = section.get("section_page_narrative_briefs")
        has_narratives = bool(selected_briefs) or bool(state.get("brand_page_narrative_briefs"))
        
        if has_narratives:
            if not selected_briefs:
                from src.services.brand_evidence_service import select_section_page_narrative_briefs
                selected_briefs = select_section_page_narrative_briefs(section, state, max_briefs=5)
                
            project_records = []
            from urllib.parse import urlparse
            for brief in (selected_briefs or []):
                page_type = str(brief.get("page_type") or "").casefold()
                url_path = urlparse(str(brief.get("source_url") or brief.get("url") or "")).path.casefold()
                signals = brief.get("routing_signals") if isinstance(brief.get("routing_signals"), dict) else {}
                is_project = (
                    page_type in {"portfolio", "projects", "case_study", "case-study"}
                    or any(segment in url_path for segment in ["/projects", "/project", "/portfolio", "/case"])
                    or bool(signals.get("projects"))
                )
                if is_project:
                    name = clean_project_name(str(brief.get("page_title") or "").strip())
                    if not name and signals.get("projects"):
                        name = clean_project_name(str(signals["projects"][0]).strip())
                    if not name or name.casefold() in noisy_project_names:
                        continue
                        
                    narrative = str(brief.get("narrative_brief") or "")
                    location = extract_project_location_from_narrative(narrative)
                    if location == "-" and signals.get("project_locations"):
                        location = clean_location_value(str(signals["project_locations"][0]).strip())
                    elif location == "-" and signals.get("explicit_geography"):
                        location = clean_location_value(str(signals["explicit_geography"][0]).strip())
                    if not location:
                        location = "-"
                        
                    sector = "-"
                    sector_match = re.search(r"(?:sector|industry|field|القطاع|مجال)\s*:\s*([A-Za-z0-9\u0600-\u06FF\s\-]{2,40})", narrative, re.IGNORECASE)
                    if sector_match:
                        sector = sector_match.group(1).strip()
                        
                    services_list = signals.get("services") or []
                    tech_list = signals.get("technologies") or []
                    
                    project_records.append({
                        "name": name,
                        "location": location,
                        "sector": sector,
                        "services": services_list,
                        "technologies": tech_list,
                    })
        else:
            project_records = [
                record for record in (understanding.get("relevant_project_records") or [])
                if isinstance(record, dict) and valid_project_record(record)
            ]

        if project_records:
            try:
                from src.services.brand_evidence_service import _area_relevance_score_for_text

                project_records = sorted(
                    project_records,
                    key=lambda record: (
                        -_area_relevance_score_for_text(
                            " ".join(
                                [
                                    str(record.get("name") or ""),
                                    str(record.get("location") or ""),
                                    str(record.get("sector") or ""),
                                    " ".join(str(item) for item in (record.get("services") or [])),
                                    " ".join(str(item) for item in (record.get("technologies") or [])),
                                ]
                            ),
                            state,
                        ),
                        str(record.get("name") or "").casefold(),
                    ),
                )
            except Exception:
                pass

        project_like_section = (
            section_type in {"proof", "case_study", "case-study"}
            or axis in {"brand_projects", "projects"}
            or bool(re.search(r"\b(project|projects|portfolio|case study|case studies)\b", heading, re.IGNORECASE))
            or bool(re.search(r"[\u0645][\u0634][\u0627][\u0631][\u064a][\u0639]|[\u0646][\u0645][\u0627][\u0630][\u062c]|[\u0623][\u0639][\u0645][\u0627][\u0644]", heading))
        )

        if project_like_section:
            if len(project_records) < 2:
                if len(project_records) == 1:
                    p = project_records[0]
                    name = p.get("name")
                    location = p.get("location") or "-"
                    sector = p.get("sector") or "-"
                    if is_ar:
                        return f"\nيقدم المشروع الرئيسي المرصود {name} في {location} نموذجًا واقعيًا لخدمات الشركة في قطاع {sector}.\n"
                    else:
                        return f"\nThe primary observed project {name} in {location} demonstrates the company's capabilities in the {sector} sector.\n"
                return ""
            rows: List[str] = []
            for record in project_records[:5]:
                name = str(record.get("name") or "").strip()
                location = str(record.get("location") or "").strip() or "-"
                sector = str(record.get("sector") or "").strip() or "-"
                details_values = [
                    *(str(item).strip() for item in (record.get("services") or []) if str(item).strip()),
                    *(str(item).strip() for item in (record.get("technologies") or []) if str(item).strip()),
                ]
                details = ", ".join(list(dict.fromkeys(details_values))[:4]) or "-"
                if is_ar:
                    rows.append(f"| {name} | {location} | {sector} | {details} |")
                else:
                    rows.append(f"| {name} | {location} | {sector} | {details} |")
            if is_ar:
                return (
                    "| \u0627\u0644\u0645\u0634\u0631\u0648\u0639 | \u0627\u0644\u0645\u0648\u0642\u0639 \u0627\u0644\u0645\u0631\u0635\u0648\u062f | \u0627\u0644\u0642\u0637\u0627\u0639 | \u062e\u062f\u0645\u0627\u062a/\u062a\u0642\u0646\u064a\u0627\u062a \u0645\u0631\u0635\u0648\u062f\u0629 |\n"
                    "|---|---|---|---|\n"
                    + "\n".join(rows)
                )
            return (
                "| Project | Observed Location | Sector | Observed Services/Technologies |\n"
                "|---|---|---|---|\n"
                + "\n".join(rows)
            )

        def build_comparison_table(arabic: bool) -> str:
            subheadings = [str(item).strip() for item in (section.get("subheadings") or []) if str(item).strip()]
            options: List[str] = []
            for item in subheadings:
                label = item.split(":", 1)[0].strip(" .:-|")
                if label and len(label) <= 80:
                    options.append(label)
            local_global = bool(
                re.search(r"\b(local|global|international|traditional|agency|freelancer)\b", heading, re.IGNORECASE)
                or re.search(
                    "\u0645\u062d\u0644|\u0639\u0627\u0644\u0645|\u062a\u0642\u0644\u064a\u062f|\u0641\u0631\u064a\u0644\u0627\u0646\u0633|\u0634\u0631\u0643",
                    heading,
                )
            )
            if len(options) >= 2:
                left, right = options[0], options[1]
                if arabic:
                    return (
                        f"| \u0648\u062c\u0647 \u0627\u0644\u0645\u0642\u0627\u0631\u0646\u0629 | {left} | {right} |\n"
                        "|---|---|---|\n"
                        "|\u0627\u0644\u0647\u062f\u0641| \u064a\u0646\u0627\u0633\u0628 \u0627\u062d\u062a\u064a\u0627\u062c\u064b\u0627 \u0645\u062d\u062f\u062f\u064b\u0627 \u0648\u0648\u0627\u0636\u062d\u064b\u0627 | \u064a\u0646\u0627\u0633\u0628 \u0627\u062d\u062a\u064a\u0627\u062c\u064b\u0627 \u0623\u0643\u062b\u0631 \u062a\u0639\u0642\u064a\u062f\u064b\u0627 \u0623\u0648 \u0646\u0645\u0648\u064b\u0627 |\n"
                        "|\u0627\u0644\u062a\u062e\u0635\u064a\u0635| \u0623\u0642\u0644 \u063a\u0627\u0644\u0628\u064b\u0627 \u0648\u0623\u0633\u0647\u0644 \u0641\u064a \u0627\u0644\u0645\u0642\u0627\u0631\u0646\u0629 | \u0623\u0639\u0644\u0649 \u0648\u064a\u062d\u062a\u0627\u062c \u0646\u0637\u0627\u0642\u064b\u0627 \u0623\u0648\u0636\u062d |\n"
                        "|\u0627\u0644\u062a\u0634\u063a\u064a\u0644| \u0623\u0628\u0633\u0637 \u0641\u064a \u0627\u0644\u0625\u0637\u0644\u0627\u0642 \u0648\u0627\u0644\u0645\u062a\u0627\u0628\u0639\u0629 | \u064a\u062d\u062a\u0627\u062c \u0645\u0631\u0627\u062c\u0639\u0629 \u0648\u062a\u0643\u0627\u0645\u0644\u0627\u062a \u0623\u0643\u062b\u0631 |\n"
                        "|\u0645\u0627 \u064a\u062c\u0628 \u062d\u0633\u0645\u0647| \u0627\u0644\u0645\u062d\u062a\u0648\u0649\u060c \u0627\u0644\u062a\u0635\u0645\u064a\u0645\u060c \u0648\u062d\u062f\u0648\u062f \u0627\u0644\u062e\u062f\u0645\u0629 | \u0627\u0644\u0648\u0638\u0627\u0626\u0641\u060c \u0627\u0644\u062a\u0643\u0627\u0645\u0644\u0627\u062a\u060c \u0648\u062e\u0637\u0629 \u0627\u0644\u062a\u0637\u0648\u064a\u0631 |"
                    )
                return (
                    f"| Comparison point | {left} | {right} |\n"
                    "|---|---|---|\n"
                    "| Goal | Fits a clear, narrower need | Fits a broader or more complex need |\n"
                    "| Customization | Usually easier to compare | More flexible but needs clearer scope |\n"
                    "| Operation | Simpler to launch and maintain | Needs more integrations and review |\n"
                    "| Clarify before deciding | Content, design, and service limits | Functions, integrations, and growth plan |"
                )
            if arabic and local_global:
                return (
                    "|\u0648\u062c\u0647 \u0627\u0644\u0645\u0642\u0627\u0631\u0646\u0629| \u062e\u064a\u0627\u0631 \u0645\u062d\u0644\u064a | \u062e\u064a\u0627\u0631 \u0639\u0627\u0644\u0645\u064a |\n"
                    "|---|---|---|\n"
                    "|\u0627\u0644\u062a\u0648\u0627\u0635\u0644| \u0623\u0633\u0647\u0644 \u0641\u064a \u0627\u0644\u0644\u063a\u0629 \u0648\u0641\u0631\u0648\u0642 \u0627\u0644\u062a\u0648\u0642\u064a\u062a | \u0642\u062f \u064a\u062d\u062a\u0627\u062c \u062a\u0646\u0633\u064a\u0642\u064b\u0627 \u0623\u0643\u062b\u0631 |\n"
                    "|\u0641\u0647\u0645 \u0627\u0644\u0633\u064a\u0627\u0642| \u0623\u0642\u0631\u0628 \u0644\u0644\u063a\u0629 \u0648\u0639\u0627\u062f\u0627\u062a \u0627\u0644\u0639\u0645\u0644 | \u064a\u062d\u062a\u0627\u062c \u0634\u0631\u062d\u064b\u0627 \u0623\u0648\u0636\u062d \u0644\u0644\u0633\u0648\u0642 |\n"
                    "|\u0627\u0644\u062a\u062e\u0635\u064a\u0635| \u0645\u0646\u0627\u0633\u0628 \u0644\u0627\u062d\u062a\u064a\u0627\u062c\u0627\u062a \u0645\u062d\u0644\u064a\u0629 \u062f\u0642\u064a\u0642\u0629 | \u0642\u0648\u064a \u0645\u0639 \u0646\u0645\u0627\u0630\u062c \u0648\u0646\u0638\u0645 \u0645\u0648\u062d\u062f\u0629 |\n"
                    "|\u0645\u0627 \u062a\u0631\u0627\u062c\u0639\u0647| \u0646\u0637\u0627\u0642 \u0627\u0644\u062f\u0639\u0645 \u0648\u0622\u0644\u064a\u0629 \u0627\u0644\u062a\u0633\u0644\u064a\u0645 | \u0627\u0644\u0644\u063a\u0629\u060c \u0627\u0644\u062a\u0648\u0627\u0641\u0642\u060c \u0648\u062d\u062f\u0648\u062f \u0627\u0644\u062a\u0639\u062f\u064a\u0644 |"
                )
            if arabic:
                return (
                    "|\u0648\u062c\u0647 \u0627\u0644\u0645\u0642\u0627\u0631\u0646\u0629| \u062e\u064a\u0627\u0631 \u0623\u0628\u0633\u0637 | \u062e\u064a\u0627\u0631 \u0623\u0643\u062b\u0631 \u062a\u062e\u0635\u064a\u0635\u064b\u0627 |\n"
                    "|---|---|---|\n"
                    "|\u0627\u0644\u0647\u062f\u0641| \u062d\u0644 \u0633\u0631\u064a\u0639 \u0644\u0627\u062d\u062a\u064a\u0627\u062c \u0645\u062d\u062f\u062f | \u0645\u0644\u0627\u0621\u0645\u0629 \u0627\u062d\u062a\u064a\u0627\u062c \u062e\u0627\u0635 \u0623\u0648 \u0637\u0648\u064a\u0644 \u0627\u0644\u0645\u062f\u0649 |\n"
                    "|\u0627\u0644\u0645\u0631\u0648\u0646\u0629| \u0623\u0642\u0644 \u0644\u0643\u0646\u0647 \u0623\u0633\u0647\u0644 \u0641\u064a \u0627\u0644\u0645\u0642\u0627\u0631\u0646\u0629 | \u0623\u0639\u0644\u0649 \u0648\u064a\u062d\u062a\u0627\u062c \u062a\u0641\u0627\u0635\u064a\u0644 \u0623\u0643\u062b\u0631 |\n"
                    "|\u0627\u0644\u0623\u0646\u0633\u0628 \u0644\u0647| \u0645\u0646 \u064a\u0631\u064a\u062f \u0645\u0633\u0627\u0631\u064b\u0627 \u0648\u0627\u0636\u062d\u064b\u0627 | \u0645\u0646 \u0644\u062f\u064a\u0647 \u0645\u062a\u0637\u0644\u0628\u0627\u062a \u062f\u0642\u064a\u0642\u0629 |"
                )
            return (
                "| Comparison point | Simpler option | More customized option |\n"
                "|---|---|---|\n"
                "| Goal | Solves a clear immediate need | Fits a specific or long-term need |\n"
                "| Flexibility | Lower but easier to compare | Higher but needs more detail |\n"
                "| Best fit | Readers who want a clear path | Readers with precise requirements |"
            )

        if is_ar:
            if location_like:
                subheadings = [str(item).strip() for item in (section.get("subheadings") or []) if str(item).strip()]
                if not subheadings:
                    subheadings = ["المناطق الأقرب للعمل أو الدراسة", "المناطق العائلية الهادئة", "المناطق الاقتصادية"]
                rows = []
                for item in subheadings[:5]:
                    label = re.sub(r"^\s*شقق\s+للايجار\s+في\s+", "", item)
                    label = label.split(":", 1)[0].strip() or item
                    rows.append(f"| {label} | حسب نمط السكن والقرب من الخدمات | تحقق من المساحة، المواصلات، والخدمات القريبة |")
                return (
                    "| المنطقة أو الخيار | الأنسب له | نقطة التحقق قبل القرار |\n"
                    "|---|---|---|\n"
                    + "\n".join(rows)
                )
            if section_type == "comparison" or axis == "comparison":
                if not real_estate_like:
                    return build_comparison_table(True)
                if real_estate_like:
                    return (
                        "| المعيار | خيار إقامة قصيرة | خيار سكن طويل المدى |\n"
                        "|---|---|---|\n"
                        "| التجهيز | غالبًا يحتاج أثاثًا وخدمات جاهزة | يمكن تخصيص الأثاث والتجهيزات حسب الحاجة |\n"
                        "| المرونة | مناسب للزيارات المؤقتة والتنقل السريع | مناسب للعائلات أو الاستقرار لفترة أطول |\n"
                        "| ما يجب مراجعته | شروط الحجز، الخدمات، وموقع الوحدة | العقد، الصيانة، القرب من العمل أو المدارس |\n"
                        "| الأنسب له | الأفراد والزوار والمهام المؤقتة | الأسر والمقيمون لفترة طويلة |"
                    )
                return (
                    "| المعيار | الخيار الأبسط | الخيار الأكثر تخصيصًا |\n"
                    "|---|---|---|\n"
                    "| الهدف | تلبية احتياج مباشر بسرعة | ملاءمة احتياج خاص أو أكثر تعقيدًا |\n"
                    "| المرونة | أقل غالبًا لكنه أسهل في القرار | أعلى لكنه يحتاج مراجعة تفاصيل أكثر |\n"
                    "| ما يجب مراجعته | الشروط الأساسية وما يشمله الخيار | الحدود والاستثناءات والمتطلبات الإضافية |\n"
                    "| الأنسب له | من يريد حلًا واضحًا وسريعًا | من لديه متطلبات دقيقة أو طويلة المدى |"
                )
            if section_type == "pricing" or axis == "pricing":
                if real_estate_like:
                    return (
                        "| عامل التكلفة | كيف يؤثر على الإيجار |\n"
                        "|---|---|\n"
                        "| الموقع والحي | القرب من العمل أو المدارس والخدمات قد يرفع الطلب والسعر |\n"
                        "| المساحة وعدد الغرف | كلما زادت المساحة أو عدد الغرف زادت التكلفة غالبًا |\n"
                        "| التأثيث والخدمات | الشقق المفروشة أو المخدومة تكون أعلى تكلفة عادة |\n"
                        "| مدة العقد | الإيجار الشهري أو القصير قد يختلف عن السنوي حسب شروط المالك |"
                    )
                return (
                    "| عامل التكلفة | كيف يؤثر على القرار |\n"
                    "|---|---|\n"
                    "| نطاق الاحتياج | كلما زادت المتطلبات زادت التكلفة غالبًا |\n"
                    "| مستوى التخصيص | الحلول الجاهزة أبسط، والتخصيص يحتاج وقتًا وجهدًا أكبر |\n"
                    "| الدعم والمتابعة | وجود متابعة مستمرة قد يغير التكلفة النهائية |\n"
                    "| الشروط الإضافية | راجع ما يدخل في السعر وما يحتاج اتفاقًا منفصلًا |"
                )
            return (
                "| العنصر | ما يجب توضيحه في هذا السكشن |\n"
                "|---|---|\n"
                "| الخدمة | ما الذي يحصل عليه العميل فعليًا |\n"
                "| الدليل | مثال أو قدرة مرصودة من المصادر |\n"
                "| الفائدة | أثر الخدمة على قرار العميل |\n"
                "| حدود الادعاء | ما لا يجب وعد القارئ به دون دليل |"
            )

        if section_type == "comparison" or axis == "comparison":
            if not real_estate_like:
                return build_comparison_table(False)
            if real_estate_like:
                return (
                    "| Criterion | Short-stay option | Long-term housing option |\n"
                    "|---|---|---|\n"
                    "| Setup | Usually needs ready furnishings or services | Can be furnished gradually around daily needs |\n"
                    "| Flexibility | Better for temporary stays | Better for family stability or longer plans |\n"
                    "| What to verify | Booking terms, services, and location | Contract, maintenance, commute, and nearby services |\n"
                    "| Best fit | Visitors and temporary assignments | Families and longer-term residents |"
                )
            return (
                "| Criterion | Simpler option | More customized option |\n"
                "|---|---|---|\n"
                "| Goal | Solve a direct need quickly | Fit a more specific or complex need |\n"
                "| Flexibility | Easier to compare but less adaptable | More adaptable but needs more review |\n"
                "| What to verify | Core terms and included items | Boundaries, exclusions, and extra requirements |\n"
                "| Best fit | Readers who need a clear quick path | Readers with detailed long-term requirements |"
            )
        return (
            "| Element | What This Section Should Clarify |\n"
            "|---|---|\n"
            "| Service | What the client actually receives |\n"
            "| Evidence | Observed capability or source-backed example |\n"
            "| Benefit | How it helps the buyer decide |\n"
            "| Claim boundary | What must not be promised without evidence |"
        )

    def _project_table_matches_safe_records(self, table: str, records: List[Dict[str, Any]]) -> bool:
        """Check whether a project table is made from safe project names."""
        if not self._is_valid_markdown_table(table):
            return False
        safe_names = {
            re.sub(r"\s+", " ", str(record.get("name") or "")).strip().casefold()
            for record in records
            if str(record.get("name") or "").strip()
        }
        if not safe_names:
            return False
        rows = [
            self._markdown_table_cells(line)
            for line in str(table or "").splitlines()[2:]
            if line.strip().startswith("|")
        ]
        row_names = [
            re.sub(r"\s+", " ", row[0]).strip().casefold()
            for row in rows
            if row and re.sub(r"\s+", " ", row[0]).strip()
        ]
        if len(row_names) < 2:
            return False
        return all(name in safe_names for name in row_names)

    def _ensure_project_proof_format(self, content: str, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        """Keep project proof sections away from noisy writer-generated tables by default."""
        if not self._is_project_like_section(section):
            return content
        original_heading = str(section.get("heading_text") or "")
        self._downgrade_unsupported_testimonial_heading(section, state)
        if str(section.get("heading_text") or "") != original_heading:
            self._sync_heading_role_contract(
                section,
                state,
                original_heading,
                outline=state.get("outline", []),
                existing_content=content,
            )
        records = self._project_records_from_narrative_pack(state, section, limit=5)
        section["safe_project_records_from_pack"] = records
        required_records = self._project_records_required_for_proof(records, state)
        if required_records and not self._content_mentions_any_project_record(content or "", required_records):
            self._record_section_quality_issue(section, "project_proof_missed_target_relevant_evidence")
            logger.info(
                "[ProjectProofGate] Missing target-relevant project proof for section '%s' (auto-inject suppressed). required=%s",
                section.get("heading_text", ""),
                [record.get("name") for record in required_records],
            )

        existing_tables = self._extract_markdown_tables(content or "")
        table_type = str(section.get("table_type") or "").lower()
        explicit_project_table = table_type in {"project_evidence_table", "project_table"}
        if explicit_project_table and records and existing_tables:
            has_safe_table = any(
                self._project_table_matches_safe_records(block, records)
                and self._is_decision_useful_markdown_table(block)
                for _, _, block in existing_tables
            )
            if has_safe_table:
                return content
            replacement = self._build_required_section_table(section, state)
            if replacement and not self._content_has_repair_placeholder_leak(replacement):
                logger.info(
                    "[ProjectProofGate] Replacing unsafe project table with safe table for section '%s'.",
                    section.get("heading_text", ""),
                )
                return self._replace_first_markdown_table_region(content or "", replacement)
        return content

    def _ensure_required_table_content(self, content: str, section: Dict[str, Any], state: Dict[str, Any]) -> str:
        """Insert a fallback table when the outline required one and none exists."""
        if not section.get("requires_table"):
            return content
        table = self._build_required_section_table(section, state)
        if table and self._content_has_repair_placeholder_leak(table):
            table = ""
            self._record_section_quality_issue(section, "table_placeholder_blocked")
        if not table:
            if section.get("requires_table") and not self._count_useful_markdown_tables(content or ""):
                self._record_section_quality_issue(section, "table_incomplete_or_placeholder")
            return content
        existing_tables = self._extract_markdown_tables(content or "")
        has_valid_table = any(self._is_valid_markdown_table(block) for _, _, block in existing_tables)
        has_useful_table = any(self._is_decision_useful_markdown_table(block) for _, _, block in existing_tables)
        if existing_tables and not has_valid_table:
            if self._content_has_repair_placeholder_leak(table):
                self._record_section_quality_issue(section, "table_placeholder_blocked")
                return content
            logger.info(
                "[TableGate] Replacing malformed table in section '%s'.",
                section.get("heading_text", ""),
            )
            return self._replace_first_markdown_table(content or "", table)
        if has_valid_table and not has_useful_table:
            if self._content_has_repair_placeholder_leak(table):
                self._record_section_quality_issue(section, "table_placeholder_blocked")
                return content
            logger.info(
                "[TableGate] Replacing low-usefulness table in section '%s'.",
                section.get("heading_text", ""),
            )
            return self._replace_first_markdown_table(content or "", table)
        if has_useful_table:
            heading = str(section.get("heading_text") or "")
            section_type = str(section.get("section_type") or "").lower()
            axis = str(section.get("taxonomy_axis") or "").lower()
            project_like_section = (
                section_type in {"proof", "case_study", "case-study"}
                or axis in {"brand_projects", "projects"}
                or bool(re.search(r"\b(project|projects|portfolio|case study|case studies)\b", heading, re.IGNORECASE))
                or bool(re.search(r"[\u0645][\u0634][\u0627][\u0631][\u064a][\u0639]|[\u0646][\u0645][\u0627][\u0630][\u062c]|[\u0623][\u0639][\u0645][\u0627][\u0644]", heading))
            )
            table_pattern = re.compile(r"(?ms)^\s*\|.+\|\s*\n\s*\|[\s:\-|]+\|\s*(?:\n\s*\|.*\|\s*)+")
            if project_like_section:
                replaced = table_pattern.sub(table, content or "", count=1)
                if replaced != (content or ""):
                    return replaced.strip()
                return (table + "\n\n" + (content or "").strip()).strip()
            understanding = section.get("section_brand_understanding") if isinstance(section.get("section_brand_understanding"), dict) else {}
            explicit_names = [
                str(record.get("name") or "").strip()
                for record in (understanding.get("relevant_project_records") or [])
                if (
                    isinstance(record, dict)
                    and str(record.get("name") or "").strip()
                    and str(record.get("target_area_relevance") or "").casefold() == "explicit"
                )
            ]
            if project_like_section and explicit_names and not any(name in (content or "") for name in explicit_names):
                replaced = table_pattern.sub(table, content or "", count=1)
                if replaced != (content or ""):
                    return replaced.strip()
                return (table + "\n\n" + (content or "").strip()).strip()
            return content
        parts = re.split(r"\n\s*\n", (content or "").strip(), maxsplit=1)
        if len(parts) == 2 and parts[0].strip():
            return parts[0].strip() + "\n\n" + table + "\n\n" + parts[1].strip()
        return ((content or "").strip() + "\n\n" + table).strip()

    def _format_section_raw_brand_blocks_for_prompt(self, blocks: List[Dict[str, Any]]) -> str:
        """Format selected raw brand blocks for prompt visibility without dumping full pages."""
        if not blocks:
            return ""

        lines: List[str] = []
        for idx, block in enumerate(blocks, start=1):
            if idx > 1:
                lines.append("")
            lines.append(f"Source URL: {block.get('source_url') or block.get('url') or ''}")
            lines.append(f"Page type: {block.get('page_type') or 'other'}")
            lines.append(f"Heading: {block.get('heading') or ''}")
            lines.append("Observed text:")
            lines.append(str(block.get("observed_text") or block.get("text") or "").strip())
            lines.append("Observed facts:")
            facts = block.get("observed_facts") or []
            if facts:
                for fact in facts[:8]:
                    lines.append(f"- {fact}")
            else:
                lines.append("- No structured facts extracted from this block.")

        return "\n".join(lines).strip()

    def _format_section_brand_page_briefs_for_prompt(self, briefs: List[Dict[str, Any]]) -> str:
        """Format selected page-level brand briefs as readable writer context."""
        if not briefs:
            return ""

        lines: List[str] = []
        for idx, brief in enumerate(briefs, start=1):
            if idx > 1:
                lines.append("")
            lines.append(f"Source URL: {brief.get('source_url') or brief.get('url') or ''}")
            lines.append(f"Page type: {brief.get('page_type') or 'other'}")
            lines.append(f"Page title: {brief.get('page_title') or ''}")
            lines.append("Grounded summary:")
            lines.append(str(brief.get("grounded_summary") or "").strip())

            observed_pairs = [
                ("Observed services", brief.get("observed_services") or []),
                ("Observed projects", brief.get("observed_projects") or []),
                ("Observed technologies", brief.get("observed_technologies") or []),
                ("Observed process steps", brief.get("observed_process_steps") or []),
                ("Explicit geography", brief.get("explicit_geography") or []),
                ("Observed pricing", brief.get("observed_pricing") or []),
            ]
            for label, values in observed_pairs:
                if values:
                    lines.append(f"{label}: {', '.join(str(value) for value in values[:10])}")

            boundaries = brief.get("claim_boundaries") or []
            if boundaries:
                lines.append("Claim boundaries:")
                for boundary in boundaries[:6]:
                    lines.append(f"- {boundary}")

        return "\n".join(lines).strip()

    def _build_brand_section_evidence_audit(
        self,
        section: Dict[str, Any],
        section_brand_page_briefs: List[Dict[str, Any]],
        section_page_narrative_briefs: List[Dict[str, Any]],
        section_raw_brand_blocks: List[Dict[str, Any]],
        section_brand_understanding: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create a compact per-section audit of selected brand evidence."""
        def compact_list(values: Any, limit: int = 8) -> List[str]:
            if not isinstance(values, list):
                values = [values] if values else []
            result: List[str] = []
            seen = set()
            for value in values:
                text = str(value or "").strip()
                if not text:
                    continue
                key = text.casefold()
                if key in seen:
                    continue
                seen.add(key)
                result.append(text[:160])
                if len(result) >= limit:
                    break
            return result

        urls: List[str] = []
        headings: List[str] = []
        for brief in section_page_narrative_briefs or []:
            if not isinstance(brief, dict):
                continue
            urls.append(brief.get("source_url") or brief.get("url") or "")
            headings.append(brief.get("page_title") or brief.get("heading") or "")
        for brief in section_brand_page_briefs or []:
            if not isinstance(brief, dict):
                continue
            urls.append(brief.get("source_url") or brief.get("url") or "")
            headings.append(brief.get("page_title") or brief.get("heading") or "")
        for block in section_raw_brand_blocks or []:
            if not isinstance(block, dict):
                continue
            urls.append(block.get("source_url") or block.get("url") or "")
            headings.append(block.get("heading") or block.get("page_title") or "")

        understanding = section_brand_understanding if isinstance(section_brand_understanding, dict) else {}
        return {
            "section_id": section.get("section_id") or section.get("id"),
            "heading": section.get("heading_text", ""),
            "selected_briefs_count": len(section_brand_page_briefs or []),
            "selected_narrative_briefs_count": len(section_page_narrative_briefs or []),
            "selected_blocks_count": len(section_raw_brand_blocks or []),
            "selected_urls": compact_list(urls),
            "selected_headings": compact_list(headings),
            "relevant_projects": compact_list(understanding.get("relevant_projects", [])),
            "relevant_project_records": [
                {
                    "name": str(record.get("name", ""))[:120],
                    "location": str(record.get("location", ""))[:120],
                    "sector": str(record.get("sector", ""))[:120],
                    "services": compact_list(record.get("services", []), limit=4),
                    "technologies": compact_list(record.get("technologies", []), limit=4),
                    "target_area_relevance": str(record.get("target_area_relevance", ""))[:40],
                }
                for record in (understanding.get("relevant_project_records", []) or [])
                if isinstance(record, dict) and str(record.get("name", "")).strip()
            ][:8],
            "relevant_project_families": compact_list([
                item.get("name") if isinstance(item, dict) else item
                for item in (understanding.get("relevant_project_families", []) or [])
            ]),
            "relevant_services": compact_list(understanding.get("relevant_services", [])),
            "relevant_process_steps": compact_list(understanding.get("relevant_process_steps", [])),
            "relevant_technologies": compact_list(understanding.get("relevant_technologies", [])),
            "not_supported_for_this_section": compact_list(understanding.get("not_supported_for_this_section", [])),
            "safe_project_records_from_pack": [
                {
                    "name": str(record.get("name", ""))[:120],
                    "location": str(record.get("location", ""))[:120],
                    "sector": str(record.get("sector", ""))[:120],
                    "services": compact_list(record.get("services", []), limit=4),
                    "technologies": compact_list(record.get("technologies", []), limit=4),
                }
                for record in (section.get("safe_project_records_from_pack", []) or [])
                if isinstance(record, dict) and str(record.get("name", "")).strip()
            ][:8],
            "writer_truth_trace": section.get("writer_truth_trace", {}),
            "section_quality_issues": compact_list(section.get("section_quality_issues", [])),
            "fulfillment_status": section.get("fulfillment_status", "pending"),
            "fulfillment_reason": section.get("fulfillment_reason", ""),
            "evidence_density": section.get("evidence_density", {}),
            "heading_fidelity": section.get("heading_fidelity", {}),
        }

    async def _step_2_write_sections(self, state: Dict[str, Any]) -> Dict[str, Any]:
        input_data = state.get("input_data", {})
        title = input_data.get("title", "Untitled")
        state = await self._ensure_brand_evidence_state_current(
            state,
            reason="pre_section_writing",
        )
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
            self._ensure_heading_role_contract_current(
                section,
                state,
                outline=outline,
                index=idx,
            )
            if not section.get("section_contract"):
                section["section_contract"] = self._build_section_contract(section, outline, idx, state)
            self._enrich_section_contract(section, outline, idx, state)
            self._apply_commercial_section_role(section, state, idx, len(outline))
            self._enforce_commercial_role_contract(section, state)
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

        self._ensure_heading_role_contract_current(
            section,
            state,
            outline=state.get("outline", []),
            index=section_index,
        )
        section_id = section.get("section_id") or section.get("id")
        brand_url = state.get("brand_url")
        brand_link_used = state.get("brand_link_used", False)
        section_type = (section.get("section_type") or "").lower()
        no_usable_brand_evidence = bool(state.get("brand_evidence_failure_mode"))

        # Always allow the introduction to use the brand link, regardless of state.
        is_introduction = section_type == "introduction"
        can_use_brand_link = bool(brand_url) and not no_usable_brand_evidence and (is_introduction or not brand_link_used)

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
        if is_introduction and brand_url and not no_usable_brand_evidence:
            assigned = section.setdefault("assigned_links", [])
            existing_urls = {
                (lnk.get("url") if isinstance(lnk, dict) else lnk)
                for lnk in assigned
            }
            if brand_url not in existing_urls:
                assigned.insert(0, {"url": brand_url, "text": f"Brand Homepage ({brand_url})"})
                logger.info(f"[brand_link] Injected brand homepage link into introduction: {brand_url}")

        used_phrases = state.get("used_phrases", [])

        brand_label = state.get("display_brand_name") or state.get("brand_name") or ""
        if no_usable_brand_evidence:
            brand_context = (
                f"Brand name: {brand_label}. "
                "The crawler did not collect usable brand page text. Treat this as name-only context. "
                "No page-by-page brand knowledge pack is available. "
                "Do not describe brand services, listings, processes, pricing, geography, guarantees, trust signals, or local presence."
            ).strip()
        else:
            brand_context = (
                f"Brand name: {brand_label}. "
                "For factual brand claims, use only the page-by-page brand knowledge pack supplied in this prompt. "
                "Other extracted evidence objects are routing and audit diagnostics, not writer truth."
            ).strip()
        
        # Select legacy evidence objects for audit/validation only. The writer-facing
        # factual truth is the page-by-page brand knowledge pack rendered separately.
        section_source_text = ""
        section_brand_page_briefs: List[Dict[str, Any]] = []
        section_page_narrative_briefs: List[Dict[str, Any]] = []
        section_raw_brand_blocks: List[Dict[str, Any]] = []
        section_brand_understanding: Dict[str, Any] = {}
        brand_page_knowledge_pack_context = state.get("brand_page_knowledge_pack_context", "")
        if not brand_page_knowledge_pack_context and state.get("brand_page_narrative_briefs"):
            self._persist_brand_page_knowledge_pack(state)
            brand_page_knowledge_pack_context = state.get("brand_page_knowledge_pack_context", "")

        try:
            from src.services.brand_evidence_service import (
                build_section_brand_understanding,
                select_section_brand_page_briefs,
                select_section_page_narrative_briefs,
                select_section_raw_brand_blocks,
            )
            section_page_narrative_briefs = select_section_page_narrative_briefs(section, state)
            section_brand_page_briefs = select_section_brand_page_briefs(section, state)
            section_raw_brand_blocks = select_section_raw_brand_blocks(section, state)
            section_for_understanding = dict(section)
            section_for_understanding["section_page_narrative_briefs"] = section_page_narrative_briefs
            section_for_understanding["section_brand_page_briefs"] = section_brand_page_briefs
            section_brand_understanding = build_section_brand_understanding(
                section_for_understanding,
                state,
                section_raw_brand_blocks,
            )
            section["section_page_narrative_briefs"] = section_page_narrative_briefs
            section["section_brand_page_briefs"] = section_brand_page_briefs
            section["section_raw_brand_blocks"] = section_raw_brand_blocks
            section["section_brand_understanding"] = section_brand_understanding

            formatted_raw_blocks = self._format_section_raw_brand_blocks_for_prompt(section_raw_brand_blocks)
            if formatted_raw_blocks:
                section_source_text = formatted_raw_blocks

            status = (
                section_brand_understanding
                .get("recommended_angle", {})
                .get("preferred_section_style", "general_guidance")
                if isinstance(section_brand_understanding, dict)
                else "unavailable"
            )
            logger.info("[brand_page_briefs] selected_count=%s", len(section_brand_page_briefs))
            logger.info("[brand_page_narrative_briefs] selected_count=%s", len(section_page_narrative_briefs))
            logger.info("[brand_raw_blocks] selected_count=%s", len(section_raw_brand_blocks))
            logger.info("[brand_section_understanding] status=%s", status)
        except Exception as e:
            logger.warning(f"Failed to select raw brand blocks and compile section understanding: {e}")

        # Writer-truth firewall:
        # Keep selected briefs/raw blocks/understanding for audit and validation,
        # but do not expose those extracted structures to the writer. The prompt
        # should see only the page-by-page knowledge pack as brand factual truth.
        writer_section_source_text = ""
        writer_section_brand_page_briefs: List[Dict[str, Any]] = []
        writer_section_page_narrative_briefs: List[Dict[str, Any]] = []
        writer_section_raw_brand_blocks: List[Dict[str, Any]] = []
        writer_section_brand_understanding: Dict[str, Any] = {}

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
        if str(state.get("content_type") or content_type or "").lower() == "brand_commercial":
            self._apply_commercial_section_role(section, state, section_index, total_sections)
        else:
            section["brand_usage_policy"] = self._brand_usage_policy_for_section(section, state)
        brand_usage_policy = str(section.get("brand_usage_policy") or "").lower()
        writer_brand_page_knowledge_pack_context = (
            brand_page_knowledge_pack_context
            if brand_usage_policy in {"brand_owned", "brand_light", "brand_cta", "soft_intro_brand"}
            else ""
        )
        if brand_page_knowledge_pack_context and not writer_brand_page_knowledge_pack_context:
            logger.info(
                "[brand_knowledge_firewall] Hidden knowledge pack for neutral section '%s'. policy=%s",
                section.get("heading_text", ""),
                brand_usage_policy,
            )

        # Step 3A-1: record that the single source of truth is available to the
        # writer alongside the legacy knowledge pack.
        gt_writer_record = self._record_ground_truth_consumption(state, "writer")

        # Step 3B (writer-only, parallel read): expose the consolidated Brand Ground
        # Truth to the writer ALONGSIDE the legacy page-by-page pack, for brand-
        # eligible sections only (same firewall). The knowledge pack is NOT removed -
        # this is an additive read so we can measure ground truth's value on writer
        # output before wiring strategy/outline/validator. No other consumer and no
        # saved file is affected (only this writer-local context copy changes).
        ground_truth_injected = False
        if writer_brand_page_knowledge_pack_context:
            gt_block = self._format_ground_truth_for_writer(state)
            if gt_block:
                writer_brand_page_knowledge_pack_context = (
                    writer_brand_page_knowledge_pack_context + "\n\n" + gt_block
                )
                ground_truth_injected = True

        writer_truth_trace = {
            "section_id": section.get("section_id"),
            "heading": section.get("heading_text", ""),
            "section_job": (section.get("section_intent_snapshot") or {}).get("section_job") or section.get("commercial_section_role", ""),
            "brand_usage_policy": brand_usage_policy,
            "knowledge_pack_visible": bool(writer_brand_page_knowledge_pack_context),
            "knowledge_pack_chars": len(writer_brand_page_knowledge_pack_context or ""),
            "ground_truth_available": bool(gt_writer_record.get("used")),
            "ground_truth_chars": gt_writer_record.get("markdown_chars", 0),
            "ground_truth_injected_into_writer": ground_truth_injected,
            "legacy_section_source_visible": bool(writer_section_source_text),
            "legacy_page_briefs_visible": bool(writer_section_brand_page_briefs or writer_section_page_narrative_briefs),
            "legacy_raw_blocks_visible": bool(writer_section_raw_brand_blocks),
            "legacy_understanding_visible": bool(writer_section_brand_understanding),
        }
        section["writer_truth_trace"] = writer_truth_trace
        state.setdefault("section_truth_trace", []).append(writer_truth_trace)
        logger.info(
            "[writer_truth_trace] section_id=%s policy=%s pack_visible=%s pack_chars=%s legacy_visible=%s",
            writer_truth_trace["section_id"],
            writer_truth_trace["brand_usage_policy"],
            writer_truth_trace["knowledge_pack_visible"],
            writer_truth_trace["knowledge_pack_chars"],
            any(
                [
                    writer_truth_trace["legacy_section_source_visible"],
                    writer_truth_trace["legacy_page_briefs_visible"],
                    writer_truth_trace["legacy_raw_blocks_visible"],
                    writer_truth_trace["legacy_understanding_visible"],
                ]
            ),
        )

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
            section_source_text=writer_section_source_text,
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
            writing_blueprint=writing_blueprint,
            market_angle=market_angle,
            used_anchors=state.get("used_anchors", []),
            section_brand_page_briefs=writer_section_brand_page_briefs,
                        section_page_narrative_briefs=writer_section_page_narrative_briefs,
            brand_page_knowledge_pack_context=writer_brand_page_knowledge_pack_context,
            section_raw_brand_blocks=writer_section_raw_brand_blocks,
            section_brand_understanding=writer_section_brand_understanding
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
            section_source_text=writer_section_source_text,
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
            used_anchors=state.get("used_anchors", []),
            section_brand_page_briefs=writer_section_brand_page_briefs,
                        section_page_narrative_briefs=writer_section_page_narrative_briefs,
            brand_page_knowledge_pack_context=writer_brand_page_knowledge_pack_context,
            section_raw_brand_blocks=writer_section_raw_brand_blocks,
            section_brand_understanding=writer_section_brand_understanding
        )

        raw_content = (
            res_data.get("content")
            or res_data.get("generated_content")
            or res_data.get("section_content")
            or ""
        )
        content = self._enforce_section_heading_lock(raw_content, section)
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
            final_content = self._apply_commercial_section_quality_gates(final_content, section, state)
            fulfillment_report = self._evaluate_brand_owned_section_fulfillment(section, final_content, state)
            policy_report = self._evaluate_brand_usage_policy_fulfillment(section, final_content, state)
            fulfillment_report = self._stricter_fulfillment_report(fulfillment_report, policy_report)
            role_report = self._evaluate_section_role_fulfillment(section, final_content, state)
            fulfillment_report = self._stricter_fulfillment_report(fulfillment_report, role_report)
            section["fulfillment_status"] = fulfillment_report.get("fulfillment_status", "satisfied")
            section["fulfillment_reason"] = fulfillment_report.get("fulfillment_reason", "")
            section["evidence_density"] = fulfillment_report.get("evidence_density", {})
            section["heading_fidelity"] = fulfillment_report.get("heading_fidelity", {})
            repairable_weak_fulfillment = (
                section.get("fulfillment_status") == "weak"
                and any(
                    marker in section.get("fulfillment_reason", "").lower()
                    for marker in ("evidence density", "heading drift", "brand usage policy", "role drift", "faq repair leak", "faq section", "comparison section")
                )
            )
            if section["fulfillment_status"] == "unsupported" or repairable_weak_fulfillment:
                logger.warning(
                    "[brand_fulfillment] %s section='%s' reason='%s'. Applying soft-first correction.",
                    section.get("fulfillment_status"),
                    section.get("heading_text", ""),
                    section.get("fulfillment_reason", ""),
                )
                original_heading = section.get("heading_text", "")
                downgraded_heading = self._fulfill_and_downgrade_heading(section, state)
                if downgraded_heading and downgraded_heading != original_heading:
                    section["heading_text"] = downgraded_heading
                    self._sync_heading_role_contract(
                        section,
                        state,
                        original_heading,
                        outline=state.get("outline", []),
                        index=section_index,
                        existing_content=final_content,
                    )
                    execution_plan = self._build_execution_plan(section, state)
                    execution_plan["brand_link_allowed"] = can_use_brand_link
                    execution_plan["brand_url"] = brand_url
                    fulfillment_report = self._evaluate_brand_owned_section_fulfillment(section, final_content, state)
                    policy_report = self._evaluate_brand_usage_policy_fulfillment(section, final_content, state)
                    fulfillment_report = self._stricter_fulfillment_report(fulfillment_report, policy_report)
                    role_report = self._evaluate_section_role_fulfillment(section, final_content, state)
                    fulfillment_report = self._stricter_fulfillment_report(fulfillment_report, role_report)
                    section["fulfillment_status"] = fulfillment_report.get("fulfillment_status", "satisfied")
                    section["fulfillment_reason"] = fulfillment_report.get("fulfillment_reason", "")
                    section["evidence_density"] = fulfillment_report.get("evidence_density", {})
                    section["heading_fidelity"] = fulfillment_report.get("heading_fidelity", {})

                repairable_weak_fulfillment = (
                    section.get("fulfillment_status") == "weak"
                    and any(
                        marker in section.get("fulfillment_reason", "").lower()
                        for marker in ("evidence density", "heading drift", "brand usage policy", "role drift", "faq repair leak", "faq section", "comparison section")
                    )
                )
                if (
                    section.get("fulfillment_status") == "unsupported"
                    or repairable_weak_fulfillment
                    or bool((section.get("heading_contract_sync") or {}).get("body_rewrite_required"))
                ) and not section.get("_brand_fulfillment_repair_attempted"):
                    section["_brand_fulfillment_repair_attempted"] = True
                    repair_plan = dict(execution_plan or {})
                    repair_feedback = [
                        "BRAND FULFILLMENT CORRECTION:",
                        f"- Reason: {section.get('fulfillment_reason', 'unsupported brand-owned section')}",
                        "- Rewrite only the offending unsupported parts while preserving the section structure.",
                        "- Use only the [BRAND PAGE KNOWLEDGE PACK - PAGE BY PAGE] as the evidence boundary.",
                        "- If the knowledge pack contains project/client names relevant to this heading, mention at least one exactly.",
                        "- Keep each brand-owned paragraph anchored to observed services, projects, technologies, workflow steps, or source-backed boundaries.",
                        "- Do not turn service or feature headings into generic buyer-advice criteria.",
                        "- If no raw evidence supports pricing, geography, trust, certification, timelines, guarantees, or market leadership, remove that claim.",
                        "- Obey the section brand usage policy: neutral sections must not mention the brand; brand-light sections may mention it once and must not use project examples.",
                    ]
                    repair_plan["structure_rule"] = "\n".join(
                        [
                            str(repair_plan.get("structure_rule") or execution_plan.get("structure_rule") or "").strip(),
                            *repair_feedback,
                        ]
                    ).strip()

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
                        execution_plan=repair_plan,
                        draft_to_fix=final_content,
                        area=area_for_section,
                        used_phrases=used_phrases,
                        used_internal_links=state.get("used_internal_links", []),
                        used_external_links=state.get("used_external_links", []),
                        section_index=section_index,
                        total_sections=total_sections,
                        brand_context=brand_context,
                        section_source_text=writer_section_source_text,
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
                        used_anchors=state.get("used_anchors", []),
                        section_brand_page_briefs=writer_section_brand_page_briefs,
                        section_page_narrative_briefs=writer_section_page_narrative_briefs,
                        brand_page_knowledge_pack_context=writer_brand_page_knowledge_pack_context,
                        section_raw_brand_blocks=writer_section_raw_brand_blocks,
                        section_brand_understanding=writer_section_brand_understanding,
                    )

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
                        execution_plan=repair_plan,
                        draft_to_fix=final_content,
                        area=area_for_section,
                        used_phrases=used_phrases,
                        used_internal_links=state.get("used_internal_links", []),
                        used_external_links=state.get("used_external_links", []),
                        section_index=section_index,
                        total_sections=total_sections,
                        brand_context=brand_context,
                        section_source_text=writer_section_source_text,
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
                        used_anchors=state.get("used_anchors", []),
                        section_brand_page_briefs=writer_section_brand_page_briefs,
                        section_page_narrative_briefs=writer_section_page_narrative_briefs,
                        brand_page_knowledge_pack_context=writer_brand_page_knowledge_pack_context,
                        section_raw_brand_blocks=writer_section_raw_brand_blocks,
                        section_brand_understanding=writer_section_brand_understanding,
                    )

                    candidate_content = (
                        repair_data.get("content")
                        or repair_data.get("generated_content")
                        or repair_data.get("section_content")
                        or ""
                    )
                    if candidate_content:
                        candidate_content = self._enforce_section_heading_lock(candidate_content, section)
                        candidate_content = LinkManager.sanitize_section_links(
                            content=candidate_content,
                            state=state,
                            brand_url=brand_url or "",
                            max_external=2,
                        )
                        candidate_final = self.validator.enforce_paragraph_structure(candidate_content)
                        candidate_final = self._apply_commercial_section_quality_gates(candidate_final, section, state)
                        candidate_report = self._evaluate_brand_owned_section_fulfillment(section, candidate_final, state)
                        candidate_policy_report = self._evaluate_brand_usage_policy_fulfillment(section, candidate_final, state)
                        candidate_report = self._stricter_fulfillment_report(candidate_report, candidate_policy_report)
                        candidate_role_report = self._evaluate_section_role_fulfillment(section, candidate_final, state)
                        candidate_report = self._stricter_fulfillment_report(candidate_report, candidate_role_report)
                        if candidate_report.get("fulfillment_status") != "unsupported":
                            final_content = candidate_final
                            content = candidate_final
                            found_links = re.findall(r'\[.*?\]\((https?://.*?)\)', final_content)
                            section["fulfillment_status"] = candidate_report.get("fulfillment_status", "satisfied")
                            section["fulfillment_reason"] = candidate_report.get("fulfillment_reason", "")
                            section["evidence_density"] = candidate_report.get("evidence_density", {})
                            section["heading_fidelity"] = candidate_report.get("heading_fidelity", {})
                            if isinstance(section.get("heading_contract_sync"), dict):
                                section["heading_contract_sync"]["body_rewrite_required"] = False
                            section["section_quality_issues"] = [
                                issue
                                for issue in section.get("section_quality_issues", [])
                                if issue != "heading_contract_body_rewrite_required"
                            ]
                            logger.info(
                                "[brand_fulfillment] corrective rewrite accepted section='%s' status='%s'.",
                                section.get("heading_text", ""),
                                section.get("fulfillment_status"),
                            )
                        else:
                            logger.warning(
                                "[brand_fulfillment] corrective rewrite still unsupported section='%s' reason='%s'. Keeping safer previous version.",
                                section.get("heading_text", ""),
                                candidate_report.get("fulfillment_reason", ""),
                            )

            brand_evidence_audit = self._build_brand_section_evidence_audit(
                section=section,
                section_brand_page_briefs=section_brand_page_briefs,
                section_page_narrative_briefs=section_page_narrative_briefs,
                section_raw_brand_blocks=section_raw_brand_blocks,
                section_brand_understanding=section_brand_understanding,
            )
            section["brand_evidence_audit"] = brand_evidence_audit
            logger.info(
                "[brand_section_audit] section_id=%s heading=%s narrative_briefs=%s briefs=%s blocks=%s urls=%s projects=%s services=%s fulfillment=%s",
                brand_evidence_audit.get("section_id"),
                brand_evidence_audit.get("heading"),
                brand_evidence_audit.get("selected_narrative_briefs_count"),
                brand_evidence_audit.get("selected_briefs_count"),
                brand_evidence_audit.get("selected_blocks_count"),
                brand_evidence_audit.get("selected_urls"),
                brand_evidence_audit.get("relevant_projects"),
                brand_evidence_audit.get("relevant_services"),
                brand_evidence_audit.get("fulfillment_status"),
            )
            if state.get("workflow_logger"):
                state["workflow_logger"].log_step_details(
                    step_name=f"BRAND_SECTION_EVIDENCE_AUDIT: {section_id}",
                    duration=0,
                    output_data=brand_evidence_audit,
                )

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
                        section_source_text=writer_section_source_text,
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
                        used_anchors=state.get("used_anchors", []),
                        section_brand_page_briefs=writer_section_brand_page_briefs,
                        section_page_narrative_briefs=writer_section_page_narrative_briefs,
                        brand_page_knowledge_pack_context=writer_brand_page_knowledge_pack_context,
                        section_raw_brand_blocks=writer_section_raw_brand_blocks,
                        section_brand_understanding=writer_section_brand_understanding
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
                        section_source_text=writer_section_source_text,
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
                        used_anchors=state.get("used_anchors", []),
                        section_brand_page_briefs=writer_section_brand_page_briefs,
                        section_page_narrative_briefs=writer_section_page_narrative_briefs,
                        brand_page_knowledge_pack_context=writer_brand_page_knowledge_pack_context,
                        section_raw_brand_blocks=writer_section_raw_brand_blocks,
                        section_brand_understanding=writer_section_brand_understanding
                    )

                    new_content = repair_data.get("content", "")
                    if new_content:
                        logger.info(f"Section '{section.get('heading_text')}' repaired successfully.")
                        new_content = self._enforce_section_heading_lock(new_content, section)
                        final_content = self.validator.enforce_paragraph_structure(new_content)
                        final_content = self._apply_commercial_section_quality_gates(final_content, section, state)
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
            claim_gate_brief = None
            if state.get("brand_name") or writer_brand_page_knowledge_pack_context:
                from src.services.brand_evidence_service import apply_brand_claim_gate
                brand_usage_policy = str(section.get("brand_usage_policy") or self._brand_usage_policy_for_section(section, state)).lower()
                claim_gate_brief = {
                    "brand_name": state.get("brand_name", ""),
                    "section_source_text": writer_brand_page_knowledge_pack_context or "",
                    "brand_usage_policy": brand_usage_policy,
                    "brand_sensitive": brand_usage_policy in {"brand_owned", "brand_cta", "soft_intro_brand"},
                    "observed_project_names": self._brand_project_names_for_policy(state),
                }
                if state.get("brand_aliases"):
                    claim_gate_brief["brand_aliases"] = state.get("brand_aliases")
                final_content = apply_brand_claim_gate(final_content, claim_gate_brief)

            brand_name = state.get("brand_name", "")
            mentions_in_section = 0
            if brand_name and final_content:
                # Use word boundaries or just count occurrences
                pattern = r'\b{}\b'.format(re.escape(brand_name.lower()))
                mentions_in_section = len(re.findall(pattern, final_content.lower()))

                # In Arabic, word boundaries might be tricky with prefixes. Let's do a direct count as fallback if word boundaries fail, but regex with \b works decently.
                if mentions_in_section == 0 and brand_name.lower() in final_content.lower():
                     mentions_in_section = final_content.lower().count(brand_name.lower())

            ret_dict = {
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
            for alias_key in ["content", "section_content"]:
                if alias_key in res_data and isinstance(res_data.get(alias_key), str):
                    ret_dict[alias_key] = apply_brand_claim_gate(
                        res_data[alias_key],
                        claim_gate_brief
                    ) if claim_gate_brief else res_data[alias_key]
            if claim_gate_brief:
                from src.services.brand_evidence_service import apply_brand_claim_gate
                for key in ["generated_content", "content", "section_content"]:
                    if key in ret_dict:
                        ret_dict[key] = apply_brand_claim_gate(ret_dict[key], claim_gate_brief)
                final_content = ret_dict["generated_content"]
            return ret_dict
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

        self._finalize_intro_sections_for_output(state, final_sections)

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

        self._finalize_intro_sections_for_output(state, ordered_sections)

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

        meta_claim_repairs: List[str] = []
        for field, context in (
            ("h1", "h1"),
            ("meta_title", "meta_title"),
            ("meta_description", "meta_description"),
        ):
            original = str(meta_json.get(field) or "")
            if not original:
                continue
            cleaned, issues = self._sanitize_unsupported_brand_claims(
                original,
                state,
                context=context,
                brand_sensitive=self._brand_name_in_text(original, state),
            )
            meta_json[field] = cleaned
            meta_claim_repairs.extend(f"{field}:{issue}" for issue in issues)

        article_schema = meta_json.get("article_schema")
        if isinstance(article_schema, dict):
            for field, context in (("headline", "h1"), ("description", "meta_description")):
                original = str(article_schema.get(field) or "")
                if not original:
                    continue
                cleaned, issues = self._sanitize_unsupported_brand_claims(
                    original,
                    state,
                    context=context,
                    brand_sensitive=self._brand_name_in_text(original, state),
                )
                article_schema[field] = cleaned
                meta_claim_repairs.extend(f"article_schema.{field}:{issue}" for issue in issues)

        if meta_claim_repairs:
            existing_repairs = state.setdefault("unsupported_brand_claim_repairs", [])
            existing_repairs.extend(item for item in meta_claim_repairs if item not in existing_repairs)
            logger.warning("[unsupported_brand_claim_guard] meta_repairs=%s", meta_claim_repairs)

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
            if content_type == "brand_commercial":
                plan["structure_rule"] = (
                    "EXACTLY 2-3 PARAGRAPHS. Use simple buyer language. "
                    "Mention at most 1-2 broad observed brand capabilities; do not dump technical catalogs in the introduction."
                )

        elif section_type == "conclusion":
            plan["writing_mode"] = "summary-driven"
            if content_type == "brand_commercial":
                plan["cta_eligible"] = True
                plan["cta_type"] = "strong"
                plan["structure_rule"] = (
                    "EXACTLY 2 PARAGRAPHS. Make this a strong sales close grounded in observed brand capabilities. "
                    "End with a clear next step; do not reopen generic market education."
                )
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
        is_commercial = str(state.get("content_type") or "").lower() == "brand_commercial"
        quality_warnings: List[str] = []
        rendered_section_contents: Dict[str, str] = {}

        safe_title, title_claim_issues = self._sanitize_unsupported_brand_claims(
            title,
            state,
            context="title",
            brand_sensitive=self._brand_name_in_text(title, state),
        )
        parts = [f"# {safe_title}"]
        for issue in title_claim_issues:
            quality_warnings.append(f"title: unsupported_brand_claim_removed:{issue}")
        for section_index, outline_section in enumerate(outline):
            section_id = outline_section.get("section_id")
            generated_section = sections_dict.get(section_id, {}) if section_id else {}
            section = {**outline_section, **generated_section}

            content = self._enforce_section_heading_lock(
                str(section.get("generated_content", "") or "").strip(),
                outline_section,
            )
            if is_commercial and self._is_commercial_intro_section(section, state):
                content, intro_report = self._finalize_commercial_intro_surgically(
                    content,
                    section,
                    state,
                )
                self._record_final_intro_report(state, section, intro_report)
                if isinstance(generated_section, dict):
                    generated_section["generated_content"] = content
                    generated_section["final_intro_quality_report"] = dict(intro_report)
                    generated_section["section_quality_issues"] = list(
                        section.get("section_quality_issues", [])
                    )
            if is_commercial:
                content = self._apply_commercial_section_quality_gates(
                    content,
                    section,
                    state,
                    skip_intro_contract=self._is_commercial_intro_section(section, state),
                )
                if isinstance(generated_section, dict):
                    generated_section["generated_content"] = content
                    generated_section["section_quality_issues"] = list(section.get("section_quality_issues", []))
            else:
                content = self._normalize_ordered_lists(content)

            section_type = (outline_section.get("section_type") or "").lower()
            heading = str(outline_section.get("heading_text") or "").strip()
            if is_commercial and heading:
                original_heading = heading
                heading, heading_claim_issues = self._sanitize_unsupported_brand_claims(
                    heading,
                    state,
                    section=section,
                    context="heading",
                    brand_sensitive=self._section_visibly_references_brand(section, state),
                )
                for issue in heading_claim_issues:
                    self._record_section_quality_issue(section, f"unsupported_brand_claim_removed:{issue}")
                outline_section["heading_text"] = heading
                if heading != original_heading:
                    self._sync_heading_role_contract(
                        outline_section,
                        state,
                        original_heading,
                        outline=outline,
                        index=section_index,
                        existing_content=content,
                    )
                    if isinstance(generated_section, dict):
                        for key in (
                            "heading_text",
                            "section_type",
                            "taxonomy_axis",
                            "coverage_role",
                            "commercial_section_role",
                            "brand_usage_policy",
                            "section_intent_snapshot",
                            "section_contract",
                            "heading_contract_sync",
                            "section_quality_issues",
                        ):
                            if key in outline_section:
                                generated_section[key] = outline_section[key]
                    section = {**outline_section, **generated_section}
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
                if section_id:
                    rendered_section_contents[section_id] = content

            for issue in (generated_section.get("section_quality_issues") or section.get("section_quality_issues") or []):
                warning = f"{section_id or heading}: {issue}"
                if warning not in quality_warnings:
                    quality_warnings.append(warning)
            fulfillment_status = str(generated_section.get("fulfillment_status") or section.get("fulfillment_status") or "").lower()
            fulfillment_reason = str(generated_section.get("fulfillment_reason") or section.get("fulfillment_reason") or "").strip()
            if fulfillment_status == "unsupported" and fulfillment_reason:
                warning = f"{section_id or heading}: unsupported fulfillment - {fulfillment_reason}"
                if warning not in quality_warnings:
                    quality_warnings.append(warning)

        final_markdown = "\n\n".join(part for part in parts if part).strip()
        malformed_tables = [
            block for _, _, block in self._extract_markdown_tables(final_markdown)
            if not self._is_valid_markdown_table(block)
        ]
        if malformed_tables:
            quality_warnings.append("Malformed markdown table detected after section assembly.")
        before_limit_count = self._count_valid_markdown_tables(final_markdown)
        final_markdown = self._limit_markdown_tables(final_markdown, max_tables=2)
        after_limit_count = self._count_valid_markdown_tables(final_markdown)
        useful_table_count = self._count_useful_markdown_tables(final_markdown)
        if before_limit_count > 2:
            quality_warnings.append(f"Reduced table count from {before_limit_count} to {after_limit_count}.")
        if is_commercial and state.get("include_tables", True) and useful_table_count < 1:
            quality_warnings.append("Commercial article has no decision-useful markdown table after repair.")

        primary_keyword = str(state.get("primary_keyword") or state.get("raw_title") or "").strip()
        intro_section = next(
            (
                sec for sec in outline
                if str(sec.get("section_type") or "").lower() in {"introduction", "intro"}
                or str(sec.get("heading_level") or "").upper() == "INTRO"
            ),
            None,
        )
        intro_text = (
            rendered_section_contents.get(intro_section.get("section_id"))
            if intro_section and intro_section.get("section_id")
            else state.get("introduction_text")
        ) or ""
        if is_commercial and primary_keyword and intro_text and not self._text_contains_keyword(intro_text, primary_keyword):
            quality_warnings.append("Commercial introduction does not contain the primary keyword.")

        brand_url = str(state.get("brand_url") or "").strip()
        if is_commercial and brand_url:
            last_section = next((sec for sec in reversed(outline) if str(sec.get("section_type") or "").lower() == "conclusion" or str(sec.get("commercial_section_role") or "").lower() == "cta"), None)
            if last_section:
                last_id = last_section.get("section_id")
                last_content = str(rendered_section_contents.get(last_id) or "")
                if brand_url not in last_content:
                    quality_warnings.append("Commercial conclusion does not contain a brand URL CTA.")

        if self._content_has_repair_placeholder_leak(final_markdown):
            quality_warnings.append("repair_placeholder_leak in final article content")

        critical_semantic_markers = (
            "project_proof_missed_target_relevant_evidence",
            "unsupported_testimonial_heading",
            "unsupported_brand_claim_removed",
            "role drift",
            "faq_repair_leak",
            "repair_placeholder_leak",
            "table_placeholder_blocked",
            "table_incomplete_or_placeholder",
            "heading_contract_body_rewrite_required",
            "intro_final_enforcement_failed",
            "Commercial introduction does not contain the primary keyword",
            "unsupported fulfillment",
            "Commercial article has no decision-useful markdown table",
            "Malformed markdown table",
            "empty_numbered_list_item",
            "process_section_insufficient_steps",
            "conclusion_missing_brand_url_cta",
            "section_body_empty",
        )
        needs_revision = any(
            any(marker in item for marker in critical_semantic_markers)
            for item in quality_warnings
        )
        state["content_stage_quality_report"] = {
            "status": "needs_revision" if needs_revision else "pass",
            "warnings": quality_warnings,
            "valid_table_count": after_limit_count,
            "useful_table_count": useful_table_count,
            "role_collisions": state.get("commercial_role_collision_report", []),
            "section_truth_trace": state.get("section_truth_trace", []),
        }
        state["content_stage_status"] = "needs_revision" if state["content_stage_quality_report"]["status"] == "needs_revision" else "success"
        if state["content_stage_status"] == "needs_revision":
            state["final_status"] = "needs_revision"
        if quality_warnings:
            logger.warning("[content_stage_quality_gate] warnings=%s", quality_warnings)
        elif state.get("commercial_role_collision_report"):
            logger.info("[content_stage_quality_gate] role_collisions=%s", state.get("commercial_role_collision_report"))

        output_dir = state.get("output_dir", self.work_dir)
        quality_report_path = os.path.join(output_dir, "quality_warnings.txt")
        try:
            with open(quality_report_path, "w", encoding="utf-8") as f:
                f.write(f"STATUS: {state['content_stage_status']}\n")
                f.write(f"WARNING_COUNT: {len(quality_warnings)}\n")
                f.write("-" * 60 + "\n")
                for warning in quality_warnings:
                    f.write(f"- {warning}\n")
        except OSError as exc:
            logger.error("Failed to write quality_warnings.txt: %s", exc)
        workflow_logger = state.get("workflow_logger")
        if workflow_logger:
            workflow_logger.log_event("content_stage_quality_gate", state["content_stage_quality_report"])
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

        raw_title, title_claim_issues = self._sanitize_unsupported_brand_claims(
            raw_title,
            state,
            context="title",
            brand_sensitive=self._brand_name_in_text(raw_title, state),
        )
        meta_title, meta_title_claim_issues = self._sanitize_unsupported_brand_claims(
            meta_title,
            state,
            context="meta_title",
            brand_sensitive=self._brand_name_in_text(meta_title, state),
        )
        meta_description, meta_description_claim_issues = self._sanitize_unsupported_brand_claims(
            seo_meta.get("meta_description", ""),
            state,
            context="meta_description",
            brand_sensitive=self._brand_name_in_text(seo_meta.get("meta_description", ""), state),
        )
        final_claim_repairs = [
            *(f"title:{issue}" for issue in title_claim_issues),
            *(f"meta_title:{issue}" for issue in meta_title_claim_issues),
            *(f"meta_description:{issue}" for issue in meta_description_claim_issues),
        ]
        if final_claim_repairs:
            existing_repairs = state.setdefault("unsupported_brand_claim_repairs", [])
            existing_repairs.extend(item for item in final_claim_repairs if item not in existing_repairs)

        if state.get("heading_only_mode"):
            outline = state.get("outline", [])
            if state.get("brand_generation_guardrails", {}).get("brand_section_policy") != "do_not_create_dedicated_brand_proof_or_why_choose_sections":
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

            content_stage_status = state.get("content_stage_status", "success")
            return {
                "title": raw_title,
                "slug": state.get("slug", "unknown"),
                "primary_keyword": state.get("primary_keyword", ""),
                "content_stage_only_mode": True,
                "content_only_mode": state.get("content_only_mode", False),
                "heading_only_mode": False,
                "final_markdown": final_markdown,
                "outline_structure": outline_map,
                "status": content_stage_status,
                "message": "Content draft generated for review." if content_stage_status == "success" else "Content draft generated with quality warnings; review required.",
                "content_stage_quality_report": state.get("content_stage_quality_report", {}),
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
            "meta_description": meta_description,
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
            from jinja2 import Environment
            env = Environment(loader=create_prompt_template_loader())
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
                    "Keep the article buyer-aware, but treat the provided brand as the primary provider "
                    "being evaluated. Service, proof, differentiation, process, and conclusion sections "
                    "should use observed brand evidence to explain what the brand actually provides. "
                    "Do not convert brand service sections into generic provider-selection advice."
                )

            if sanitized_brand_advantages:
                sanitized_brand_advantages = [
                    str(item).strip()
                    for item in sanitized_brand_advantages
                    if str(item).strip()
                ][:3]

            if sanitized_writing_blueprint:
                sanitized_writing_blueprint = (
                    "Keep headings buyer-focused while making brand-owned sections answerable from observed "
                    "brand evidence. Prefer service/catalog, project/example, and process headings that the "
                    "writer can fulfill with specific brand facts."
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
            sanitized_strategy["supported_eeat_signals"] = [
                s for s in sanitized_strategy.get("supported_eeat_signals", [])
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

        if not has_legal and "process" in roles:
            roles["process"] = "Explain the practical customer journey simply, omitting legal or technical compliance checklists."

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
