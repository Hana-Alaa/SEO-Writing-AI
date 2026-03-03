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
from langdetect import detect  
from jinja2 import Template, StrictUndefined
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
            start_time = time.time()
            
            try:
                # Execute the async coordination step
                new_state = await func(state)
                
                if new_state is None:
                    new_state = state
                
                duration = time.time() - start_time
                if self.observer:
                    self.observer.log_workflow_step(step_name, duration)
                logger.info(f"--- Finished Step: {step_name} (Duration: {duration:.2f}s) ---")
                return {"status": "success", "step": step_name, "duration": duration, "data": new_state}
            
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"Error in step '{step_name}' attempt {attempt + 1}: {e}")
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
        
        with open("prompts/templates/00_content_strategy.txt", "r", encoding="utf-8") as f:
            self.content_strategy = Template(f.read(), undefined=StrictUndefined)

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
            # ("analysis", self._step_0_analysis, 0),
            # ("web_research", self._step_web_research, 1),  
            # ("semantic_layer", self._step_semantic_layer, 1),
            ("analysis_init", self._step_0_init, 0),
            ("web_research", self._step_0_web_research, 1),
            ("serp_analysis", self._step_0_serp_analysis, 1),
            ("intent_title", self._step_0_intent_title, 0),
            ("style_analysis", self._step_0_style_analysis, 1),
            ("content_strategy", self._step_0_content_strategy, 3),
            ("outline_generation", self._step_1_outline, 1),

            ("content_writing", self._step_2_write_sections, 1),
            # ("image_prompting", self._step_4_generate_image_prompts, 0),
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

        return self._assemble_final_output(state)

    # ---------------- COORDINATION STEPS (ASYNC) ----------------
    async def _detect_intent_ai(self, raw_title: str, primary_keyword: str) -> str:

        prompt = self.intent_template.render(
            raw_title=raw_title,
            primary_keyword=primary_keyword
        )

        response = await self.ai_client.send(prompt, step="intent")
        return response.strip()

    async def _step_0_init(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Setup unique directories and sluggification."""

        input_data = state.get("input_data", {})
        raw_title = input_data.get("title", "Untitled Article")
        keywords = input_data.get("keywords", [])
        primary_keyword = keywords[0] if keywords else raw_title
        user_lang = input_data.get("article_language")
        # article_language = user_lang if user_lang else (detect(raw_title) if raw_title else "en")
        # article_language = detect(raw_title) if raw_title else "en"
        article_language = self._resolve_article_language(raw_title, user_lang)
        area = input_data.get("area")
        state["area"] = area
        state["article_language"] = article_language
        state["primary_keyword"] = primary_keyword
        state["raw_title"] = raw_title
        state["keywords"] = keywords

        # keep input_data in sync for downstream steps
        state.setdefault("input_data", {})
        state["input_data"]["article_language"] = article_language

        # Generate slug and directory
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        slug_base = self._sluggify(primary_keyword)
        slug = f"{slug_base}_{timestamp}"
        state["slug"] = slug
        
        output_dir = os.path.join(self.work_dir, slug)
        os.makedirs(output_dir, exist_ok=True)
        state["output_dir"] = output_dir
        state["used_phrases"] = []

        return state

    async def _step_0_web_research(self, state):

        primary_keyword = state["primary_keyword"]
        area = state.get("area")
        search_query = f"{primary_keyword} in {area}" if state.get("area") else primary_keyword

        with open("prompts/templates/seo_web_research.txt") as f:
            template = Template(f.read())

        research_prompt = template.render(
            primary_keyword=search_query
        )

        raw = await self.ai_client.send_with_web(
            prompt=research_prompt,
            max_results=3
        )
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

    async def _step_0_intent_title(self, state: Dict[str, Any]) -> Dict[str, Any]:
        raw_title = state.get("raw_title") or state.get("input_data", {}).get("title", "Untitled Article")
        primary_keyword = state.get("primary_keyword") or (state.get("keywords", [raw_title])[0] if state.get("keywords") else raw_title)
        article_language = state.get("article_language") or state.get("input_data", {}).get("article_language", "en")

        serp_data = state.get("serp_data", {})
        area = state.get("area")

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

        title_data = await self.title_generator.generate(
            raw_title=raw_title,
            primary_keyword=primary_keyword,
            article_language=article_language,
            serp_titles=top_titles,
            serp_cta_styles=cta_styles,
            area=area
        )

        intent_raw = title_data.get("intent", "Informational")
        optimized_title = title_data.get("optimized_title", raw_title)

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


        raw = await self.ai_client.send(
            analysis_prompt,
            step="serp_analysis"
        )

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
            lsi = light_serp.get("lsi") or []
            related = light_serp.get("related") or []
            fallback_keywords = [primary_keyword] + lsi[:5] + related[:5]
            
            serp_insights["strategic_intelligence"]["keyword_clusters"] = [
                {
                    "cluster_name": "Semantic Cluster (Fallback)",
                    "keywords": list(dict.fromkeys(fallback_keywords)) # Remove duplicates
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
            fallback_keywords = [primary_keyword] + lsi[:5] + related[:5]
            clusters = [{
                "cluster_name": "Semantic Keywords Cluster (Safety Fallback)",
                "keywords": list(dict.fromkeys(fallback_keywords))
            }]

        prompt = self.content_strategy.render(
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
            raw = await self.ai_client.send(prompt, step="content_strategy")
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
        urls_norm = normalize_urls(urls_raw) or []
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
                area=area,
                feedback=feedback,
                mandatory_section_types = list(mandatory)
            )

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
            brand_url=urls_norm[0].get("link", "") if urls_norm else ""
        )

        state["allowed_external_domains"] = {
            "sama.gov.sa", "cst.gov.sa", "ndmo.gov.sa",
            "developers.google.com", "web.dev", "schema.org", "w3.org", "statista.com"
        }

        outline = DataInjector.distribute_urls_to_outline(outline, urls_norm, strategy="conservative")

        state["link_strategy"] = {
            "internal_topics": urls_norm,
            "authority_topics": [{"link": f"https://{d}", "type": "authority"} for d in state["allowed_external_domains"]],
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
        
        state["outline"] = outline
        present_types = {sec.get("section_type") for sec in outline}

        user_urls = state.get("input_data", {}).get("urls", [])

        internal_links = [
            u["link"] for u in user_urls if u.get("link")
        ]

        state["internal_url_set"] = set(internal_links)
        state["brand_url"] = internal_links[0] if internal_links else None

        missing = mandatory - present_types

        if missing:
            logger.error(f"[outline_validate] Missing mandatory sections: {missing}")
            # we could raise error or just log depending on strictness
            # raise ValueError(f"Missing mandatory sections: {missing}")

        return state

    # async def _step_2_write_sections(self, state: Dict[str, Any]) -> Dict[str, Any]:
    #     input_data = state.get("input_data", {})
    #     title = input_data.get("title", "Untitled")
    #     outline = state.get("outline", [])
    #     global_keywords = state.get("global_keywords", {})
    #     intent = state.get("intent", "Informational")
    #     # article_language = input_data.get("article_language", "en")
    #     # article_language = state.get("article_language", "en")
    #     article_language = state.get("article_language") or state.get("input_data", {}).get("article_language", "en")
    #     seo_intelligence = state.get("seo_intelligence", {})
    #     link_strategy = state.get("link_strategy", {})
        
    #     if not outline:
    #         raise RuntimeError("No outline found for section writing.")

    #     content_type = state.get("content_type", "informational")

    #     if PARALLEL_SECTIONS:
    #         tasks = [
    #             self._write_single_section(
    #                 title=title,
    #                 global_keywords=global_keywords,
    #                 section=section,
    #                 article_intent=intent,
    #                 seo_intelligence=seo_intelligence,
    #                 content_type=content_type,
    #                 link_strategy=link_strategy,
    #                 state=state,
    #                 section_index=idx,
    #                 total_sections=len(outline)
    #             )
    #             for idx, section in enumerate(outline)
    #         ]
    #         logger.info(f"Writing {len(tasks)} sections in PARALLEL mode")
    #         results = await asyncio.gather(*tasks, return_exceptions=True)
    #     else:
    #         logger.info(f"Writing {len(outline)} sections in SEQUENTIAL mode")
    #         results = []
    #         for idx, section in enumerate(outline):
    #             res = await self._write_single_section(
    #                 title=title,
    #                 global_keywords=global_keywords,
    #                 section=section,
    #                 article_intent=intent,
    #                 seo_intelligence=seo_intelligence,
    #                 content_type=content_type,
    #                 link_strategy=link_strategy,
    #                 state=state,
    #                 section_index=idx,
    #                 total_sections=len(outline)
    #             )
    #             results.append(res)
                
    #             # Update link state from the result
    #     if res and isinstance(res, dict) and res.get("brand_link_used"):
    #         state["brand_link_used"] = True

    #     sections_content = {}
    #     for res in results:
    #         if isinstance(res, Exception):
    #             logger.error(f"Section failed: {res}")
    #             continue
    #         if res:
    #             sections_content[res["section_id"]] = res

    #     state["sections"] = sections_content
        
    #     # Local SEO Enforcement (Retry first section if area is missing)
    #     area = state.get("area")
    #     if area and sections_content:
    #         first_id = outline[0]["section_id"]
    #         first_res = sections_content.get(first_id)
            
    #         if first_res and area.lower() not in first_res["generated_content"].lower():
    #             logger.info(f"Local area '{area}' missing in first section. Retrying with enforcement...")
                
    #             # Regenerate first section once
    #             retry_res = await self._write_single_section(
    #                 title,
    #                 global_keywords,
    #                 outline[0],
    #                 intent,
    #                 seo_intelligence,
    #                 content_type,
    #                 link_strategy,
    #                 state,
    #                 force_local=True
    #             )
                
    #             if retry_res:
    #                 sections_content[first_id] = retry_res
    #                 state["sections"] = sections_content
    #                 logger.info("First section regenerated successfully with Local SEO enforcement.")
    #             else:
    #                 logger.warning("Retry of first section failed.")

    #     logger.info(f"Successfully wrote {len(sections_content)} sections.")
    #     return state
    
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
        can_use_brand_link = bool(brand_url) and (not brand_link_used)

        execution_plan = self._build_execution_plan(section, state)
        if force_local:
            execution_plan["local_context_required"] = True
            
        # execution_plan["brand_link_allowed"] = (not brand_link_used)
        execution_plan["brand_link_allowed"] = bool(brand_url) and (not brand_link_used)
        execution_plan["brand_url"] = brand_url

        used_phrases = state.get("used_phrases", [])

        # Try 1
        res_data = await self.section_writer.write(
            title=title,
            global_keywords=global_keywords,
            section=section,
            article_intent=article_intent,
            seo_intelligence=seo_intelligence,
            content_type=content_type,
            link_strategy=link_strategy,
            # brand_url=brand_url,
            brand_link_used=brand_link_used,
            brand_link_allowed=can_use_brand_link,
            allow_external_links=True,
            execution_plan=execution_plan,
            area=state.get("area"),
            used_phrases=used_phrases,
            used_internal_links=state.get("used_internal_links", []),
            used_external_links=state.get("used_external_links", []), 
            section_index=section_index,
            total_sections=total_sections,
            brand_url=state.get("brand_url")
        )
        content = res_data.get("content", "")
        used_links = res_data.get("used_links", [])
        brand_link_used_in_sec = res_data.get("brand_link_used", False)

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
                    total_sections=total_sections
                )
                content = res_data.get("content", "")
                used_links = res_data.get("used_links", [])
                brand_link_used_in_sec = res_data.get("brand_link_used", False)

        # Multi-Layer Paragraph Structure and Strict SEO Validation
        if content:
            is_valid, validation_errors = self._validate_section_output(
                content, 
                section, 
                section_index, 
                total_sections, 
                state.get("area"),
                execution_plan.get("cta_type", "none")
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
                    total_sections=total_sections
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
                    total_sections=total_sections
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

            # sanitize links first
            content = self._sanitize_section_links(
                content=content,
                state=state,
                brand_url=brand_url or "",
                max_external=1
            )

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
    
    async def _step_4_5_download_images(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Downloads images (now parallel in the client)."""
        prompts = state.get("image_prompts", [])
        keywords = state.get("input_data", {}).get("keywords", [])
        # primary_keyword = (keywords[0] if keywords else "") or ""
        primary_keyword = state.get("primary_keyword")
        logo_path = state.get("input_data", {}).get("logo_path")
        reference_path = state.get("input_data", {}).get("logo_reference_path")
        brand_visual_style = state.get("brand_visual_style", "")

        images = await self.image_client.generate_images(
            prompts,
            primary_keyword=primary_keyword,
            logo_path=logo_path,
            reference_path=reference_path,
            brand_visual_style=brand_visual_style
        )

        for img in images:
            if "local_path" in img:
                img["local_path"] = f"../images/{os.path.basename(img['local_path'])}"

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
            assembled["final_markdown"] = self._prune_redundant_intros(assembled["final_markdown"])
            
        state["final_output"] = assembled
        return state

    async def _step_6_image_inserter(self, state):
        final_md = state.get("final_output", {}).get("final_markdown", "")
        images = state.get("images", [])

        if not final_md or not images:
            return state

        new_md = await self.image_inserter.insert(final_md, images)
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
            max_brand=1,
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
        render_data = {
            "title": final_output.get("title"),
            "meta_title": final_output.get("meta_title"),
            "meta_description": final_output.get("meta_description"),
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

        # if html_content:
        #     html_content = html_content.replace('<table>', '<div class="table-wrapper">\n<table>')
        #     html_content = html_content.replace('</table>', '</table>\n</div>')
            
        return state
    
    # ---------------- UTILITIES ----------------
    # def _enforce_paragraph_structure(self, text: str) -> str:
    #     """
    #     Enforce a maximum of 3 sentences per paragraph.
    #     Splits existing paragraphs into smaller ones if they exceed the limit.
    #     """
    #     if not text:
    #         return text
            
    #     # Split by existing paragraphs
    #     paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    #     fixed = []

    #     for p in paragraphs:
    #         # Skip tables or bullet lists (simplified check)
    #         if p.startswith("|") or p.startswith("- ") or p.startswith("* "):
    #             fixed.append(p)
    #             continue

    #         # Split paragraph into sentences using regex that supports Arabic (؟) and English (.)
    #         # and handles common abbreviations or decimal points if possible (simplified here)
    #         sentences = re.split(r'(?<=[.!؟])\s+', p)

    #         if len(sentences) > 3:
    #             # Group into chunks of 3 sentences
    #             for i in range(0, len(sentences), 3):
    #                 chunk = " ".join(sentences[i:i+3])
    #                 fixed.append(chunk.strip())
    #         else:
    #             fixed.append(p)

    #     return "\n\n".join(fixed)

    def _enforce_paragraph_structure(self, text: str) -> str:
        """
        Enforce max 3 sentences per paragraph WITHOUT breaking markdown tables/lists.
        """
        if not text:
            return text

        # 1) Protect table blocks first (2+ pipe lines)
        table_pattern = re.compile(r'((?:^\s*\|.*\|\s*$\n?){2,})', re.MULTILINE)
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

    def _validate_section_output(self, content: str, section: Dict[str, Any], section_index: int, total_sections: int, area: str, cta_type: str) -> tuple[bool, List[str]]:
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
            if kw_count < 2:
                errors.append(f"Primary keyword '{primary_kw}' appears only {kw_count} time(s), need at least 2")

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

    # def sanitize_links(self, markdown: str, max_external=3, max_brand=1, brand_url=None):

    #     links = re.findall(r'\[(.*?)\]\((https?://.*?)\)', markdown)

    #     used_urls = set()
    #     external_count = 0
    #     brand_count = 0

    #     def replace_link(match):
    #         nonlocal external_count, brand_count
    #         text = match.group(1)
    #         url = match.group(2)

    #         if url in used_urls:
    #             return text

    #         if brand_url and url.startswith(brand_url):
    #             if brand_count >= max_brand:
    #                 return text
    #             brand_count += 1

    #         else:
    #             if external_count >= max_external:
    #                 return text
    #             external_count += 1

    #         used_urls.add(url)
    #         return match.group(0)

    #     cleaned = re.sub(
    #         r'\[(.*?)\]\((https?://.*?)\)',
    #         replace_link,
    #         markdown
    #     )

    #     return cleaned

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
        """Enforces H1 length rules (60-70 chars) as per the framework."""
        return 60 <= len(h1) <= 70

    def validate_strategy_alignment(self, strategy, primary_keyword, area):
        angle = strategy.get("primary_angle", "").lower()
        if primary_keyword.lower() not in angle:
            return False, "Primary keyword not reflected in strategy angle"

        if area and area.lower() not in strategy.get("strategic_positioning","").lower():
            return False, "Local positioning missing"

        return True, None

    def _assemble_final_output(self, state: Dict[str, Any]) -> Dict[str, Any]:
        input_data = state.get("input_data", {})
        final_out = state.get("final_output", {})
        seo_meta = state.get("seo_meta", {})
        images = state.get("images", [])
        seo_report = state.get("seo_report", {})
        # state["performance"] = self.ai_client.observer.summarize_model_calls()
        performance = self.ai_client.observer.summarize_model_calls()


        return {
            "title": input_data.get("title", "Untitled"),
            "slug": state.get("slug", "unknown"),
            "primary_keyword": state.get("primary_keyword", ""),
            "final_markdown": final_out.get("final_markdown", ""),
            "article_language": state.get("article_language", "en"),

            # SEO
            "meta_title": seo_meta.get("meta_title", ""),
            "meta_description": seo_meta.get("meta_description", ""),
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
            commercial_sections = [
                s for s in h2_sections
                if s.get("section_intent") in ["Commercial", "Transactional"]
            ]

            ratio = len(commercial_sections) / max(len(h2_sections), 1)

            if ratio < 0.6:
                errors.append(f"Commercial intent distribution too weak ({ratio:.0%}). Brand articles require at least 60% commercial/transactional H2 sections.")

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

        covered = sum(
            1
            for sec in h2_sections
            if any(
                q.lower() in sec.get("heading_text", "").lower()
                for q in paa_questions
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

        # Update the first FAQ section
        first_faq["questions"] = list(dict.fromkeys(all_questions)) # Deduplicate
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
            plan["structure_rule"] = "recap + final CTA"

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
        Validates that each paragraph contains exactly 2 to 3 sentences.
        Returns False if any paragraph (non-list/table) violates this.
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
            
            # User requirement: exactly 2-3 sentences
            if not (2 <= len(sentences) <= 3):
                return False

        return True

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
        
    # def _sanitize_section_links(self, content: str, state: Dict[str, Any], brand_url: str, max_external: int = 1) -> str:
    #     if not content:
    #         return content

    #     internal_set = state.get("internal_url_set", set()) or set()
    #     blocked_domains = state.get("blocked_external_domains", set()) or set()
    #     allowed_domains = state.get("allowed_external_domains", set()) or set()
    #     max_external = state.get("max_external_links", 3)

    #     used_external = set(state.get("used_external_links", []))
    #     external_count = 0

    #     pattern = r'\[([^\]]+)\]\(([^)]+)\)'

    #     def repl(m):
    #         nonlocal external_count
    #         text, raw_url = m.group(1), m.group(2).strip()

    #         # kill invalid markdown links
    #         if raw_url.lower() in {"none", "null", ""}:
    #             return text

    #         if not raw_url.startswith("http"):
    #             return text

    #         cu = self._canon_url(raw_url)
    #         dom = self._domain(cu)

    #         # INTERNAL by exact set OR same site
    #         if cu in internal_set or self._is_same_site(cu, brand_url):
    #             return f"[{text}]({raw_url})"

    #         # external rules
    #         if dom in blocked_domains:
    #             return text
    #         if not self._is_authority_domain(dom, allowed_domains):
    #             return text
    #         if cu in used_external:
    #             return text
    #         # if external_count >= max_external:
    #         if len(used_external) >= max_external:
    #             return text

    #         external_count += 1
    #         used_external.add(cu)
    #         return f"[{text}]({raw_url})"

    #     cleaned = re.sub(pattern, repl, content)
    #     state["used_external_links"] = list(used_external)
    #     return cleaned

    def _sanitize_section_links(self, content: str, state: Dict[str, Any], brand_url: str, max_external: int = None) -> str:
        if not content:
            return content

        if brand_url in {"None", "", None}:
            brand_url = ""

        internal_set = state.get("internal_url_set", set()) or set()
        blocked_domains = state.get("blocked_external_domains", set()) or set()
        allowed_domains = state.get("allowed_external_domains", set()) or set()

        used_external = set(state.get("used_external_links", []))
        if max_external is None:
            max_external = state.get("max_external_links", 3)

        pattern = r'\[([^\]]+)\]\(([^)]+)\)'

        def repl(m):
            text, raw_url = m.group(1), m.group(2).strip()

            if raw_url.lower() in {"none", "null", ""}:
                return text

            if not raw_url.startswith("http"):
                return text

            cu = self._canon_url(raw_url)
            dom = self._domain(cu)

            # internal
            if cu in internal_set or (brand_url and self._is_same_site(cu, brand_url)):
                return f"[{text}]({raw_url})"

            # blocked
            if dom in blocked_domains:
                return text

            # strict whitelist
            if allowed_domains and dom not in allowed_domains:
                return text

            # global limit
            if cu in used_external or len(used_external) >= max_external:
                return text

            used_external.add(cu)
            return f"[{text}]({raw_url})"

        cleaned = re.sub(pattern, repl, content)
        state["used_external_links"] = list(used_external)
        return cleaned

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