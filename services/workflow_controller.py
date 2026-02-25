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
BASE_DIR = Path(__file__).resolve().parents[1] 

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO, format="%(message)s")

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
        state.setdefault("content_type", "editorial")
        state.setdefault("brand_link_used", 0)

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
            ("image_prompting", self._step_4_generate_image_prompts, 0),
            ("image_generation", self._step_4_5_download_images, 2),
            # ("section_validation", self._step_4_validate_sections, 0),
            ("assembly", self._step_5_assembly, 0),
            ("image_inserter", self._step_6_image_inserter, 0),
            ("meta_schema", self._step_7_meta_schema, 0),
            ("article_validation", self._step_8_article_validation, 0),
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
        article_language = user_lang if user_lang else (detect(raw_title) if raw_title else "en")
        area = input_data.get("area")
        state["area"] = area
        state["article_language"] = article_language
        state["primary_keyword"] = primary_keyword
        state["raw_title"] = raw_title
        state["keywords"] = keywords

        # Generate slug and directory
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        slug_base = self._sluggify(primary_keyword)
        slug = f"{slug_base}_{timestamp}"
        state["slug"] = slug
        
        output_dir = os.path.join(self.work_dir, slug)
        os.makedirs(output_dir, exist_ok=True)
        state["output_dir"] = output_dir

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
        raw_title = state.get("raw_title")
        primary_keyword = state.get("primary_keyword")
        article_language = state["input_data"].get("article_language")
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

        with open("prompts/templates/00_seo_intent_title.txt") as f:
            template = Template(f.read())

        prompt = template.render(
            raw_title=raw_title,
            primary_keyword=primary_keyword,
            article_language=article_language,
            serp_titles=top_titles,
            serp_cta_styles=cta_styles,
            area=area
        )

        raw = await self.ai_client.send(prompt, step="intent_title")

        clean = re.sub(r"```json|```", "", raw).strip()
        data = recover_json(clean) or {}

        intent = data.get("intent", "Informational")
        serp_confirmed = (
            state.get("seo_intelligence", {})
                .get("strategic_analysis", {})
                .get("intent_analysis", {})
                .get("confirmed_intent")
        )
        confidence = state["seo_intelligence"]["strategic_analysis"]["intent_analysis"]["intent_confidence_score"]

        if confidence > 0.6:
            intent = serp_confirmed

        optimized_title = data.get("optimized_title", raw_title)

        state["intent"] = intent
        if intent in ["Transactional", "Commercial"]:
            state["content_type"] = "brand"
        else:
            state["content_type"] = "editorial"
        # state["content_strategy"] = {
        #     "intent": intent,
        #     "area": state.get("area"),
        #     "competitive_mode": "serp_driven"
        # }
        state["intent"] = intent
        state["input_data"]["title"] = optimized_title

        return state

    async def _step_0_style_analysis(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Analyzes the reference image if provided to determine the brand's visual style."""
        ref_path = state.get("input_data", {}).get("logo_reference_path")
        
        if ref_path and os.path.exists(ref_path):
            logger.info(f"Analyzing brand style from reference: {ref_path}")
            style_desc = await self.ai_client.describe_image_style(ref_path)
            state["brand_visual_style"] = style_desc
        else:
            state["brand_visual_style"] = ""
            
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
        # area = state.get("input_data", {}).get("area", "Global")
        full_intel = seo_intelligence.get("strategic_analysis", {})

        intent_layer = full_intel.get("intent_analysis", {})
        structural_layer = full_intel.get("structural_intelligence", {})
        strategic_layer = full_intel.get("strategic_intelligence", {})

        clusters = strategic_layer.get("keyword_clusters", [])
        if not clusters:
            # Safety Fallback: Reconstruct from semantic assets if clusters are missing
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

        raw = await self.ai_client.send(prompt, step="content_strategy")

        clean = re.sub(r"```json|```", "", raw).strip()
        data = recover_json(clean) or {}

        state["content_strategy"] = data

        logger.info(f"CONTENT STRATEGY GENERATED:\n{json.dumps(data, indent=2)}")

        return state

    async def _step_1_outline(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Generates the article outline with a soft retry loop for validation failures."""
        
        input_data = state.get("input_data", {})
        title = input_data.get("title") or "Untitled"
        keywords = input_data.get("keywords") or []
        urls_raw = input_data.get("urls", [])
        seo_intelligence = state.get("seo_intelligence", {})
        content_strategy = state.get("content_strategy", {})
        area = state.get("area")
        
        content_type = state.get("content_type", "editorial") or "editorial"
        intent = state.get("intent") or "Informational"
        article_language = input_data.get("article_language", "en")
        
        feedback = None
        outline = []
        outline_data = {}

        for attempt in range(3):
            logger.info(f"Generating outline (Attempt {attempt + 1}/3)...")
            outline_data = await self.outline_gen.generate(
                title=title,
                keywords=keywords,
                urls=urls_raw,
                article_language=article_language,
                intent=intent,
                seo_intelligence=seo_intelligence,
                content_type=content_type,
                content_strategy=content_strategy,
                area=area,
                feedback=feedback
            )

            if not outline_data or not outline_data.get("outline"):
                if attempt < 2:
                    feedback = "Outline generation returned empty result. Please provide a full, structured JSON outline."
                    continue
                raise RuntimeError("Outline generation returned empty result after 3 attempts.")
            
            outline = outline_data.get("outline", [])
            
            # Validation Layer
            errors = []
            
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
        paa_questions = seo_intelligence.get("semantic_assets", {}).get("paa_questions", [])
        self.enforce_paa_sections(outline, paa_questions, min_percent=0.15)
        
        keyword_expansion = outline_data.get("keyword_expansion", {})
        state["global_keywords"] = keyword_expansion
        
        lsi_keywords = keyword_expansion.get("lsi", [])
        if lsi_keywords:
            lsi_pool = lsi_keywords.copy()
            for sec in outline:
                sec_lsi = lsi_pool[:3]
                sec["assigned_keywords"].extend(sec_lsi)
                lsi_pool = lsi_pool[3:]
    
        for idx, sec in enumerate(outline):
            self.outline_gen._normalize_section(
                sec,
                idx,
                content_type,
                content_strategy,
                area
            )
        
        urls_norm = normalize_urls(urls_raw)
        state["brand_url"] = urls_norm[0].get("link") if urls_norm else None
        outline = DataInjector.distribute_urls_to_outline(outline, urls_norm, strategy="conservative")
        
        state["link_strategy"] = {
            "internal_topics": [u for u in urls_norm if u.get("type") == "internal"],
            "authority_topics": [u for u in urls_norm if u.get("type") == "authority"],
            "affiliate_policy": {"max_per_section": 3, "placement": "distributed", "tone": "neutral"}
        }
        
        primary_keywords = keywords[:]
        primary_keyword = primary_keywords[0] if primary_keywords else title
        for sec in outline:
            sec["primary_keywords"] = primary_keywords
            sec["primary_keyword"] = primary_keyword
            sec["article_language"] = article_language
            if not sec.get("assigned_keywords"):
                 # Robust safety fallback
                 sec["assigned_keywords"] = keywords[:3] if keywords else [primary_keyword]
        
        state["outline"] = outline
        return state
        content_strategy = state.get("content_strategy", {})
        area = state.get("area")
        
        content_type = state.get("content_type", "editorial") or "editorial"
        intent = state.get("intent") or "Informational"
        content_strategy = state.get("content_strategy", {})
        article_language = input_data.get("article_language", "en")
        
        outline_data = await self.outline_gen.generate(
            title=title,
            keywords=keywords,
            urls=urls_raw,
            article_language=article_language,
            intent=intent,
            seo_intelligence=seo_intelligence,
            content_type=content_type,
            content_strategy=content_strategy,
            area=area,
        )

        if not outline_data:
            raise RuntimeError("Outline generation returned empty result.")
        
        outline = outline_data.get("outline", [])
        
        # -----------------------------------
        # Step 1: Enforce base structure & PAA
        outline = self._enforce_outline_structure(
            outline,
            intent=intent,
            area=area,
            content_type=content_type
        )
        
        outline = self._enforce_intent_distribution(
            outline,
            state["intent"],
            state["content_type"]
        )

        outline = self._inject_local_seo(outline, state.get("area"))

        outline = self._enforce_content_angle(
            outline,
            state.get("content_strategy")
        )

        outline = self._adjust_paa_by_intent(
            outline,
            state["intent"]
        )

        self._validate_outline_quality(outline, state["intent"])

        paa_questions = seo_intelligence.get("semantic_assets", {}).get("paa_questions", [])
        paa_check = self.enforce_paa_sections(outline, paa_questions, min_percent=0.15)
        if not paa_check["paa_ok"]:
            logger.warning(
                f"[paa_validate] PAA coverage too low: {paa_check['paa_ratio']:.0%} "
                f"(missing ~{paa_check['missing_count']} PAA-inspired H2s). "
                f"Prompt 01_outline_generator.txt should produce ≥15% PAA coverage."
            )
        
        present_types = {
            (s.get("section_type") or "").lower().strip() for s in outline
        }
        if "faq" not in present_types:
            logger.warning(
                "[outline_validate] Missing section_type='faq'. "
                "Prompt 01_outline_generator.txt must include a faq section."
            )
        if "conclusion" not in present_types:
            logger.warning(
                "[outline_validate] Missing section_type='conclusion'. "
                "Prompt 01_outline_generator.txt must include a conclusion section."
            )
        
        # -----------------------------------
        # Step 4: Prevent duplicate H2 headings
        seen_h2 = set()
        unique_outline = []
        for sec in outline:
            if (sec.get("heading_level") or "").upper() == "H2" and sec["heading_text"] in seen_h2:
                sec["heading_text"] += f" ({len(seen_h2)+1})"
            seen_h2.add(sec["heading_text"])
            unique_outline.append(sec)
        outline = unique_outline
        
        # -----------------------------------
        # Step 5: Normalize sections & distribute LSI keywords
        keyword_expansion = outline_data.get("keyword_expansion", {})
        state["global_keywords"] = keyword_expansion
        
        lsi_keywords = keyword_expansion.get("lsi", [])
        if lsi_keywords:
            # Round-robin distribution across sections
            # for idx, sec in enumerate(outline):
            #     sec["assigned_keywords"].extend([lsi_keywords[i % len(lsi_keywords)] for i in range(idx, idx+3)])
        
            lsi_pool = lsi_keywords.copy()

            for sec in outline:
                sec_lsi = lsi_pool[:3]
                sec["assigned_keywords"].extend(sec_lsi)
                lsi_pool = lsi_pool[3:]
    
        for idx, sec in enumerate(outline):
            self.outline_gen._normalize_section(
                sec,
                idx,
                content_type,
                content_strategy,
                area
            )
        
        # -----------------------------------
        urls_norm = normalize_urls(urls_raw)
        state["brand_url"] = urls_norm[0].get("link") if urls_norm else None
        outline = DataInjector.distribute_urls_to_outline(outline, urls_norm, strategy="conservative")
        
        state["link_strategy"] = {
            "internal_topics": [u for u in urls_norm if u.get("type") == "internal"],
            "authority_topics": [u for u in urls_norm if u.get("type") == "authority"],
            "affiliate_policy": {
                "max_per_section": 3,
                "placement": "distributed",
                "tone": "neutral"
            }
        }
        
        primary_keywords = keywords[:]
        primary_keyword = primary_keywords[0] if primary_keywords else title
        for sec in outline:
            sec["primary_keywords"] = primary_keywords
            sec["primary_keyword"] = primary_keyword
            sec["article_language"] = article_language
        
        for sec in outline:
            if not sec.get("assigned_keywords"):
                raise ContentGeneratorError(f"Section {sec.get('section_id')} missing assigned keywords.")
        
        state["outline"] = outline
        return state

    async def _step_2_write_sections(self, state: Dict[str, Any]) -> Dict[str, Any]:
        input_data = state.get("input_data", {})
        title = input_data.get("title", "Untitled")
        outline = state.get("outline", [])
        global_keywords = state.get("global_keywords", {})
        intent = state.get("intent", "Informational")
        article_language = input_data.get("article_language", "en")
        seo_intelligence = state.get("seo_intelligence", {})
        link_strategy = state.get("link_strategy", {})
        
        if not outline:
            raise RuntimeError("No outline found for section writing.")

        content_type = state.get("content_type", "editorial")

        tasks = [
            self._write_single_section(
                title,
                global_keywords,
                section,
                intent,
                seo_intelligence,
                content_type,
                link_strategy,
                state
            )
            for section in outline
        ]

        if PARALLEL_SECTIONS:
            logger.info(f"Writing {len(tasks)} sections in PARALLEL mode")
            results = await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logger.info(f"Writing {len(tasks)} sections in SEQUENTIAL mode")
            results = []
            for t in tasks:
                results.append(await t)

        sections_content = {}
        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Section failed: {res}")
                continue
            if res:
                sections_content[res["section_id"]] = res

        state["sections"] = sections_content
        if sections_content:
            first_section = list(sections_content.values())[0]
            if state.get("area"):
                if state["area"].lower() not in first_section["generated_content"].lower():
                    logger.warning("Local area missing in first section content")

        logger.info(f"Successfully wrote {len(sections_content)} sections.")
        return state

    async def _write_single_section( self, title: str, global_keywords: Dict[str, Any], section: Dict[str, Any], article_intent: str, seo_intelligence: Dict[str, Any], content_type: str, link_strategy: Dict[str, Any], state: Dict[str, Any],)-> Optional[Dict[str, Any]]:
        """Worker to write one section."""
        
        section_id = section.get("section_id") or section.get("id")
        # content_type = state.get("content_type", "editorial")
        urls = state.get("input_data", {}).get("urls", [])
        # brand_url = urls[0].get("link") if urls else None
        brand_url = state.get("brand_url")

        brand_link_used = state.get("brand_link_used", 0)

        # if urls:
        #     brand_url = urls[0].get("link")

        brand_link_used = state.get("brand_link_used", 0)

        execution_plan = self._build_execution_plan(section, state)

        content = await self.section_writer.write(
            title=title,
            global_keywords=global_keywords,
            section=section,
            article_intent=article_intent,
            seo_intelligence=seo_intelligence,
            content_type=content_type,
            link_strategy=link_strategy,
            brand_url=brand_url,
            brand_link_used=brand_link_used,
            brand_link_allowed=(brand_link_used == 0),
            allow_external_links=True,
            execution_plan=execution_plan,
            area=state.get("area")
        )

        if content and brand_url and brand_url in content:
            state["brand_link_used"] = 1

        if content:
            return {
                **section,
                "section_id": section_id,
                "generated_content": content
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
        primary_keyword = (keywords[0] if keywords else "") or ""
        logo_path = state.get("input_data", {}).get("logo_path")
        
        # image_client.generate_images is now async
        images = await self.image_client.generate_images(
            prompts, 
            primary_keyword=primary_keyword,
            logo_path=logo_path
        )

        # Normalize paths for markdown linking
        for img in images:
            if "local_path" in img:
                # Images are in output/images, articles are in output/slug/
                # We want the path in the markdown to be ../images/filename.webp
                # so it works from within the article folder.
                img["local_path"] = f"../images/{os.path.basename(img['local_path'])}"

        state["images"] = images
        return state
 
    async def _step_5_assembly(self, state):
        title = state.get("input_data", {}).get("title", "Untitled")
        outline = state.get("outline", [])
        # sections_list = list(state["sections"].values())
        sections_dict = state.get("sections", {})
        article_language = state.get("input_data", {}).get("article_language", "ar")

        ordered_sections = [
            sections_dict[s["section_id"]]
            for s in outline
            if s.get("section_id") in sections_dict
        ]

        assembled = await self.assembler.assemble(title=title, sections=ordered_sections, article_language=article_language)
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
            article_language=state.get("input_data", {}).get("article_language", "en"),
            secondary_keywords=state.get("input_data", {}).get("keywords", []),
            include_meta_keywords=state.get("include_meta_keywords", False),
            article_url=state.get("final_url")
        )

        meta_json = recover_json(meta_raw)

        if not meta_json:
            logger.error("Meta schema returned invalid JSON")
            return state

        meta_json = enforce_meta_lengths(meta_json)

        state["seo_meta"] = meta_json
        return state

    async def _step_8_article_validation(self, state):

        final_md = state.get("final_output", {}).get("final_markdown", "")
        meta = state.get("seo_meta", {})
        images = state.get("images", [])
        input_data = state.get("input_data", {})

        title = input_data.get("title", "")
        article_language = input_data.get("article_language", "en")
        keywords = input_data.get("keywords", [])
        primary_keyword = keywords[0] if keywords else ""

        if not final_md:
            state["seo_report"] = {
                "status": "FAIL",
                "issues": ["Final markdown missing"]
            }
            return state
        
        
        final_md = self.sanitize_links(
            final_md,
            max_external=3,
            max_brand=1,
            brand_url=state.get("brand_url")
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

        if state.get("content_type") == "brand":
            ratio = self.calculate_sales_density(final_md)
            
            # Dynamic threshold based on SERP structural intelligence
            serp_strat = state.get("seo_intelligence", {}).get("strategic_analysis", {})
            struct_intel = serp_strat.get("structural_intelligence", {})
            cta_intensity = str(struct_intel.get("cta_intensity_pattern", "soft")).lower()
            
            threshold_map = {
                "soft": 0.25,
                "moderate": 0.35,
                "aggressive": 0.45
            }
            required_ratio = threshold_map.get(cta_intensity, 0.25)

            if ratio < required_ratio:
                critical_issues.append(f"Sales density too low: {ratio} (Target: {required_ratio} based on {cta_intensity} SERP pattern)")

        ok, local_issues = self.validate_local_seo(
            final_md,
            meta,
            state.get("area")
        )
        critical_issues.extend(local_issues)

        ok, angle_issue = self.validate_content_angle(
            final_md,
            state.get("content_strategy", {})
        )
        if not ok:
            critical_issues.append(angle_issue)

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
            "output_dir": output_dir
        }
        
        try:
            html_path = render_html_page(render_data)
            logger.info(f"HTML Page rendered successfully at: {html_path}")
            state["html_path"] = html_path
        except Exception as e:
            logger.error(f"Failed to render HTML page: {e}")
            
        return state
    
    # ---------------- UTILITIES ----------------
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

    def sanitize_links(self, markdown: str, max_external=3, max_brand=1, brand_url=None):

        links = re.findall(r'\[(.*?)\]\((https?://.*?)\)', markdown)

        used_urls = set()
        external_count = 0
        brand_count = 0

        def replace_link(match):
            nonlocal external_count, brand_count
            text = match.group(1)
            url = match.group(2)

            if url in used_urls:
                return text

            if brand_url and url.startswith(brand_url):
                if brand_count >= max_brand:
                    return text
                brand_count += 1

            else:
                if external_count >= max_external:
                    return text
                external_count += 1

            used_urls.add(url)
            return match.group(0)

        cleaned = re.sub(
            r'\[(.*?)\]\((https?://.*?)\)',
            replace_link,
            markdown
        )

        return cleaned

    def validate_intent_from_serp(serp_data, ai_intent):
        top = serp_data.get("top_results", [])
        if not top:
            return ai_intent

        service_like = 0
        editorial_like = 0

        for r in top:
            h1 = (r.get("headings", {}).get("h1", "")).lower()
            cta = r.get("cta_style", "")
            word_count = r.get("estimated_word_count", 0)

            if cta in ["soft commercial", "aggressive"]:
                service_like += 1
            elif word_count > 1200:
                editorial_like += 1

        if service_like >= 2:
            return "Commercial"

        if editorial_like >= 2:
            return "Informational"

        return ai_intent

    def validate_strategy_alignment(strategy, primary_keyword, area):
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
    _BRAND_ROLES:     ClassVar[set] = {"benefits"}

    def _enforce_outline_structure(self, outline: List[Dict[str, Any]], intent: str, area: Optional[str], content_type: str,) -> List[Dict[str, Any]]:
        """
        VALIDATES that the LLM-generated outline contains the required semantic
        section_type roles.  Does NOT inject or mutate heading_text.

        If a mandatory role is missing, a WARNING is logged so the prompt can
        be iterated on – the pipeline still continues (soft validation).
        """
        present_types = {
            (s.get("section_type") or "").lower().strip()
            for s in outline
        }

        # --- universal mandatory roles ---
        for role in self._MANDATORY_ROLES:
            if role not in present_types:
                logger.warning(
                    f"[outline_validate] Missing mandatory section_type='{role}'. "
                    f"Check 01_outline_generator.txt prompt."
                )

        # --- content-type specific roles ---
        if content_type == "editorial":
            for role in self._EDITORIAL_ROLES:
                if role not in present_types:
                    logger.warning(
                        f"[outline_validate] Editorial outline missing section_type='{role}'. "
                        f"Expected for content_type='editorial'."
                    )

        elif content_type == "brand":
            for role in self._BRAND_ROLES:
                if role not in present_types:
                    logger.warning(
                        f"[outline_validate] Brand outline missing section_type='{role}'. "
                        f"Expected for content_type='brand'."
                    )

        # --- assign section_ids for any section that is missing one ---
        for i, sec in enumerate(outline):
            if not sec.get("section_id"):
                sec["section_id"] = f"sec_{i+1:02d}"

        return outline

    def _enforce_intent_distribution(self, outline, intent, content_type):
        errors = []
        h2_sections = [s for s in outline if (s.get("heading_level") or "").upper() == "H2"]

        if content_type == "brand":
            commercial_sections = [
                s for s in h2_sections
                if s.get("section_intent") in ["Commercial", "Transactional"]
            ]

            ratio = len(commercial_sections) / max(len(h2_sections), 1)

            if ratio < 0.6:
                errors.append(f"Commercial intent distribution too weak ({ratio:.0%}). Brand articles require at least 60% commercial/transactional H2 sections.")

        if intent == "Informational":
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

    def _adjust_paa_by_intent(self, outline, intent):
        if intent in ["Transactional", "Commercial"]:
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

        return errors

    def _build_execution_plan(self, section, state):
        content_type = state.get("content_type")
        intent = state.get("intent")
        area = state.get("area")

        plan = {}
        plan["structure_rule"] = "standard structured paragraphs"
        
        # Writing Mode
        if content_type == "brand":
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
        if content_type == "brand" and section.get("section_type") == "core":
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

    def calculate_sales_density(self, markdown: str):
        sales_terms = [
            "خدمة", "شركة", "نقدم", "نساعدك", "تواصل",
            "عرض سعر", "استشارة", "احجز", "أفضل شركة"
        ]

        paragraphs = [p.strip() for p in markdown.split("\n") if p.strip()]
        sales_count = 0

        for p in paragraphs:
            if any(term in p for term in sales_terms):
                sales_count += 1

        ratio = sales_count / max(len(paragraphs), 1)
        return round(ratio, 2)

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

    # async def _step_1_outline(self, state: Dict[str, Any]) -> Dict[str, Any]:
    #     """Generates the article outline using AI."""
    #     input_data = state.get("input_data", {})
    #     title = input_data.get("title") or "Untitled"
    #     keywords = input_data.get("keywords") or []
    #     urls_raw = input_data.get("urls", [])
    #     seo_intelligence = state.get("seo_intelligence", {})
    #     content_strategy = state.get("content_strategy", {})
    #     area = state.get("area")
        
    #     content_type = state.get("content_type", "editorial")
    #     if not content_type:
    #         content_type = "editorial"

    #     intent = state.get("intent") or "Informational"
    #     article_language = input_data.get("article_language", "en")

    #     outline_data = await self.outline_gen.generate(
    #         title=title,
    #         keywords=keywords,
    #         urls=urls_raw,
    #         article_language=article_language,
    #         intent=intent,
    #         seo_intelligence=seo_intelligence,
    #         content_type=content_type,
    #         content_strategy=content_strategy,
    #         area=area
    #     )

    #     if not outline_data:
    #         raise RuntimeError("Outline generation returned empty result.")

    #     outline = outline_data.get("outline", [])

    #     # enforce first
    #     outline = self._enforce_outline_structure(
    #         outline,
    #         intent=intent,
    #         area=area,
    #         content_type=content_type
    #     )
    #     paa_questions = seo_intelligence.get("semantic_assets", {}).get("paa_questions", [])
    #     outline = self.enforce_paa_sections(outline, paa_questions, min_percent=0.3)
    #     rules = content_strategy.get("rules", {})
    #     if rules.get("faq_required") and not any("FAQ" in sec["heading_text"] for sec in outline):
    #         outline.append(self.generate_faq_section(seo_intelligence.get("semantic_assets", {}).get("paa_questions", [])))
    #     # Force Conclusion if missing
    #     if not any("خاتمة" in sec.get("heading_text", "") or 
    #        "Conclusion" in sec.get("heading_text", "")
    #        for sec in outline):

    #         outline.append({
    #             "section_id": f"sec_{len(outline)+1:02}",
    #             "heading_level": "H2",
    #             "heading_text": "الخاتمة",
    #             "section_intent": state.get("intent", "Informational"),
    #             "content_goal": "تلخيص المقال وتوجيه القارئ لاتخاذ القرار",
    #             "assigned_keywords": [state.get("primary_keyword", "")],
    #             "content_scope": "تلخيص المزايا والعيوب وتوصية نهائية واضحة",
    #             "forbidden_elements": [],
    #             "allowed_flow_steps": ["Summary", "Recommendation", "CTA"],
    #             "image_plan": {
    #                 "required": False,
    #                 "image_type": "none",
    #                 "alt_text": ""
    #             },
    #             "cta_allowed": True,
    #             "cta_type": "soft",
    #             "cta_rules": {
    #                 "placement": "none",
    #                 "max_sentences": 1,
    #                 "mandatory": False
    #             },
    #             "requires_table": False,
    #             "table_columns": [],
    #             "estimated_word_count_min": 150,
    #             "estimated_word_count_max": 250
    #         })

    #     for idx, sec in enumerate(outline):
    #         self.outline_gen._normalize_section(
    #             sec,
    #             idx,
    #             content_type,
    #             content_strategy,
    #             area
    #         )

    #     keyword_expansion = outline_data.get("keyword_expansion", {})
    #     state["global_keywords"] = keyword_expansion

    #     urls_norm = normalize_urls(urls_raw)
    #     brand_url = urls_norm[0].get("link") if urls_norm else None
    #     state["brand_url"] = brand_url

    #     # Use "conservative" strategy for Guest Post / External publishing mode
    #     outline = DataInjector.distribute_urls_to_outline(outline, urls_norm, strategy="conservative")

    #     state["link_strategy"] = {
    #         "internal_topics": [u for u in urls_norm if u.get("type") == "internal"],
    #         "authority_topics": [u for u in urls_norm if u.get("type") == "authority"],
    #         "affiliate_policy": {
    #             "max_per_section": 3,
    #             "placement": "distributed",
    #             "tone": "neutral"
    #         }
    #     }

    #     primary_keywords = keywords[:] 
    #     primary_keyword = primary_keywords[0] if primary_keywords else title

    #     for sec in outline:
    #         sec["primary_keywords"] = primary_keywords
    #         sec["primary_keyword"] = primary_keyword
    #         sec["article_language"] = article_language

    #     for sec in outline:
    #         if not sec.get("assigned_keywords"):
    #             raise ContentGeneratorError(
    #                 f"Section {sec.get('section_id')} missing assigned keywords."
    #             )

    #     if not outline:
    #         raise ContentGeneratorError("AI returned empty outline list.")

    #     state["outline"] = outline
    #     return state



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

    # async def _step_0_analysis(self, state: Dict[str, Any]) -> Dict[str, Any]:
    #     """Setup unique directories and sluggification."""

    #     input_data = state.get("input_data", {})
    #     raw_title = input_data.get("title", "Untitled Article")
    #     keywords = input_data.get("keywords", [])
    #     primary_keyword = keywords[0] if keywords else raw_title
    #     user_lang = input_data.get("article_language")
    #     article_language = user_lang if user_lang else (detect(raw_title) if raw_title else "en")
        
    #     intent = await self._detect_intent_ai(raw_title, primary_keyword)

    #     valid_intents = {"Informational", "Commercial", "Transactional", "Comparative"}

    #     if intent not in valid_intents:
    #         logger.warning(f"Invalid intent returned: {intent}")
    #         intent = "Informational"

    #     # competitive_raw = await self.ai_client.send(
    #     #     f"Provide competitive SERP-style structural insights for the keyword: {primary_keyword}",
    #     #     step="competitive_analysis"
    #     # )
    #     # competitive_insights = recover_json(competitive_raw) or {"notes": competitive_raw}

    #     optimized_title = await self.title_generator.generate(
    #         raw_title=raw_title,
    #         primary_keyword=primary_keyword,
    #         intent=intent,
    #         article_language=article_language
    #     )

    #     state["input_data"]["title"] = optimized_title
        
    #     # Add timestamp to slug for unique folder
    #     import datetime
    #     timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    #     slug_base = self._sluggify(optimized_title)
    #     slug = f"{slug_base}_{timestamp}"
        
    #     state["primary_keyword"] = primary_keyword
    #     state["intent"] = intent
    #     state["slug"] = slug
    #     state["input_data"]["article_language"] = article_language
    #     # state["competitive_insights"] = competitive_insights

    #     article_dir = os.path.join(self.work_dir, "output", slug)
    #     image_dir = os.path.join(article_dir, "images")
    #     os.makedirs(image_dir, exist_ok=True)

        
    #     base_url = "https://yourdomain.com/"
    #     final_url = base_url + slug
    #     state["final_url"] = final_url

    #     # Update client storage path
    #     self.image_client.save_dir = image_dir
        
    #     state["output_dir"] = article_dir
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
