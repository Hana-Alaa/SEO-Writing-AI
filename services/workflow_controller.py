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
from typing import Dict, Any, List, Optional, Callable

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
            save_dir=os.path.join(work_dir, "output", "images"), 
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
            ("analysis", self._step_0_analysis, 0),
            ("web_research", self._step_web_research, 1),  
            # ("semantic_layer", self._step_semantic_layer, 1),
            ("serp_analysis", self._step_serp_analysis, 1),
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

    async def _step_0_analysis(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Setup unique directories and sluggification."""

        input_data = state.get("input_data", {})
        raw_title = input_data.get("title", "Untitled Article")
        keywords = input_data.get("keywords", [])
        primary_keyword = keywords[0] if keywords else raw_title
        user_lang = input_data.get("article_language")
        article_language = user_lang if user_lang else (detect(raw_title) if raw_title else "en")
        
        intent = await self._detect_intent_ai(raw_title, primary_keyword)

        valid_intents = {"Informational", "Commercial", "Transactional", "Comparative"}

        if intent not in valid_intents:
            logger.warning(f"Invalid intent returned: {intent}")
            intent = "Informational"

        # competitive_raw = await self.ai_client.send(
        #     f"Provide competitive SERP-style structural insights for the keyword: {primary_keyword}",
        #     step="competitive_analysis"
        # )
        # competitive_insights = recover_json(competitive_raw) or {"notes": competitive_raw}

        optimized_title = await self.title_generator.generate(
            raw_title=raw_title,
            primary_keyword=primary_keyword,
            intent=intent,
            article_language=article_language
        )

        state["input_data"]["title"] = optimized_title
        
        # Add timestamp to slug for unique folder
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        slug_base = self._sluggify(optimized_title)
        slug = f"{slug_base}_{timestamp}"
        
        state["primary_keyword"] = primary_keyword
        state["intent"] = intent
        state["slug"] = slug
        state["input_data"]["article_language"] = article_language
        # state["competitive_insights"] = competitive_insights

        article_dir = os.path.join(self.work_dir, "output", slug)
        image_dir = os.path.join(article_dir, "images")
        os.makedirs(image_dir, exist_ok=True)

        
        base_url = "https://yourdomain.com/"
        final_url = base_url + slug
        state["final_url"] = final_url

        # Update client storage path
        self.image_client.save_dir = image_dir
        
        state["output_dir"] = article_dir
        return state

    async def _step_web_research(self, state):

        primary_keyword = state["primary_keyword"]

        with open("prompts/templates/seo_web_research.txt") as f:
            template = Template(f.read())

        research_prompt = template.render(
            primary_keyword=primary_keyword
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


        # if not serp_data:
        #     logger.warning("SERP JSON parsing failed.")
        #     serp_data = {}

        # if serp_data.get("top_results"):
        #     serp_data["top_results"] = serp_data["top_results"][:5]

        # if serp_data.get("paa_questions"):
        #     serp_data["paa_questions"] = serp_data["paa_questions"][:8]

        # state["serp_data"] = serp_data

        # trimmed_serp = {
        #     "top_results": serp_data.get("top_results", [])[:3],
        #     "paa_questions": serp_data.get("paa_questions", [])[:6],
        #     "lsi_keywords": serp_data.get("lsi_keywords", [])[:15],
        #     "related_searches": serp_data.get("related_searches", [])[:8],
        #     "autocomplete_suggestions": serp_data.get("autocomplete_suggestions", [])[:8]
        # }

        # logger.info(f"SERP stored successfully: {len(trimmed_serp.get('top_results', []))} results")

        # state["serp_data"] = trimmed_serp
        # if not trimmed_serp.get("top_results"):
        #     raise RuntimeError("SERP returned no top results")
        # return state

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

    async def _step_serp_analysis(self, state):

        serp_data = state.get("serp_data", {})
        primary_keyword = state.get("primary_keyword")

        with open("prompts/templates/seo_serp_analysis.txt") as f:
            template = Template(f.read())

        # light_serp = {
        #     "paa": [q["question"] for q in serp_data.get("paa_questions", [])][:10],
        #     "lsi": serp_data.get("lsi_keywords", [])[:20],
        #     "related": serp_data.get("related_searches", [])[:15],
        #     "titles_pattern": [r["title"] for r in serp_data.get("top_results", [])][:5]
        # }
        
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

        if not serp_insights.get("keyword_clusters"):
            serp_insights["keyword_clusters"] = [
                {
                    "cluster_name": "Primary Cluster",
                    "keywords": [primary_keyword]
                }
            ]

        # state["seo_intelligence"] = serp_insights
        existing = state.get("seo_intelligence", {})
        existing.update(serp_insights)
        state["seo_intelligence"] = existing

        return state

    async def _step_1_outline(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Generates the article outline using AI."""
        input_data = state.get("input_data", {})
        title = input_data.get("title") or "Untitled"
        keywords = input_data.get("keywords") or []
        urls_raw = input_data.get("urls", [])
        seo_intelligence = state.get("seo_intelligence", {})
        
        content_type = state.get("content_type", "editorial")
        if not content_type:
            content_type = "editorial"

        intent = state.get("intent") or "Informational"
        article_language = input_data.get("article_language", "en")

        outline_data = await self.outline_gen.generate(
            title=title,
            keywords=keywords,
            urls=urls_raw,
            article_language=article_language,
            intent=intent,
            seo_intelligence=seo_intelligence,
            content_type=content_type
        )

        if not outline_data:
            raise RuntimeError("Outline generation returned empty result.")

        outline = outline_data.get("outline", [])
        # Force Conclusion if missing
        if not any("خاتمة" in sec.get("heading_text", "") or 
           "Conclusion" in sec.get("heading_text", "")
           for sec in outline):

            outline.append({
                "section_id": f"sec_{len(outline)+1:02}",
                "heading_level": "H2",
                "heading_text": "الخاتمة",
                "section_intent": state.get("intent", "Informational"),
                "content_goal": "تلخيص المقال وتوجيه القارئ لاتخاذ القرار",
                "assigned_keywords": [state.get("primary_keyword", "")],
                "content_scope": "تلخيص المزايا والعيوب وتوصية نهائية واضحة",
                "forbidden_elements": [],
                "allowed_flow_steps": ["Summary", "Recommendation", "CTA"],
                "image_plan": {
                    "required": False,
                    "image_type": "none",
                    "alt_text": ""
                },
                "cta_allowed": True,
                "cta_type": "soft",
                "cta_rules": {
                    "placement": "none",
                    "max_sentences": 1,
                    "mandatory": False
                },
                "requires_table": False,
                "table_columns": [],
                "estimated_word_count_min": 150,
                "estimated_word_count_max": 250
            })

        keyword_expansion = outline_data.get("keyword_expansion", {})
        state["global_keywords"] = keyword_expansion

        urls_norm = normalize_urls(urls_raw)
        brand_url = urls_norm[0].get("link") if urls_norm else None
        state["brand_url"] = brand_url

        # Use "conservative" strategy for Guest Post / External publishing mode
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
                raise ContentGeneratorError(
                    f"Section {sec.get('section_id')} missing assigned keywords."
                )

        if not outline:
            raise ContentGeneratorError("AI returned empty outline list.")

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
        

        # supporting_keywords = (
        #     global_keywords.get("lsi", []) +
        #     global_keywords.get("semantic", [])
        # )
        # supporting_keywords = global_keywords


        # serp_data=state.get("serp_data")
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
    )-> Optional[Dict[str, Any]]:
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
            allow_external_links=True
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

        image_prompts = await self.image_prompt_planner.generate(
            title=title,
            primary_keyword=primary_keyword,
            keywords=keywords,
            outline=outline
        )
        print("FINAL IMAGE PROMPTS COUNT:", len(image_prompts))

        state["image_prompts"] = image_prompts
        return state

    async def _step_4_5_download_images(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Downloads images (now parallel in the client)."""
        prompts = state.get("image_prompts", [])
        if not prompts:
            return state
            
        keywords = state.get("input_data", {}).get("keywords", [])
        primary_keyword = (keywords[0] if keywords else "") or ""
        
        # image_client.generate_images is now async
        images = await self.image_client.generate_images(prompts, primary_keyword=primary_keyword)
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

        report_raw = await self.article_validator.validate(
            final_markdown=final_md, 
            meta=meta, 
            images=images,
            title=title,
            article_language=article_language,
            primary_keyword=primary_keyword,
            word_count=word_count,
            keyword_count=keyword_count,
            keyword_density=keyword_density
        )

        report_json = recover_json(report_raw)

        if not isinstance(report_json, dict):
            state["seo_report"] = {
                "status": "FAIL",
                "issues": ["Validator returned malformed JSON"]
            }
            return state

        issues = report_json.get("issues", [])

        if report_json.get("status") not in ["PASS", "FAIL"]:
            report_json["status"] = "FAIL"

        state["seo_report"] = report_json
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
            "schema": seo_meta.get("schema", {}),

            # Media
            "images": images,

            # Validation
            "seo_report": seo_report,

            # Performance
            "performance": performance,

            # Debug / Storage
            "output_dir": state.get("output_dir", ""),
        }

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

