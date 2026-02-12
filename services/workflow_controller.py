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
from services.content_generator import OutlineGenerator, SectionWriter, Assembler
from services.section_validator import SectionValidator
from services.image_inserter import ImageInserter
from services.meta_schema_generator import MetaSchemaGenerator
from services.article_validator import ArticleValidator
from utils.json_utils import recover_json
from utils.seo_utils import enforce_meta_lengths
BASE_DIR = Path(__file__).resolve().parents[1] 

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PARALLEL_SECTIONS = False

class AsyncExecutor:
    """Executes async workflow steps with logging and retries."""

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
        # self.ai_client = GeminiClient()
        # self.ai_client = GroqClient()
        
        # self.ai_client = HuggingFaceClient(
        #     model="TheBloke/Llama-2-7B-Chat-GGML"
        # )

        self.work_dir = work_dir
        self.executor = AsyncExecutor()
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
        self.section_validator = SectionValidator(self.ai_client)
        self.image_inserter = ImageInserter()
        self.meta_schema = MetaSchemaGenerator(self.ai_client)
        self.article_validator = ArticleValidator(self.ai_client)
        
        # Image generator
        api_key = os.getenv("STABILITY_API_KEY")
        self.image_client = ImageGenerator(
            save_dir=os.path.join(work_dir, "output", "images"), 
            api_key=api_key
        )

    async def run_workflow(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Main entry point for the async pipeline."""
        # Initialize state keys
        state.setdefault("input_data", {})
        state.setdefault("seo_meta", {})
        state.setdefault("outline", [])
        state.setdefault("sections", {})
        state.setdefault("images", [])
        state.setdefault("final_output", {})

        steps = [
            ("analysis", self._step_0_analysis, 0),
            ("outline_generation", self._step_1_outline, 1),
            ("image_prompting", self._step_4_generate_image_prompts, 0),
            ("image_generation", self._step_4_5_download_images, 2),
            ("content_writing", self._step_2_write_sections, 1),
            ("section_validation", self._step_4_validate_sections, 0),
            ("assembly", self._step_5_assembly, 0),
            ("image_inserter", self._step_6_image_inserter, 0),
            ("meta_schema", self._step_7_meta_schema, 0),
            ("article_validation", self._step_8_article_validation, 0)
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

        competitive_raw = await self.ai_client.send(
            f"Provide competitive SERP-style structural insights for the keyword: {primary_keyword}",
            step="competitive_analysis"
        )
        competitive_insights = recover_json(competitive_raw) or {"notes": competitive_raw}

        optimized_title = await self.title_generator.generate(
            raw_title=raw_title,
            primary_keyword=primary_keyword,
            intent=intent,
            article_language=article_language
        )

        state["input_data"]["title"] = optimized_title
        slug = self._sluggify(optimized_title)
        state["input_data"]["article_language"] = article_language
        state["primary_keyword"] = primary_keyword
        state["intent"] = intent
        state["competitive_insights"] = competitive_insights

        article_dir = os.path.join(self.work_dir, "output", slug)
        image_dir = os.path.join(article_dir, "images")
        os.makedirs(image_dir, exist_ok=True)
        
        base_url = "https://yourdomain.com/"
        final_url = base_url + slug
        state["final_url"] = final_url

        # Update client storage path
        self.image_client.save_dir = image_dir
        
        state["slug"] = slug
        state["output_dir"] = article_dir
        return state

    async def _step_1_outline(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Generates the article outline using AI."""
        input_data = state.get("input_data", {})
        title = input_data.get("title") or "Untitled"
        keywords = input_data.get("keywords") or []
        urls_raw = input_data.get("urls", [])

        competitive_insights = state.get("competitive_insights", {})
        intent = state.get("intent") or "Informational"
        article_language = input_data.get("article_language", "en")

        outline_data = await self.outline_gen.generate(
            title=title,
            keywords=keywords,
            urls=urls_raw,
            article_language=article_language,
            competitive_insights=competitive_insights,
            intent=intent
        )

        if not outline_data:
            raise RuntimeError("Outline generation returned empty result.")

        outline = outline_data.get("outline", [])
        keyword_expansion = outline_data.get("keyword_expansion", {})

        state["outline"] = outline
        state["global_keywords"] = keyword_expansion

        urls_norm = normalize_urls(urls_raw)
        outline = DataInjector.distribute_urls_to_outline(outline, urls_norm)

        primary_keywords = keywords[:] 
        primary_keyword = primary_keywords[0] if primary_keywords else title

        for sec in outline:
            sec["primary_keywords"] = primary_keywords
            sec["primary_keyword"] = primary_keyword
            sec["article_language"] = article_language

        state["outline"] = outline
        return state

    async def _step_2_write_sections(self, state: Dict[str, Any]) -> Dict[str, Any]:
        input_data = state.get("input_data", {})
        title = input_data.get("title", "Untitled")
        outline = state.get("outline", [])
        global_keywords = state.get("global_keywords", {})
        intent = state.get("intent", "Informational")
        article_language = input_data.get("article_language", "en")
        if not outline:
            raise RuntimeError("No outline found for section writing.")

        tasks = [
            self._write_single_section(
                title,
                global_keywords,
                section,
                intent,
                state.get("competitive_insights", {})
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
        competitive_insights: Dict[str, Any]
    )-> Optional[Dict[str, Any]]:


        """Worker to write one section."""
        
        section_id = section.get("section_id") or section.get("id")

        content = await self.section_writer.write(
            title=title,
            global_keywords=global_keywords,
            section=section,
            article_intent=article_intent,
            competitive_insights=competitive_insights
        )

        if content:
            return {
                **section,
                "section_id": section_id,
                "generated_content": content
            }
        return None

    async def _step_4_validate_sections(self, state):
        input_data = state.get("input_data", {})
        title = input_data.get("title", "Untitled")
        article_language = input_data.get("article_language", "ar")

        sections = state.get("sections", {})
        outline = state.get("outline", [])

        failed_sections = []

        for sec in outline:
            sid = sec.get("section_id")
            content = sections.get(sid, {}).get("generated_content", "")

            if not content:
                continue

            result = await self.section_validator.validate(
                title,
                article_language,
                sec,
                content
            )

            sections[sid]["validation_report"] = result

            if result["status"].upper() == "FAIL":
                failed_sections.append({
                    "section_id": sid,
                    "issues": result.get("issues", [])
                })

        state["sections"] = sections
        state["failed_sections"] = failed_sections
        state["validation_passed"] = len(failed_sections) == 0

        return state

    async def _step_4_generate_image_prompts(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Generates image prompts using the image client."""
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

    # async def _step_5_validation(self, state: Dict[str, Any]) -> Dict[str, Any]:
    #     """Validates the output against SEO rules."""
    #     from utils.seo_validator import SEOValidator
    #     validator = SEOValidator()
        
    #     final_out = state.get("final_output", {})
    #     content = final_out.get("final_markdown", "")
        
    #     if not content:
    #         logger.warning("Validation skipped: no final content found.")
    #         return state
            
    #     metadata = {
    #         **state.get("seo_meta", {}), 
    #         "images": state.get("images", []), 
    #         "domain": "yourdomain.com"
    #     }
        
    #     # validator.validate is sync, wrapping to avoid blocking event loop
    #     report = await asyncio.to_thread(validator.validate, content, metadata)
    #     state["seo_report"] = report
    #     return stat
    
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

        keyword_count = clean_text.lower().count(keyword.lower())

        density = 0.0
        if word_count > 0:
            density = (keyword_count / word_count) * 1000  # per 1000 words

        return word_count, keyword_count, round(density, 2)

    def _assemble_final_output(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Compiles the final structured result."""
        final_out = state.get("final_output", {})
        input_data = state.get("input_data", {})
        
        return {
            "title": input_data.get("title", "Untitled"),
            "slug": state.get("slug", "unknown"),
            "final_markdown": final_out.get("final_markdown", ""),
            "meta_title": final_out.get("meta_title", ""),
            "meta_description": final_out.get("meta_description", ""),
            "images": state.get("images", []),
            "seo_report": state.get("seo_report", {}),
            "output_dir": state.get("output_dir", ""),
            "workflow_state": state,
            "raw_text": final_out.get("raw_text", "")
        }
