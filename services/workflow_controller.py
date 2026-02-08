"""
Phase 5 – Orchestration Layer (Asynchronous)
- Fully asynchronous pipeline for high-performance article generation.
- Parallelizes section writing and image generation.
- Implements robust error handling, logging, and retries.
"""

import logging
import os
import time
import re
import asyncio
from typing import Dict, Any, List, Optional, Callable

from services.image_generator import ImageGenerator, ImagePromptPlanner
# from services.openrouter_client import OpenRouterClient
from services.groq_client import GroqClient
# from services.gemini_client import GeminiClient
# from services.huggingface_client import HuggingFaceClient
from services.content_generator import OutlineGenerator, SectionWriter, Assembler

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
        # self.ai_client = OpenRouterClient()
        # self.ai_client = GeminiClient()
        self.ai_client = GroqClient()
        
        # self.ai_client = HuggingFaceClient(
        #     model="TheBloke/Llama-2-7B-Chat-GGML"
        # )

        self.work_dir = work_dir
        self.executor = AsyncExecutor()
        self.image_prompt_planner = ImagePromptPlanner(
            ai_client=self.ai_client,
            template_path=os.path.join(self.work_dir, "templates", "image_prompt_template.txt")
        )
            
        # Content generation services
        self.outline_gen = OutlineGenerator(self.ai_client)
        self.section_writer = SectionWriter(self.ai_client)
        self.assembler = Assembler(self.ai_client)
        
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
            ("content_writing", self._step_2_write_sections, 1),
            ("image_prompting", self._step_4_generate_image_prompts, 0),
            ("image_generation", self._step_4_5_download_images, 2),
            ("final_assembly", self._step_3_assembly, 0),
            ("seo_validation", self._step_5_validation, 0)
        ]

        for name, func, retries in steps:
            result = await self.executor.run_step(name, func, state, retries=retries)
            state = result.get("data", state)
            
            if result["status"] == "error":
                logger.error(f"Workflow stopped at critical step: {name}")
                break

        return self._assemble_final_output(state)

    # ---------------- COORDINATION STEPS (ASYNC) ----------------
    
    async def _step_0_analysis(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Setup unique directories and sluggification."""
        input_data = state.get("input_data", {})
        title = input_data.get("title", "Untitled Article")
        slug = self._sluggify(title)
        
        article_dir = os.path.join(self.work_dir, "output", slug)
        image_dir = os.path.join(article_dir, "images")
        os.makedirs(image_dir, exist_ok=True)
        
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
        
        urls = input_data.get("urls", [])
        outline = await self.outline_gen.generate(title, keywords, urls)
        if not outline:
            raise RuntimeError("Outline generation returned empty result.")
            
        state["outline"] = outline
        return state

    async def _step_2_write_sections(self, state: Dict[str, Any]) -> Dict[str, Any]:
        input_data = state.get("input_data", {})
        title = input_data.get("title", "Untitled")
        global_keywords = input_data.get("keywords", [])
        outline = state.get("outline", [])

        if not outline:
            raise RuntimeError("No outline found for section writing.")

        tasks = [
            self._write_single_section(title, global_keywords, section)
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

    async def _write_single_section(self, title: str, global_keywords: List[str], section: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Worker to write one section."""
        section_id = section.get("section_id") or section.get("id")
        content = await self.section_writer.write(title, global_keywords, section)
        
        if content:
            return {
                **section,
                "section_id": section_id,
                "generated_content": content
            }
        return None

    async def _step_4_generate_image_prompts(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Generates image prompts using the image client."""
        input_data = state.get("input_data", {})
        title = input_data.get("title", "Untitled")
        keywords = input_data.get("keywords", [])

        outline = state.get("outline", [])
        
        image_prompts = await self.image_prompt_planner.generate(
            title=title,
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

    async def _step_3_assembly(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Full assembly: stitch sections + insert images + generate final markdown and metadata."""
        input_data = state.get("input_data", {})
        title = input_data.get("title", "Untitled")
        sections_dict = state.get("sections", {})
        outline = state.get("outline", [])
        images = state.get("images", [])

        # Map section_id -> image
        image_map = {img['section_id']: img for img in images}

        final_sections = []
        for s in outline:
            sid = s.get("section_id")
            sec = sections_dict.get(sid)
            if not sec:
                continue
            content = sec.get("generated_content", "")

            # Add image if exists
            img_html = ""
            img_data = image_map.get(sid)
            if img_data:
                img_html = f'\n\n![{img_data["alt_text"]}]({img_data.get("local_path","")})\n\n'
        
            # Minimal transition
            final_sections.append(content + img_html)

        final_markdown = "\n\n".join(final_sections)

        # Metadata
        keywords = state.get("input_data", {}).get("keywords", [])
        primary_keyword = keywords[0] if keywords else ""


        prompt = f"""
        Create an SEO-optimized H1 title for the article "{title}".
        - Include the primary keyword: "{primary_keyword}"
        - Length: 60-70 chars
        - Commercial keywords: sales-oriented if applicable
        - Prefer adding the year 2026
        Return only the title string.
        """
        meta_title = await self.ai_client.send(prompt, step="title_generation")
        meta_description = f"Read our comprehensive guide on {title}"[:160]

        assembled = await self.assembler.assemble(
            title=title,
            sections=[sec for sec in sections_dict.values()],
            image_plan=state.get("images", [])
        )
        assembled["meta_title"] = meta_title
        assembled["meta_description"] = meta_description
        state["final_output"] = assembled
        return state

    # async def _step_3_assembly(self, state):
    #     input_data = state.get("input_data", {})
    #     title = input_data.get("title", "Untitled")
    #     outline = state.get("outline", [])
    #     sections = state.get("sections", {})
    #     images = state.get("images", [])

    #     image_map = {img['section_id']: img for img in images}

    #     final_sections = []
    #     for s in outline:
    #         sid = s.get("section_id")
    #         sec = sections.get(sid)
    #         if not sec:
    #             continue

    #         content = sec.get("generated_content", "")
    #         img = image_map.get(sid)

    #         if img:
    #             content += f'\n\n![{img["alt_text"]}]({img["local_path"]})\n\n'

    #         final_sections.append(content)

    #     final_markdown = "\n\n".join(final_sections)

    #     state["final_output"] = {
    #         "final_markdown": final_markdown,
    #         "meta_title": title[:70],
    #         "meta_description": f"Read our comprehensive guide about {title}"[:160]
    #     }

    #     return state

    async def _step_5_validation(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Validates the output against SEO rules."""
        from utils.seo_validator import SEOValidator
        validator = SEOValidator()
        
        final_out = state.get("final_output", {})
        content = final_out.get("final_markdown", "")
        
        if not content:
            logger.warning("Validation skipped: no final content found.")
            return state
            
        metadata = {
            **state.get("seo_meta", {}), 
            "images": state.get("images", []), 
            "domain": "yourdomain.com"
        }
        
        # validator.validate is sync, wrapping to avoid blocking event loop
        report = await asyncio.to_thread(validator.validate, content, metadata)
        state["seo_report"] = report
        return state

    # ---------------- UTILITIES ----------------
    
    def _sluggify(self, text: str) -> str:
        """Generates a clean slug from English or Arabic text."""
        clean = re.sub(r'[^\w\s-]', '', text).strip().lower()
        return re.sub(r'[-\s_]+', '-', clean)

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
            "workflow_state": state
        }