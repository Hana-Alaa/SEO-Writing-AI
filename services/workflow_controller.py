import logging
import json
import os
from typing import Dict, Any, List
from jinja2 import Template

# Import our services and schemas
# Assuming these are available in the python path
from schemas.input_validator import ArticleInput, URLItem
from services.openrouter_client import OpenRouterClient
from services.image_generator import ImageGenerator
from utils.injector import DataInjector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WorkflowController:
    """
    Orchestrates the SEO Article Generation Workflow.
    Manages state, dependencies, and step execution order.
    """
    
    def __init__(self, work_dir: str = "."):
        self.work_dir = work_dir
        self.llm_client = OpenRouterClient() # Picks up env var key
        self.image_client = ImageGenerator(save_dir=os.path.join(work_dir, "output", "images"))
        self.injector = DataInjector()
        
        # Load Prompts (Caching them)
        self.prompts = self._load_prompts()

    def _load_prompts(self) -> Dict[str, Template]:
        """Loads Jinja2 templates from the file system."""
        templates = {}
        prompt_dir = os.path.join(self.work_dir, "prompts", "templates")
        
        files = {
            "step1_outline": "step1_outline_gen.txt",
            "step2_section": "step2_section_writer.txt", 
            "step3_assembly": "step3_assembly.txt",
            "step4_images": "image_prompt_gen.txt"
        }
        
        for key, filename in files.items():
            path = os.path.join(prompt_dir, filename)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    templates[key] = Template(f.read())
            except FileNotFoundError:
                logger.error(f"Prompt template not found: {path}")
                raise
        
        return templates

    def run_workflow(self, raw_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main execution entry point.
        """
        logger.info("Starting Workflow...")
        
        # --- Input Validation ---
        try:
            validated_input = ArticleInput(**raw_input)
            logger.info("Input validation passed.")
        except Exception as e:
            logger.error(f"Input validation failed: {e}")
            raise

        # Prepare Output Path (Sluggified Title)
        from urllib.parse import quote_plus
        slug = "".join(c if c.isalnum() else "_" for c in validated_input.title.lower()).strip("_")
        self.output_dir = os.path.join(self.work_dir, "output", slug)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Update image client path
        self.image_client.save_dir = os.path.join(self.output_dir, "images")
        os.makedirs(self.image_client.save_dir, exist_ok=True)

        # Initialize State
        state = {
            "input_data": validated_input.dict(),
            "seo_meta": {}, 
            "outline": [],
            "sections": [],
            "images": [],
            "final_output": {},
            "output_folder": self.output_dir
        }

        # --- Step 0: Competitive Analysis & Intent (Simulated) ---
        # Note: In a real system, this might call a rigorous analysis tool.
        # Here we rely on the prompt system to "Simulate" it as per instructions.
        # We can simulate this by asking the LLM to yield 'competitive_insights' first, 
        # but for now we'll bundle it into Step 1's reasoning or add a distinct step 
        # if the user specifically requested a separate output. 
        # User requested 'competitive_insights.json' in Step 0. Let's do a quick LLM call for that.
        state = self._step_0_analysis(state)
        state["final_output"]["meta_title"] = self._generate_article_title(state)

        # --- Step 1: Outline Generation ---
        state = self._step_1_outline(state)
        
        # --- Step 1.5: URL Distribution ---
        # Inject URLs into the outlined sections
        raw_urls = state["input_data"].get("urls", [])
        # Convert Pydantic URLItems to dicts if needed
        url_dicts = [u if isinstance(u, dict) else u.dict() for u in raw_urls]
        
        state["outline"] = self.injector.distribute_urls_to_outline(
            state["outline"], 
            url_dicts
        )

        # --- Step 2: Section Writing (Loop) ---
        state = self._step_2_write_sections(state)

        # --- Step 3: Assembly ---
        state = self._step_3_assembly(state)

        # --- Step 4: Image Generation ---
        state = self._step_4_generate_images(state)
        
        # --- Step 5: Validation ---
        state = self._step_5_validation(state)
        
        # --- Save Final State ---
        state_path = os.path.join(self.output_dir, "workflow_state.json")
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        logger.info(f"Workflow state saved to {state_path}")
        
        logger.info("Workflow completed successfully.")
        return state

    def _step_0_analysis(self, state: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("Running Step 0: Competitive Analysis...")
        # Simple prompt to get insights
        prompt = f"""
        Analyze the topic: "{state['input_data']['title']}" for SEO.
        Keywords: {state['input_data']['keywords']}
        
        Output JSON with:
        - intent (Commercial/Informational)
        - recommended_word_count
        - content_gaps (list of 3 unique angles)
        """
        
        resp = self.llm_client.generate_completion(
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        
        if resp:
            try:
                insights = json.loads(resp)
                state["seo_meta"].update(insights)
                logger.info(f"Insights generated: {insights.get('intent', 'Unknown')}")
            except json.JSONDecodeError:
                logger.warning("Failed to parse Step 0 JSON.")
        
        return state

    def _step_1_outline(self, state: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("Running Step 1: Outline Generation...")
        
        # Prepare Context
        context = self.injector.format_prompt_variables("step1_outline_gen", state)
        # Add Step 0 insights to context if available
        if "content_gaps" in state["seo_meta"]:
            context["competitor_gaps"] = state["seo_meta"]["content_gaps"]

        prompt_content = self.prompts["step1_outline"].render(**context)
        
        resp = self.llm_client.generate_completion(
            messages=[{"role": "user", "content": prompt_content}],
            response_format={"type": "json_object"}
        )
        
        if not resp:
            raise RuntimeError("Step 1 failed to generate response.")
            
        try:
            outline_data = json.loads(resp)
            # Handle if LLM wraps it in a key
            if isinstance(outline_data, dict) and "outline" in outline_data:
                state["outline"] = outline_data["outline"]
            elif isinstance(outline_data, list):
                state["outline"] = outline_data
            else:
                 # fallback/heuristic
                 state["outline"] = outline_data.get("sections", [])
            
            logger.info(f"Generated {len(state['outline'])} outline sections.")
        except json.JSONDecodeError:
            raise ValueError("Step 1 Response was not valid JSON.")
            
        return state

    def _step_2_write_sections(self, state: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("Running Step 2: Section Writing...")
        
        from jinja2 import Undefined

        for k, v in context.items():
            if isinstance(v, Undefined) or v is None:
                context[k] = ""

        
        sections_content = []
        base_context = self.injector.format_prompt_variables("step2_section_writer", state)
        
        for section in state["outline"]:
            sec_id = section.get("section_id")
            heading_text = section.get("heading_text", "")
            logger.info(f"Writing Section: {sec_id} - {heading_text}")
            
            # Update context for this specific section
            context = base_context.copy()
            context["section"] = section

            # ضمان أن كل المتغيرات المطلوبة موجودة
            context.setdefault("outline", state.get("outline", []))
            context.setdefault("seo_meta", state.get("seo_meta", {}))
            context.setdefault("competitor_gaps", state.get("seo_meta", {}).get("content_gaps", []))
            context.setdefault("input_data", state.get("input_data", {}))

            # تحويل أي Undefined في context إلى empty string لتجنب خطأ JSON
            for k, v in context.items():
                if isinstance(v, Undefined):
                    context[k] = "" 

            try:
                prompt_content = self.prompts["step2_section"].render(**context)
            except Exception as e:
                logger.error(f"Error rendering template for section {sec_id}: {e}")
                continue
            
            content_resp = self.llm_client.generate_completion(
                messages=[{"role": "user", "content": prompt_content}]
            )
            
            if content_resp:
                section["generated_content"] = content_resp
                sections_content.append(section)
            else:
                logger.warning(f"Failed to generate content for {sec_id}")
        
        state["outline"] = sections_content  # store back
        state["sections"] = sections_content  # separate content from structure
        return state


    def _step_3_assembly(self, state: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("Running Step 3: Assembly...")
        
        context = self.injector.format_prompt_variables("step3_assembly", state)
        prompt_content = self.prompts["step3_assembly"].render(**context)
        
        resp = self.llm_client.generate_completion(
            messages=[{"role": "user", "content": prompt_content}],
            response_format={"type": "json_object"}
        )
        
        if resp:
            try:
                final_data = json.loads(resp)
                state["final_output"] = final_data
                
                # Save to file
                output_path = os.path.join(self.output_dir, "article_final.md")
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(final_data.get("final_markdown", ""))
                logger.info(f"Article saved to {output_path}")
                
            except json.JSONDecodeError:
                logger.error("Step 3 JSON parse error.")
        
        return state

    def _step_4_generate_images(self, state: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("Running Step 4: Image Generation...")
        
        # 1. Generate Prompts
        context = {
            "title": state["input_data"]["title"],
            "keywords": state["input_data"]["keywords"],
            "outline": state["outline"] # contains contents too potentially
        }
        prompt_content = self.prompts["step4_images"].render(**context)
        
        resp = self.llm_client.generate_completion(
            messages=[{"role": "user", "content": prompt_content}],
            response_format={"type": "json_object"}
        )
        
        if resp:
            try:
                data = json.loads(resp)
                image_prompts = data if isinstance(data, list) else data.get("images", [])
                if not image_prompts:
                    logger.warning("No valid image prompts found in LLM JSON response.")
            except Exception as e:
                logger.error(f"Failed to parse image prompts JSON: {e}")
        else:
            logger.warning("No response received from LLM for image prompts.")
        
        # 2. Fetch Images (Pollinations)
        if image_prompts:
            # Get primary keyword for validation
            primary_kw = state["input_data"]["keywords"][0] if state["input_data"]["keywords"] else None
            
            # Generate URLs
            generated_images = self.image_client.generate_images(image_prompts, primary_keyword=primary_kw)
            
            # 3. Download & Create Responsive Versions
            processed_images = []
            for img_obj in generated_images:
                prompt = img_obj.get("prompt", "")
                
                # Download
                local_path = self.image_client.download_image(prompt)
                
                if local_path:
                    # Update object with local path
                    img_obj["local_path"] = local_path
                    img_obj["url"] = local_path # Point to local file for now
                    
                    # Generate responsive sizes
                    self.image_client.save_responsive_versions(local_path)
                    
                    processed_images.append(img_obj)
                else:
                    logger.warning(f"Failed to download image for section {img_obj.get('section_id')}")
            
            state["images"] = processed_images
            logger.info(f"Generated and downloaded {len(processed_images)} images.")
        
        return state

    def _step_5_validation(self, state: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("Running Step 5: SEO Validation...")
        
        from utils.seo_validator import SEOValidator
        validator = SEOValidator()
        
        final_content = state["final_output"].get("final_markdown", "")
        # Construct metadata for validator (enriched)
        metadata = {
            "main_keyword": state["input_data"]["keywords"][0] if state["input_data"]["keywords"] else "",
            "secondary_keywords": state["input_data"]["keywords"][1:] if len(state["input_data"]["keywords"]) > 1 else [],
            "meta_title": state["final_output"].get("meta_title", ""),
            "meta_description": state["final_output"].get("meta_description", "")
        }
        
        report = validator.validate(final_content, metadata)
        state["validation_report"] = report
        
        if not report["passed"]:
            logger.warning(f"SEO Validation Failed: {report['errors']}")
            # In Phase 6.2 (Iteration Loop), we would handle retries here.
        else:
            logger.info("SEO Validation Passed!")
            
        return state
