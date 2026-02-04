"""
Phase 5 – Orchestration Layer
- Does NOT generate content
- Does NOT process images
- Does NOT apply business rules
- Only coordinates Phase 3 / Phase 4 services
"""

import logging
import os
from typing import Dict, Any
from services.image_generator import ImageGenerator
from services.image_service import PollinationsImageService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Executor:
    """Step-by-step executor for modular workflow."""
    def run_step(self, step_name: str, func, state: Dict[str, Any]) -> Dict[str, Any]:
        try:
            new_state = func(state)
            return {"status": "success", "data": new_state}
        except Exception as e:
            logger.error(f"{step_name} failed", exc_info=True)
            return {"status": "error", "error": str(e), "data": state}


class WorkflowController:
    """Phase 5: Orchestration layer only. No business logic or content/image generation here."""

    def __init__(self, work_dir: str = "."):
        self.work_dir = work_dir
        self.executor = Executor()

        if os.getenv("POLLINATIONS_API_KEY"):
            self.image_client = PollinationsImageService(save_dir=os.path.join(work_dir, "output", "images"))
        else:
            self.image_client = ImageGenerator(save_dir=os.path.join(work_dir, "output", "images"))

    def run_workflow(self, state: Dict[str, Any]) -> Dict[str, Any]:
        steps = [           
            ("step_0_analysis", self._step_0_analysis),
            ("step_1_outline", self._step_1_outline),
            ("step_2_write_sections", self._step_2_write_sections),
            ("step_3_assembly", self._step_3_assembly),
            ("step_4_generate_image_prompts", self._step_4_generate_image_prompts),
            ("step_4_5_image_download", self._step_4_5_download_images),
            ("step_5_validation", self._step_5_validation) 
            ]

        for name, step in steps:
            result = self.executor.run_step(name, step, state)
            if result["status"] == "error":
                return self._assemble_final_output(result["data"])
            state = result["data"]

        return self._assemble_final_output(state)

    # ----------------- STEP PLACEHOLDERS -----------------
    # Phase 5 orchestration only, actual execution happens in Phase 3/4 services

    def _step_0_analysis(self, state: Dict[str, Any]) -> Dict[str, Any]:
        return state

    def _step_1_outline(self, state: Dict[str, Any]) -> Dict[str, Any]:
        return state

    def _step_2_write_sections(self, state: Dict[str, Any]) -> Dict[str, Any]:
        return state

    def _step_3_assembly(self, state: Dict[str, Any]) -> Dict[str, Any]:
        # Phase 5 orchestrates only – no content generation
        return state

    def _step_4_generate_image_prompts(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Orchestration: call Phase 4 ImageGenerator to produce image prompts only."""
        image_prompts = self.image_client.generate_image_prompts_only(
            outline = state.get("outline", []),
            seo_meta = state.get("seo_meta", {})
        )

        state["image_prompts"] = image_prompts
        return state

    def _step_4_5_download_images(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Orchestration: call Phase 4 ImageGenerator to download/process images."""
        if not state.get("image_prompts"):
            logger.warning("No image prompts to download.")
            return state
        processed_images = self.image_client.download_and_process_images(state["image_prompts"])
        state["images"] = processed_images
        return state

    def _step_5_validation(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Orchestration: call Phase 6 SEOValidator service."""
        from utils.seo_validator import SEOValidator
        validator = SEOValidator()
        
        # Prepare metadata for validator
        metadata = {
            "main_keyword": state["input_data"].get("title", ""), 
            "secondary_keywords": state["input_data"].get("keywords", []),
            "meta_title": state.get("final_output", {}).get("meta_title", ""),
            "meta_description": state.get("final_output", {}).get("meta_description", ""),
            "images": state.get("images", []),
            "domain": "yourdomain.com" # Placeholder for internal link check
        }
        
        # In this orchestration-only mode, we assume final_markdown is already gathered in state
        final_markdown = state.get("final_output", {}).get("final_markdown", "")
        
        if final_markdown:
            report = validator.validate(final_markdown, metadata)
            state["validation_report"] = report
            logger.info(f"SEO Validation Score: {report['score']}")
        else:
            logger.warning("No final markdown found to validate.")
            
        return state

    # ----------------- FINAL OUTPUT ASSEMBLY -----------------

    def _assemble_final_output(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Combine all final results into a single dict ready for validation."""
        final_markdown = state.get("final_output", {}).get("final_markdown", "")
        meta_title = state.get("final_output", {}).get("meta_title", state["input_data"].get("title", ""))
        meta_description = state.get("final_output", {}).get("meta_description", "")

        images_metadata = []
        for img in state.get("images", []):
            images_metadata.append({
                "section_id": img.get("section_id"),
                "image_type": img.get("image_type"),
                "alt_text": img.get("alt_text"),
                "local_path": img.get("local_path"),
                "url": img.get("url")
            })

        return {
            "final_markdown": final_markdown,
            "meta_title": meta_title,
            "meta_description": meta_description,
            "images": images_metadata,
            "workflow_state": state
        }
