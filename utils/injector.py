from typing import List, Dict, Any
import math

class DataInjector:
    """
    Handles dynamic injection of keywords and URLs into the article workflow.
    """

    @staticmethod
    def distribute_urls_to_outline(
        outline: List[Dict[str, Any]], 
        urls: List[Dict[str, str]]  
    ) -> List[Dict[str, Any]]:
        """
        Distributes provided URLs to outline sections.
        Strategy: Round-robin assignment, skipping Introduction and Conclusion if possible
        unless strictly necessary (few sections).
        """
        if not urls:
            return outline

        # Identify candidate sections (exclude likely intro/conclusion if we have enough other sections)
        # Simple heuristic: Look for specific keywords in headings or IDs
        candidate_indices = []
        for i, section in enumerate(outline):
            heading = section.get("heading_text", "").lower()
            intent = section.get("section_intent", "").lower()
            
            is_intro = "intro" in heading or "introduction" in heading
            is_conclusion = "conclusion" in heading or "summary" in heading or "final thoughts" in heading
            is_faq = "faq" in heading or "frequently asked" in heading

            # Prefer body sections
            if not (is_intro or is_conclusion or is_faq):
                candidate_indices.append(i)

        # Fallback: if no body sections found (very short outline), use all sections
        if not candidate_indices:
            candidate_indices = list(range(len(outline)))

        # Assign URLs round-robin
        for idx, url_obj in enumerate(urls):
            target_idx = candidate_indices[idx % len(candidate_indices)]
            section = outline[target_idx]
            
            if "assigned_links" not in section:
                section["assigned_links"] = []
            
            # Add url if not already present (avoid duplicates if logic runs twice)
            if url_obj not in section["assigned_links"]:
                 section["assigned_links"].append(url_obj)

        return outline

    @staticmethod
    def format_prompt_variables(
        step_name: str, 
        current_state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Prepares the dictionary of variables needed for a specific prompt template.
        """
        input_data = current_state.get("input_data", {})
        
        # Base context available to all prompts
        tone_map = {
            "commercial": "Commercial",
            "informational": "Informational",
            "transactional": "Commercial",
            "comparative": "Informational"
        }
        tone_value = tone_map.get(current_state.get("seo_meta", {}).get("intent", "informational").lower(), "Informational")

        base_context = {
            "title": input_data.get("title", ""),
            "global_keywords": input_data.get("keywords", []),
            "tone": tone_value
}

        if step_name == "step1_outline_gen":
            # Context for Outline Generator
            return {
                "title": base_context["title"],
                "keywords": base_context["global_keywords"]
            }

        elif step_name == "step2_section_writer":
            # Context for Section Writer (requires specific section data)
            # This is usually called in a loop, so the caller must update 'section' variable
            return base_context

        elif step_name == "step3_assembly":
             # Context for Assembly
            return {
                "title": base_context["title"],
                "sections": current_state.get("outline", []) # Assumes outline now has 'generated_content' populated
            }

        return {}
