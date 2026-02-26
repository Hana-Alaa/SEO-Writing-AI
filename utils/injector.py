from typing import List, Dict, Any
import math

class DataInjector:
    """
    Handles dynamic injection of keywords and URLs into the article workflow.
    """

    @staticmethod
    def distribute_urls_to_outline(
        outline: List[Dict[str, Any]], 
        urls: List[Dict[str, Any]],
        strategy: str = "balanced"  # Options: "balanced", "conservative" (Guest Post)
    ) -> List[Dict[str, Any]]:
        """
        Distributes provided URLs to outline sections based on strategy.
        
        Strategies:
        - "balanced": Round-robin assignment, max 1 link per section. (Default/Internal Mode)
        - "conservative": Takes the FIRST url as 'Primary/Brand'. Assigns it ONCE to the best section 
                          (Intro > Conclusion > First Body). Other occurrences are converted to text mentions.
                          Secondary links are distributed normally but sparsely.
        """
        if not urls:
            return outline

        import logging
        logger = logging.getLogger(__name__)

        # Clean validation of section keys
        for section in outline:
            section.setdefault("assigned_links", [])
            section.setdefault("brand_mentions", [])

        if strategy == "conservative":
            # GUEST POST MODE
            primary_link = urls[0]
            secondary_links = urls[1:]
            
            # 1. Assign Primary Link ONCE
            # Priority: Intro -> Conclusion -> First Section
            target_section = None
            
            # Try finding Introduction
            for sec in outline:
                h = sec.get("heading_text", "").lower()
                if "intro" in h or "introduction" in h or "مقدمة" in h:
                    target_section = sec
                    break
            
            # Try finding Conclusion if no Intro
            if not target_section:
                for sec in outline:
                    h = sec.get("heading_text", "").lower()
                    if "conclusion" in h or "summary" in h or "خاتمة" in h:
                        target_section = sec
                        break
            
            # Fallback to first section
            if not target_section and outline:
                target_section = outline[0]
                
            if target_section:
                if primary_link not in target_section["assigned_links"]:
                    target_section["assigned_links"].append(primary_link)
                    logger.info(f"Conservative Strategy: Primary link '{primary_link.get('anchor_text')}' assigned ONLY to section '{target_section.get('heading_text')}'")
            
            # 2. Add Brand Mentions to other relevant sections
            # We add mentions to a few random body sections to keep the brand alive textually
            body_sections = [s for s in outline if s != target_section]
            for i, sec in enumerate(body_sections):
                # Add mention every ~2 sections
                if i % 2 == 0:
                    sec["brand_mentions"].append(primary_link.get("anchor_text"))
                    logger.info(f"Conservative Strategy: Added text mention for '{primary_link.get('anchor_text')}' to section '{sec.get('heading_text')}'")

            # 3. Distribute Secondary Links (Sparately)
            if secondary_links:
                # Filter sections that don't have the primary link to avoid clustering
                available_sections = [s for s in outline if not s["assigned_links"]]
                if not available_sections:
                     available_sections = outline
                
                for i, link in enumerate(secondary_links):
                    # Round robin on available sections
                    sec = available_sections[i % len(available_sections)]
                    sec["assigned_links"].append(link)
                    logger.info(f"Conservative Strategy: Secondary link '{link.get('anchor_text')}' assigned to '{sec.get('heading_text')}'")

        else:
            # BALANCED / INTERNAL MODE (Original Logic Enriched)
            candidate_indices = []
            for i, section in enumerate(outline):
                # Basic filter for "good" sections
                heading = section.get("heading_text", "").lower()
                is_intro = "intro" in heading or "introduction" in heading
                is_conclusion = "conclusion" in heading or "summary" in heading
                
                # We can link in intro/conclusion in balanced mode, but prefer body
                candidate_indices.append(i)

            if not candidate_indices:
                candidate_indices = list(range(len(outline)))

            for idx, url_obj in enumerate(urls):
                target_idx = candidate_indices[idx % len(candidate_indices)]
                section = outline[target_idx]
                
                # Avoid duplicates
                if url_obj not in section["assigned_links"]:
                    # Enforce max 2 links per section for balanced mode
                    if len(section["assigned_links"]) < 2:
                        section["assigned_links"].append(url_obj)
                        logger.info(f"Balanced Strategy: Link '{url_obj.get('anchor_text')}' assigned to '{section.get('heading_text')}'")
                    else:
                        # Try next section
                        next_idx = (target_idx + 1) % len(outline)
                        outline[next_idx]["assigned_links"].append(url_obj)
                        logger.info(f"Balanced Strategy: Link '{url_obj.get('anchor_text')}' pushed to '{outline[next_idx].get('heading_text')}' (overflow)")

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
            "brand_commercial": "Commercial",
            "informational": "Informational",
            "comparison": "Informational",
            "commercial": "Commercial",
            "transactional": "Commercial"
        }
        tone_value = tone_map.get(current_state.get("content_type", "informational").lower(), "Informational")

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
