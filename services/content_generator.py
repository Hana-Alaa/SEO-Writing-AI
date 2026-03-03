import json
import logging
import asyncio
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from jinja2 import Template, StrictUndefined
from utils.safe_json import recover_json

logger = logging.getLogger(__name__)

class ContentGeneratorError(Exception):
    """Base exception for content generation errors."""
    pass

class OutlineGenerator:
    def __init__(self, ai_client: Any, template_path: str = "prompts/templates/01_outline_generator.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    def _normalize_section(self, section: Dict[str, Any], idx: int, content_type: str, content_strategy: Dict[str, Any], area: Optional[str]):

        section.setdefault("section_id", f"section_{idx+1}")
        section.setdefault("heading_level", "H2")
        section.setdefault("heading_text", "Untitled Section")
        section.setdefault("section_type", "core")
        section.setdefault("section_intent", "Informational")
        section.setdefault("decision_layer", "Market Reality")
        section.setdefault("sales_intensity", "medium")
        section.setdefault("content_goal", "")
        section.setdefault("content_angle", "")
        section.setdefault("assigned_keywords", [])
        section.setdefault("content_scope", "")
        section.setdefault("forbidden_elements", [])
        section.setdefault("allowed_flow_steps", [])
        section.setdefault("image_plan", {
            "required": False,
            "image_type": "illustration",
            "alt_text": ""
        })
        section.setdefault("cta_allowed", False)
        section.setdefault("cta_type", "none")
        section.setdefault("cta_rules", {
            "placement": "none",
            "max_sentences": 0,
            "mandatory": False
        })
        section.setdefault("requires_table", False)
        section.setdefault("table_columns", [])
        section.setdefault("estimated_word_count_min", 300)
        section.setdefault("estimated_word_count_max", 600)
        section.setdefault("content_type", content_type)
        section.setdefault("content_strategy", content_strategy)
        section.setdefault("area", area)
        section.setdefault("requires_table", False)
        section.setdefault("table_type", "none")
        section.setdefault("requires_list", False)
        section.setdefault("list_type", "none")
        section.setdefault("cta_position", "none")
    
    def _validate_outline_schema(self, outline: List[Dict[str, Any]]) -> bool:
        required_keys = {
            "section_id",
            "heading_level",
            "heading_text",
            "section_intent"
        }

        for section in outline:
            if not required_keys.issubset(section.keys()):
                return False

        return True

    async def generate(
            self,
            title: str,
            keywords: List[str],
            urls: List[Dict[str, str]],
            article_language: str,
            intent: str,
            seo_intelligence: Dict[str, Any],
            content_type: str,
            content_strategy: Dict[str, Any],
            area: Optional[str],
            feedback: Optional[str] = None,
            mandatory_section_types: Optional[List[str]] = None
        ) -> Dict[str, Any]:

        current_year = str(datetime.now().year)
        prompt = self.template.render(
            title=title,
            keywords=keywords,
            urls=urls,
            article_language=article_language,
            intent=intent,
            seo_intelligence=seo_intelligence,
            content_type=content_type,
            content_strategy=content_strategy,
            area=area,
            feedback=feedback,
            mandatory_section_types = mandatory_section_types or [],
            current_year=current_year
        )

        logger.info("\n================ FINAL PROMPT (OutlineGenerator) ================\n")
        logger.info(prompt)
        logger.info("\n=============================================================\n")

        # response = await self.ai_client.send(prompt)
        response = await self.ai_client.send(prompt, step="outline")

        if not response:
            logger.error("Outline AI returned empty response")
            # return []
            return {
                "outline": [],
                "keyword_expansion": {}
            }


        data = recover_json(response)

        if not data or not isinstance(data, dict):
            raise ContentGeneratorError("Invalid structure returned by AI.")

        outline = data.get("outline")
        keyword_expansion = data.get("keyword_expansion", {})

        if not outline or not isinstance(outline, list):
            raise ContentGeneratorError("Invalid outline structure returned by AI.")


        if not self._validate_outline_schema(outline):
            raise ContentGeneratorError("Invalid outline schema returned by AI.")

        total_min_words = sum(
            section.get("estimated_word_count_min", 0)
            for section in outline
        )

        # if total_min_words < 1200:
        #     raise ContentGeneratorError(
        #         f"Total estimated word count too low: {total_min_words}"
        #     )

        if not outline or not isinstance(outline, list) or not self._validate_outline_schema(outline):
            raise ContentGeneratorError("Invalid outline schema returned by AI.")

        # Normalize sections
        # for idx, section in enumerate(outline):
        #     self._normalize_section(section, idx, content_type)

        if not isinstance(keyword_expansion, dict):
            keyword_expansion = {}

        keyword_expansion.setdefault("primary", keywords[0] if keywords else title)
        keyword_expansion.setdefault("core", keywords)
        keyword_expansion.setdefault("lsi", [])
        keyword_expansion.setdefault("semantic", [])
        keyword_expansion.setdefault("paa", [])


        return {
            "outline": outline,
            "keyword_expansion": keyword_expansion
        }

class SectionWriter:
    def __init__(self, ai_client: Any, template_path: str = "prompts/templates/02_section_writer.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    async def write(
        self,
        title: str,
        global_keywords: Dict[str, Any],
        section: Dict[str, Any],
        article_intent: str,
        seo_intelligence: Dict[str, Any],
        content_type: str,
        link_strategy: str,
        brand_url: str,
        brand_link_used: int,
        brand_link_allowed: bool,
        allow_external_links: bool,
        execution_plan: Dict[str, Any],
        area: str,
        used_phrases: List[str] = None,
        used_internal_links: List[str] = None,
        used_external_links: List[str] = None,
        section_index: int = 0,
        total_sections: int = 1
    ) -> Dict[str, Any]:

        brand_url = brand_url if brand_url not in ["None", ""] else None
        primary_keyword = section.get("primary_keyword") or global_keywords.get("primary", "")


        supporting_keywords = (
            global_keywords.get("lsi", []) +
            global_keywords.get("semantic", [])
        )

        article_language = section.get("article_language") or "ar"
        cta_allowed = section.get("cta_allowed", False)
        allowed_flow = section.get("allowed_flow_steps", [])

        # Flatten strategic intelligence for the template
        strategic_intelligence = seo_intelligence.get("strategic_analysis", {}).get("strategic_intelligence", {})
        
        # Ensure all expected fields are present to avoid StrictUndefined errors
        safe_seo = {
            "content_gaps": strategic_intelligence.get("content_gaps", []),
            "weaknesses_to_exploit": strategic_intelligence.get("weaknesses_to_exploit", []),
            "differentiation_strategy": strategic_intelligence.get("differentiation_strategy", []),
            "structural_patterns": strategic_intelligence.get("structural_patterns", [])
        }

        # Provide defaults for section fields
        safe_section = {
            "heading_level": section.get("heading_level", "H2"),
            "heading_text": section.get("heading_text", "Untitled Section"),
            "section_intent": section.get("section_intent", "Informational"),
            "content_scope": section.get("content_scope", ""),
            "allowed_flow_steps": allowed_flow,
            "forbidden_elements": section.get("forbidden_elements", []),
            "assigned_keywords": section.get("assigned_keywords", []),
            "assigned_links": section.get("assigned_links", []),
            "brand_mentions": section.get("brand_mentions", []),
            "estimated_word_count_min": section.get("estimated_word_count_min", 300),
            "estimated_word_count_max": section.get("estimated_word_count_max", 600),
            # "primary_keywords": primary_keywords,
            "article_language": article_language,
            "requires_table": section.get("requires_table", False),
            "table_type": section.get("table_type", "none"),
            "requires_list": section.get("requires_list", False),
            "list_type": section.get("list_type", "none"),
            "cta_position": section.get("cta_position", "none"),

            "article_intent": article_intent,
            "content_angle": section.get("content_angle", ""),
            "localized_angle": section.get("localized_angle", ""),
            "content_goal": section.get("content_goal", ""),
            "section_type": section.get("section_type", "core"),
            "decision_layer": section.get("decision_layer", "Market Reality"),
            "sales_intensity": section.get("sales_intensity", "medium"),
            "questions": section.get("questions", []),
            "assigned_links": section.get("assigned_links", []),
        }

        print("Assigned links:", safe_section["assigned_links"])

        current_year = str(datetime.now().year)

        prompt = self.template.render(
            title=title,
            global_keywords=global_keywords,
            supporting_keywords=supporting_keywords,
            primary_keyword=primary_keyword,
            article_language=article_language,
            article_intent=article_intent,
            section=safe_section,
            seo_intelligence=safe_seo,
            link_strategy=link_strategy,
            content_type=content_type,
            brand_url=brand_url,
            brand_link_used=brand_link_used,
            brand_link_allowed=brand_link_allowed,
            allow_external_links=allow_external_links,
            execution_plan=execution_plan,
            area=area,
            used_phrases=used_phrases or [],
            used_internal_links=used_internal_links or [],
            used_external_links=used_external_links or [], 
            section_index=section_index,
            total_sections=total_sections,
            is_first_section=(section_index == 0),
            is_last_section=(section_index == total_sections - 1)
        )

        logger.info("\n================ FINAL PROMPT (SectionWriter) ================\n")
        logger.info(prompt)
        logger.info("\n=============================================================\n")    

        print(f"\n=== Generating Section: {safe_section['heading_text']} ===")

        try:
            content = await self.ai_client.send(prompt, step="section")
            if not content:
                logger.warning(f"AI returned empty content for section {section.get('section_id')}")
                return {"content": "", "used_links": [], "brand_link_used": False}
            
            clean_content = content.strip().removeprefix("```").removesuffix("```").strip()
            
            # Detect used links
            found_links = re.findall(r'\[.*?\]\((https?://.*?)\)', clean_content)
            
            return {
                "content": clean_content,
                "used_links": found_links,
                "brand_link_used": any(brand_url in l for l in found_links) if brand_url else False
            }
        except Exception as e:
            logger.error(f"Error writing section {section.get('section_id', 'unknown')}: {e}")
            raise ContentGeneratorError(f"Section writing failed: {e}")

class Assembler:
    def __init__(self, ai_client: Any, template_path: str = "prompts/templates/04_article_assembler.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    async def assemble(
        self,
        title: str,
        article_language: str,
        sections: List[Dict[str, Any]]
    ) -> Dict[str, str]:

        article_language = article_language or "ar"

        final_parts = [f"# {title}"]

        for sec in sections:
            level = sec.get("heading_level", "H2")
            heading = sec.get("heading_text", "").strip()
            content = sec.get("generated_content", "").strip()

            # 1) Heading level safety
            if isinstance(level, str) and level.upper().startswith("H"):
                try:
                    level_num = int(level.upper().replace("H", ""))
                except ValueError:
                    level_num = 2
            else:
                level_num = 2

            level_num = max(2, min(level_num, 6))  

            # 2) Robust Mechanical Cleanup (Regex Based)
            cleanup_patterns = [
                r"\bIn this section,?\s*",
                r"\bIn this section we will\s*",
                r"\bNow,?\s*we will discuss\s*",
                r"\bNow we will discuss\s*"
            ]

            for pattern in cleanup_patterns:
                content = re.sub(pattern, "", content, flags=re.IGNORECASE)

            # Remove extra leading spaces after cleanup
            content = content.strip()

            final_parts.append(f"{'#' * level_num} {heading}")

            if sec.get("section_id"):
                final_parts.append(f"<!-- section_id: {sec['section_id']} -->")

            final_parts.append(content)

            # final_parts.append(f"{'#' * level_num} {heading}")
            # final_parts.append(content)

        final_markdown = "\n\n".join([p for p in final_parts if p])

        return {"final_markdown": final_markdown}

