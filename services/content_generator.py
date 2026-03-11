import json
import logging
import asyncio
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from jinja2 import Template, StrictUndefined
from utils.json_utils import recover_json

logger = logging.getLogger(__name__)

class ContentGeneratorError(Exception):
    """Base exception for content generation errors."""
    pass


def _enforce_paragraph_word_limit(content: str, max_words: int = 40) -> str:
    """
    Post-processing function that enforces a maximum word count per paragraph.
    Paragraphs exceeding max_words are split at sentence boundaries (Arabic & English).
    Skips table rows, headings, list items, code blocks, and HTML comments.
    """
    if not content:
        return content

    lines = content.split("\n")
    in_code_block = False
    in_table = False
    result_lines = []

    for line in lines:
        stripped = line.strip()
        
        # Track code blocks — skip enforcement inside them.
        if stripped.startswith("```"):
            in_code_block = not in_code_block
            result_lines.append(line)
            continue
        if in_code_block:
            result_lines.append(line)
            continue
        
        # Track tables — rows usually have pipes or alignment markers
        # Robust check: starts with | OR has at least 2 pipes OR is a separator row
        is_table_row = stripped.startswith("|") or stripped.count("|") >= 2 or (stripped.startswith("-") and "|" in stripped)
        
        if is_table_row:
            in_table = True
            result_lines.append(line)
            continue
        else:
            # If we were in a table and hit a non-empty line that isn't a table row, 
            # it might be a broken table or just the end of the table.
            if in_table and stripped:
                in_table = False 
            elif not stripped:
                in_table = False

        # Skip headings, list items, HTML comments, blank lines
        if (
            not stripped  # blank line
            or stripped.startswith("#")  # heading
            or stripped.startswith("-") or stripped.startswith("*") or stripped.startswith("+") # lists
            or stripped.startswith(">")
            or stripped.startswith("<!")
            or stripped.startswith("[")  # link-only lines (CTAs)
        ):
            result_lines.append(line)
            continue

        # Count words (works for Arabic and English)
        words = stripped.split()
        if len(words) <= max_words:
            result_lines.append(line)
            continue

        # --- Paragraph too long: split at sentence boundaries ---
        # Sentence boundaries: period, question mark, exclamation for English/Arabic,
        # Arabic period '\u06D4', Arabic comma '\u060C'.
        sentences = re.split(r'(?<=[.!?\u06D4])\s+', stripped)
        
        current_para = []
        current_count = 0

        for sentence in sentences:
            s_words = sentence.split()
            if current_count + len(s_words) > max_words and current_para:
                # Emit current paragraph and start a new one
                result_lines.append(" ".join(current_para))
                result_lines.append("")  # blank line between paragraphs
                current_para = s_words
                current_count = len(s_words)
            else:
                current_para.extend(s_words)
                current_count += len(s_words)
        
        if current_para:
            result_lines.append(" ".join(current_para))

    return "\n".join(result_lines)


class OutlineGenerator:
    def __init__(self, ai_client: Any):
        self.ai_client = ai_client
        from pathlib import Path
        
        base_outline = Path("prompts/templates/01_outline_generator_base.txt").read_text(encoding="utf-8")
        commercial_outline = Path("prompts/templates/01_outline_generator_brand_commercial.txt").read_text(encoding="utf-8")
        informational_outline = Path("prompts/templates/01_outline_generator_informational.txt").read_text(encoding="utf-8")
        comparison_outline = Path("prompts/templates/01_outline_generator_comparison.txt").read_text(encoding="utf-8")
        
        self.templates = {
            "brand_commercial": Template(base_outline + "\n\n" + commercial_outline, undefined=StrictUndefined),
            "informational": Template(base_outline + "\n\n" + informational_outline, undefined=StrictUndefined),
            "comparison": Template(base_outline + "\n\n" + comparison_outline, undefined=StrictUndefined),
        }

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
            brand_context: str,
            area: Optional[str],
            area_neighborhoods: Optional[List[str]] = None,
            feedback: Optional[str] = None,
            mandatory_section_types: Optional[List[str]] = None,
            prohibited_competitors: Optional[List[str]] = None
        ) -> Dict[str, Any]:

        current_year = str(datetime.now().year)

        template = self.templates.get(
            content_type,
            self.templates["informational"]
        )

        prompt = template.render(
            title=title,
            keywords=keywords,
            urls=urls,
            article_language=article_language,
            intent=intent,
            seo_intelligence=seo_intelligence,
            content_type=content_type,
            content_strategy=content_strategy,
            brand_context=brand_context,
            area=area,
            area_neighborhoods=area_neighborhoods or [],
            feedback=feedback,
            mandatory_section_types=mandatory_section_types or [],
            current_year=current_year,
            prohibited_competitors=prohibited_competitors or []
        )

        logger.info("\n================ FINAL PROMPT (OutlineGenerator) ================\n")
        logger.info(prompt)
        logger.info("\n=============================================================\n")

        # response = await self.ai_client.send(prompt)
        res = await self.ai_client.send(prompt, step="outline")
        response = res["content"]
        metadata = res["metadata"]

        if not response:
            logger.error("Outline AI returned empty response")
            # return []
            return {
                "outline": [],
                "keyword_expansion": {},
                "metadata": metadata
            }


        data = recover_json(response)

        if not data or not isinstance(data, dict):
            logger.error(f"CRITICAL: Failed to parse AI response as JSON for outline. Step: outline. Raw response:\n{response}")
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

        keyword_expansion["primary"] = keywords[0] if keywords else title
        keyword_expansion.setdefault("core", keywords)
        keyword_expansion.setdefault("lsi", [])
        keyword_expansion.setdefault("semantic", [])
        keyword_expansion.setdefault("paa", [])


        return {
            "outline": outline,
            "keyword_expansion": keyword_expansion,
            "metadata": metadata
        }

class SectionWriter:
    def __init__(self, ai_client: Any):
        self.ai_client = ai_client
        from pathlib import Path
        
        base_writer = Path("prompts/templates/02_section_writer_base.txt").read_text(encoding="utf-8")
        commercial_writer = Path("prompts/templates/02_section_writer_brand_commercial.txt").read_text(encoding="utf-8")
        informational_writer = Path("prompts/templates/02_section_writer_informational.txt").read_text(encoding="utf-8")
        comparison_writer = Path("prompts/templates/02_section_writer_comparison.txt").read_text(encoding="utf-8")
        
        self.templates = {
            "brand_commercial": Template(base_writer + "\n\n" + commercial_writer, undefined=StrictUndefined),
            "informational": Template(base_writer + "\n\n" + informational_writer, undefined=StrictUndefined),
            "comparison": Template(base_writer + "\n\n" + comparison_writer, undefined=StrictUndefined),
        }

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
        brand_name: str = "",
        used_phrases: List[str] = None,
        used_internal_links: List[str] = None,
        used_external_links: List[str] = None,
        section_index: int = 0,
        total_sections: int = 1,
        brand_context: str = "",
        section_source_text: str = "",
        external_sources: List[Dict[str, str]] = None,
        workflow_logger: Optional[Any] = None,
        prohibited_competitors: List[str] = None
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

        template = self.templates.get(
            content_type,
            self.templates["informational"]
        )

        prompt = template.render(
            title=title,
            global_keywords=global_keywords,
            supporting_keywords=supporting_keywords,
            primary_keyword=primary_keyword,
            article_language=article_language,
            article_intent=article_intent,
            content_type=content_type,
            section=safe_section,
            seo_intelligence=safe_seo,
            link_strategy=link_strategy,
            brand_url=brand_url,
            brand_link_used=brand_link_used,
            brand_link_allowed=brand_link_allowed,
            allow_external_links=allow_external_links,
            execution_plan=execution_plan,
            area=area,
            used_phrases=used_phrases or [],
            used_internal_links=used_internal_links or [],
            used_external_links=used_external_links or [], 
            brand_name=brand_name,
            section_index=section_index,
            total_sections=total_sections,
            brand_context=brand_context,
            section_source_text=section_source_text,
            external_sources=external_sources or [],
            is_first_section=(section_index == 0),
            is_last_section=(section_index == total_sections - 1),
            prohibited_competitors=prohibited_competitors or [],
            current_year=current_year
        )

        logger.info("\n================ FINAL PROMPT (SectionWriter) ================\n")
        logger.info(prompt)
        logger.info("\n=============================================================\n")    

        print(f"\n=== Generating Section: {safe_section['heading_text']} ===")

        try:
            res = await self.ai_client.send(prompt, step=f"section_{section_index+1}")
            response_content = res["content"]
            metadata = res["metadata"]

            if workflow_logger:
                workflow_logger.log_ai_call(
                    step_name=f"section_{section_index+1}_{section.get('heading_text', 'No Heading')}",
                    prompt=prompt,
                    response=response_content,
                    tokens=metadata.get("tokens", {}),
                    duration=metadata.get("duration", 0)
                )

            heading_text = safe_section['heading_text'] # Define heading_text for error logging

            if not response_content:
                logger.error(f"SectionWriter returned empty response for: {heading_text}")
                return {
                    "content": "",
                    "used_links": [],
                    "brand_link_used": False,
                    "metadata": metadata
                }

            data = recover_json(response_content)
            if not data:
                # Fallback to pure string if JSON recovery fails
                return {
                    "content": response_content,
                    "used_links": [],
                    "brand_link_used": False,
                    "metadata": metadata
                }

            return {
                "content": data.get("content", ""),
                "used_links": data.get("used_links", []),
                "brand_link_used": data.get("brand_link_used", False),
                "metadata": metadata
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

            # Robust Mechanical Cleanup (Regex Based)
            cleanup_patterns = [
                r"\bIn this section,?\s*",
                r"\bIn this section we will\s*",
                r"\bNow,?\s*we will discuss\s*",
                r"\bNow we will discuss\s*"
            ]

            for pattern in cleanup_patterns:
                content = re.sub(pattern, "", content, flags=re.IGNORECASE)

            # 3) Heading De-duplication (CRITICAL)
            # If the content starts with the same heading (e.g. "## FAQ"), remove that line.
            content = content.strip()
            content_lines = content.split("\n")
            if content_lines:
                first_line = content_lines[0].strip()
                # Remove markdown hashes and common prefix/suffixes for comparison
                clean_first_line = re.sub(r"^#+\s*", "", first_line).strip().lower()
                clean_heading = heading.lower()
                
                # Exact match or starts-with match (allowing for minor AI variations)
                if clean_first_line == clean_heading or clean_first_line.startswith(clean_heading):
                    logger.info(f"[Assembler] Removing duplicate heading from content: '{first_line}'")
                    content = "\n".join(content_lines[1:]).strip()

            # Skip adding the heading for the VERY FIRST section unconditionally.
            # This ensures we don't have H1 and H2 stacked at the start.
            # Also skip if it's explicitly named 'Introduction'.
            is_first_sec = (sections.index(sec) == 0)
            skip_heading = is_first_sec or heading.strip().lower() in ["introduction", "مقدمة", "مقدمه"]
            
            if not skip_heading:
                final_parts.append(f"{'#' * level_num} {heading}")

            if sec.get("section_id"):
                final_parts.append(f"<!-- section_id: {sec['section_id']} -->")

            final_parts.append(content)

            # final_parts.append(f"{'#' * level_num} {heading}")
            # final_parts.append(content)

        final_markdown = "\n\n".join([p for p in final_parts if p])

        return {"final_markdown": final_markdown}

