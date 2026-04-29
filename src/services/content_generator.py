import json
import logging
import asyncio
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, Template, StrictUndefined
from src.utils.json_utils import recover_json

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
        self.env = Environment(
            loader=FileSystemLoader("assets/prompts/templates"),
            undefined=StrictUndefined
        )
        
        self.templates = {
            "brand_commercial": "01_outline_generator_brand_commercial.txt",
            "informational": "01_outline_generator_informational.txt",
            "comparison": "01_outline_generator_comparison.txt",
            "review_mode": "01_outline_generator_review_mode.txt",
        }
        # Keep the legacy review-mode prompt untouched and move active
        # heading-only iteration to a dedicated template file.
        self.heading_only_template = "01_outline_generator_heading_only_v2.txt"

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
        section.setdefault("cta_eligible", False)
        section.setdefault("cta_type", "none")
        # Legacy compatibility for older templates/tools that still expect these fields.
        section.setdefault("cta_allowed", section.get("cta_eligible", False))
        section.setdefault("cta_rules", {
            "placement": section.get("cta_position", "none"),
            "max_sentences": 1 if section.get("cta_eligible", False) else 0,
            "mandatory": section.get("cta_type", "none") == "strong"
        })
        section.setdefault("requires_table", False)
        section.setdefault("table_columns", [])
        section.setdefault("estimated_word_count_min", 300)
        section.setdefault("estimated_word_count_max", 600)

        # --- New Decision-Complete Writing Brief Fields ---
        section.setdefault("section_promise", "")
        section.setdefault("reader_takeaway", "")
        section.setdefault("must_include_details", [])
        section.setdefault("must_not_repeat", [])
        section.setdefault("practical_decision_value", "")
        section.setdefault("evidence_expectation", "")
        section.setdefault("value_density_target", "high")
        section.setdefault("allowed_generality_level", "low")
        section.setdefault("subheading_policy", "direct_body")
        section.setdefault("subheadings", [])

        section.setdefault("content_type", content_type)
        section.setdefault("content_strategy", content_strategy)
        section.setdefault("area", area)
        section.setdefault("requires_table", False)
        section.setdefault("table_type", "none")
        section.setdefault("requires_list", False)
        section.setdefault("list_type", "none")
        section.setdefault("cta_position", "none")
        # --- Primary keyword enforcement for strategic sections ---
        # If this section is explicitly an introduction (or is the first section),
        # mark it as requiring the exact primary keyword so downstream
        # validators and writers will enforce PK presence in the intro.
        sec_type = (section.get("section_type") or "").lower()
        if sec_type == "introduction" or idx == 0:
            section.setdefault("requires_primary_keyword", True)
        else:
            section.setdefault("requires_primary_keyword", section.get("requires_primary_keyword", False))

    def _validate_outline_schema(self, outline: List[Dict[str, Any]], heading_only_mode: bool = False) -> bool:
        if heading_only_mode:
            # Leaner requirements for structural review
            required_keys = {
                "section_id",
                "heading_level",
                "heading_text",
                "section_type",
                "section_intent"
            }
        else:
            # Full writing requirements
            required_keys = {
                "section_id",
                "heading_level",
                "heading_text",
                "section_intent",
                "section_promise",
                "reader_takeaway",
                "must_include_details",
                "must_not_repeat",
                "practical_decision_value",
                "evidence_expectation",
                "value_density_target",
                "allowed_generality_level",
                "subheading_policy"
            }

        for section in outline:
            if not required_keys.issubset(section.keys()):
                missing = required_keys - set(section.keys())
                logger.error(f"Section {section.get('section_id')} missing keys: {missing}")
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
            prohibited_competitors: Optional[List[str]] = None,
            # Advanced Customization
            article_size: str = "1000",
            include_conclusion: bool = True,
            include_faq: bool = True,
            include_tables: bool = True,
            include_bullet_lists: bool = True,
            include_comparison_blocks: bool = True,
            bold_key_terms: bool = True,
            secondary_keywords: List[str] = None,
            competitor_count: int = 5,
            external_resources: List[Dict[str, str]] = None,
            style_blueprint: Dict[str, Any] = None,
            brand_name: str = "",
            brand_url: str = "",
            brand_advantages: List[str] = None,
            writing_blueprint: str = "",
            market_angle: str = "",
            heading_only_mode: bool = False,
            head_entity: str = "",
            entity_phrase: str = "",
            service_phrase: str = ""
        ) -> Dict[str, Any]:



        current_year = str(datetime.now().year)

        # Heading-Only Mode Isolation: Use specialized lightweight template
        if heading_only_mode:
            template_name = self.heading_only_template
        else:
            template_name = self.templates.get(
                content_type,
                self.templates["informational"]
            )
        template = self.env.get_template(template_name)


        final_blueprint = {
            "tonal_dna": {"persona": "Professional", "audience_level": "General", "forbidden_jargon": [], "sentence_rhythm": "Balanced"},
            "formatting_blueprint": {"bolding_frequency": "Standard"},
            "cta_strategy": {"density": "Balanced", "total_ideal_count": 3, "wording_patterns": []},
            "structural_skeleton": []
        }
        if style_blueprint:
            for k, v in style_blueprint.items():
                if isinstance(v, dict) and k in final_blueprint and isinstance(final_blueprint[k], dict):
                    final_blueprint[k].update(v)
                else:
                    final_blueprint[k] = v

        primary_keyword = keywords[0] if keywords else title
        prompt = template.render(
            title=title,
            keywords=keywords,
            primary_keyword=primary_keyword,
            urls=urls,
            article_language=article_language,
            intent=intent,
            seo_intelligence=seo_intelligence,
            content_type=content_type,
            content_strategy=content_strategy,
            brand_context=brand_context,
            brand_name=brand_name,
            brand_url=brand_url,
            area=area,
            area_neighborhoods=area_neighborhoods or [],
            feedback=feedback,
            mandatory_section_types=mandatory_section_types or [],
            market_angle=market_angle,
            current_year=current_year,
            prohibited_competitors=prohibited_competitors or [],
            # Pass advanced settings to template
            article_size=article_size,
            include_conclusion=include_conclusion,
            include_faq=include_faq,
            include_tables=include_tables,
            include_bullet_lists=include_bullet_lists,
            include_comparison_blocks=include_comparison_blocks,
            bold_key_terms=bold_key_terms,
            secondary_keywords=secondary_keywords or [],
            competitor_count=competitor_count,
            external_resources=external_resources or [],
            style_blueprint=final_blueprint,
            brand_advantages=brand_advantages or [],
            writing_blueprint=writing_blueprint or "",
            heading_only_mode=heading_only_mode,
            head_entity=head_entity,
            entity_phrase=entity_phrase,
            service_phrase=service_phrase
        )
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

        if not data:
            logger.error(f"CRITICAL: Failed to parse AI response as JSON for outline. Step: outline. Raw response (first 200 chars):\n{response[:200]}")
            raise ContentGeneratorError(f"AI returned invalid JSON structure. Starting with: {response[:50]}")

        # AUTO-RECOVERY: If AI returns a list, assume it's the outline itself.
        if isinstance(data, list):
            logger.warning("AI returned a list instead of a dictionary. Auto-wrapping as 'outline'.")
            data = {"outline": data}

        if not isinstance(data, dict):
            logger.error(f"CRITICAL: AI returned {type(data)} instead of dict. Raw response:\n{response}")
            raise ContentGeneratorError("Invalid structure returned by AI (expected dictionary).")

        outline = data.get("outline")
        keyword_expansion = data.get("keyword_expansion", {})
        semantic_entities = data.get("semantic_entities", [])
        semantic_concepts = data.get("semantic_concepts", [])
        intent_clusters = data.get("intent_clusters", [])

        if not outline or not isinstance(outline, list):
            logger.error(f"Outline missing or invalid in data: {list(data.keys())}")
            raise ContentGeneratorError("Invalid outline structure returned by AI (missing or non-list 'outline' field).")


        if not self._validate_outline_schema(outline, heading_only_mode=heading_only_mode):
            logger.error("Outline schema validation failed.")
            raise ContentGeneratorError("Invalid outline schema returned by AI (missing required section keys).")
        
        # AUTO-NORMALIZE: Ensure all sections have necessary fields for the orchestrator, even in review mode
        for idx, section in enumerate(outline):
            self._normalize_section(section, idx, content_type, content_strategy, area)


        total_min_words = sum(
            section.get("estimated_word_count_min", 0)
            for section in outline
        )

        # if total_min_words < 1200:
        #     raise ContentGeneratorError(
        #         f"Total estimated word count too low: {total_min_words}"
        #     )

        if not outline or not isinstance(outline, list) or not self._validate_outline_schema(outline, heading_only_mode=heading_only_mode):
            raise ContentGeneratorError("Invalid outline schema returned by AI.")

        # Normalize sections so defaults and strategic flags (e.g. requires_primary_keyword)
        # are applied consistently before downstream consumers use the outline.
        for idx, section in enumerate(outline):
            try:
                self._normalize_section(section, idx, content_type, content_strategy, area)
            except Exception:
                # Be tolerant: if normalization fails for a section, continue and log.
                logger.exception(f"Failed to normalize section at index {idx}")

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
            "semantic_entities": semantic_entities,
            "semantic_concepts": semantic_concepts,
            "intent_clusters": intent_clusters,
            "metadata": metadata
        }

class SectionWriter:
    def __init__(self, ai_client: Any):
        self.ai_client = ai_client
        self.env = Environment(
            loader=FileSystemLoader("assets/prompts/templates"),
            undefined=StrictUndefined
        )

        self.templates = {
            "brand_commercial": "02_section_writer_brand_commercial.txt",
            "informational": "02_section_writer_informational.txt",
            "comparison": "02_section_writer_comparison.txt",
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
        brand_link_used: bool,
        brand_link_allowed: bool,
        allow_external_links: bool,
        execution_plan: Dict[str, Any],
        area: str,
        workflow_mode: str = "core",
        brand_name: str = "",
        used_phrases: List[str] = None,
        used_topics: List[str] = None,
        used_anchors: List[str] = None,
        previous_content_summary: str = "",
        used_internal_links: List[str] = None,
        used_external_links: List[str] = None,
        used_claims: List[str] = None,
        section_index: int = 0,
        total_sections: int = 1,
        brand_context: str = "",
        section_source_text: str = "",
        external_sources: List[Dict[str, str]] = None,
        workflow_logger: Optional[Any] = None,
        prohibited_competitors: List[str] = None,
        previous_section_text: str = "",
        # Advanced Customization
        tone: Optional[str] = None,
        pov: Optional[str] = None,
        brand_voice_description: Optional[str] = None,
        brand_voice_guidelines: Optional[str] = None,
        brand_voice_examples: Optional[str] = None,
        custom_keyword_density: Optional[float] = None,
        bold_key_terms: bool = True,
        introduction_text: str = "",
        full_outline: List[Dict[str, Any]] = None,
        external_resources: List[Dict[str, Any]] = None,
        requires_primary_keyword: bool = False,
        style_blueprint: Dict[str, Any] = None,
        ctas_placed: int = 0,
        cta_type: str = "none",
        tables_placed: int = 0,
        serp_data: Dict[str, Any] = None,
        area_neighborhoods: List[str] = None,
        global_keyword_count: int = 0,
        brand_mentions_count: int = 0,
        draft_to_fix: str = None,
        brand_advantages: List[str] = None,
        writing_blueprint: str = "",
        market_angle: str = ""
    ) -> Dict[str, Any]:


        brand_url = brand_url if brand_url not in ["None", ""] else None
        primary_keyword = section.get("primary_keyword") or global_keywords.get("primary", "")


        supporting_keywords = (
            global_keywords.get("lsi", []) +
            global_keywords.get("semantic", [])
        )

        article_language = section.get("article_language") or "ar"
        allowed_flow = section.get("allowed_flow_steps", [])

        # Flatten strategic intelligence for the template
        market_insights = seo_intelligence.get("market_analysis", {}).get("market_insights", {})
        
        # Ensure all expected fields are present to avoid StrictUndefined errors
        safe_seo = {
            "content_gaps": market_insights.get("content_gaps", []),
            "brand_advantages": market_insights.get("brand_advantages", []),
            "writing_guide": market_insights.get("writing_guide", ""),
            "differentiation_strategy": market_insights.get("differentiation_strategy", []),
            "structural_patterns": market_insights.get("structural_patterns", [])
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
            "article_language": article_language,
            "requires_table": section.get("requires_table", False),
            "table_type": section.get("table_type", "none"),
            "requires_list": section.get("requires_list", False),
            "list_type": section.get("list_type", "none"),
            "cta_position": section.get("cta_position", "none"),
            "cta_type": cta_type, # New detailed flag
            "cta_allowed": section.get("cta_eligible", section.get("cta_allowed", False)),

            "article_intent": article_intent,
            "content_angle": section.get("content_angle", ""),
            "localized_angle": section.get("localized_angle", ""),
            "content_goal": section.get("content_goal", ""),
            "section_type": section.get("section_type", "core"),
            "decision_layer": section.get("decision_layer", "Market Reality"),
            "sales_intensity": section.get("sales_intensity", "medium"),
            "questions": section.get("questions", []),
            "assigned_links": section.get("assigned_links", []),
            "assigned_keywords": section.get("assigned_keywords", []),
            "mandatory_facts": section.get("mandatory_facts", []),
            "requires_table": section.get("requires_table", False),
            "table_type": section.get("table_type", "none"),
            "requires_list": section.get("requires_list", False),
            "list_type": section.get("list_type", "none"),
            "requires_primary_keyword": requires_primary_keyword,
            "global_keyword_count": global_keyword_count,
            "content_strategy": section.get("content_strategy", {}),
            
            # --- New Decision-Complete Writing Brief Fields ---
            "section_promise": section.get("section_promise", ""),
            "reader_takeaway": section.get("reader_takeaway", ""),
            "must_include_details": section.get("must_include_details", []),
            "must_not_repeat": section.get("must_not_repeat", []),
            "practical_decision_value": section.get("practical_decision_value", ""),
            "evidence_expectation": section.get("evidence_expectation", ""),
            "value_density_target": section.get("value_density_target", "high"),
            "allowed_generality_level": section.get("allowed_generality_level", "low"),
            "subheading_policy": section.get("subheading_policy", "direct_body")
        }


        print("Assigned links:", safe_section["assigned_links"])

        current_year = str(datetime.now().year)

        template_name = self.templates.get(
            content_type,
            self.templates["informational"]
        )
        template = self.env.get_template(template_name)

        # Rich Defaults with Deep Resilience
        final_blueprint = {
            "tonal_dna": {"persona": "Professional", "audience_level": "General", "forbidden_jargon": [], "sentence_rhythm": "Balanced"},
            "formatting_blueprint": {"bolding_frequency": "Standard"},
            "cta_strategy": {"density": "Balanced", "total_ideal_count": 3, "wording_patterns": []},
            "structural_skeleton": []
        }
        if style_blueprint:
            for k, v in style_blueprint.items():
                if isinstance(v, dict) and k in final_blueprint and isinstance(final_blueprint[k], dict):
                    final_blueprint[k].update(v)
                else:
                    final_blueprint[k] = v

        final_serp = {"reference_authority_links": []}
        if serp_data:
            final_serp.update(serp_data)

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
            execution_plan=execution_plan or {},
            area=area,
            used_phrases=used_phrases or [],
            used_topics=used_topics or [],
            used_anchors=used_anchors or [],
            previous_section_text=previous_section_text or "",
            previous_content_summary=previous_content_summary or "",
            used_internal_links=used_internal_links or [],
            used_external_links=used_external_links or [], 
            brand_name=brand_name,
            section_index=section_index,
            total_sections=total_sections,
            brand_context=brand_context,
            section_source_text=section_source_text,
            external_sources=external_sources or [],
            external_resources=external_resources or [],
            used_claims=used_claims or [],
            ctas_placed=ctas_placed,
            tables_placed=tables_placed,
            is_first_section=(section_index == 0),
            is_last_section=(section_index == total_sections - 1),
            prohibited_competitors=prohibited_competitors or [],
            current_year=current_year,
            workflow_mode=workflow_mode,
            # Advanced Customization
            tone=tone,
            pov=pov,
            brand_voice_description=brand_voice_description,
            brand_voice_guidelines=brand_voice_guidelines,
            brand_voice_examples=brand_voice_examples,
            custom_keyword_density=custom_keyword_density,
            bold_key_terms=bold_key_terms,
            requires_primary_keyword=requires_primary_keyword,
            introduction_text=introduction_text,
            full_outline=full_outline or [],
            style_blueprint=final_blueprint,
            serp_data=final_serp,
            area_neighborhoods=area_neighborhoods or [],
            global_keyword_count=global_keyword_count,
            brand_mentions_count=brand_mentions_count,
            draft_to_fix=draft_to_fix,
            brand_advantages=brand_advantages or [],
            writing_blueprint=writing_blueprint or "",
            market_angle=market_angle or "",
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
                # Aggressive fallback: Extract "content" value using regex if JSON parse fails
                content_match = re.search(r'"content":\s*"(.*?)"(?=,\s*"\w+":|\s*\})', response_content, re.DOTALL)
                if content_match:
                    extracted_content = content_match.group(1).encode().decode('unicode_escape', errors='ignore')
                    return {
                        "content": extracted_content,
                        "used_links": [],
                        "brand_link_used": False,
                        "metadata": metadata
                    }
                
                # If everything fails, clean the string of ANY JSON-like structure before returning
                cleaned_fallback = re.sub(r'\{.*?\}|\[.*?\]', '', response_content, flags=re.DOTALL).strip()
                return {
                    "content": cleaned_fallback if cleaned_fallback else response_content,
                    "used_links": [],
                    "brand_link_used": False,
                    "metadata": metadata
                }

            return {
                "content": data.get("content", ""),
                "used_links": data.get("used_links", []),
                "topics_covered": data.get("topics_covered", []),
                "brand_link_used": data.get("brand_link_used", False),
                "metadata": metadata
            }
        except Exception as e:
            logger.error(f"Error writing section {section.get('section_id', 'unknown')}: {e}")
            raise ContentGeneratorError(f"Section writing failed: {e}")

class Assembler:
    def __init__(self, ai_client: Any, template_path: str = "assets/prompts/templates/04_article_assembler.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    async def assemble(
        self,
        title: str,
        article_language: str,
        sections: List[Dict[str, Any]],
        content_type: str = "informational"
    ) -> Dict[str, str]:

        article_language = article_language or "ar"

        final_parts = [f"# {title}"]

        for idx, sec in enumerate(sections):
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

            # 1b) FAQ Structure Enforcement (The "Fluff Remover")
            if sec.get("section_type") == "faq":
                # Find the first H3 question (### Question)
                h3_match = re.search(r'^###\s+', content, re.MULTILINE)
                if h3_match:
                    # Strip everything before the first H3
                    content = content[h3_match.start():].strip()
                    logger.info(f"Mechanical FAQ Cleanup: Removed intro fluff from section {heading}")

            # 1c) CTA Completeness Check (Cut-off Repair)
            # If the content ends with a partial markdown link or dangling bracket
            if content.endswith(("[", "(", "!", "*", "_")):
                 content = re.sub(r'\s*[\(\[!*_]$', '', content).strip()
                 logger.warning(f"Mechanical CTA Cleanup: Trimmed dangling fragment from section {heading}")
            
            # Count open vs closed brackets to detect cut-off midway
            for open_char, close_char in [("[", "]"), ("(", ")")]:
                if content.count(open_char) > content.count(close_char):
                     # Find the last occurrence of the open character and strip from there
                     last_open = content.rfind(open_char)
                     if last_open != -1:
                        content = content[:last_open].strip()
                        logger.warning(f"Mechanical CTA Cleanup: Pruned unclosed {open_char} from section {heading}")

            # 2) Collapse multiple spaces (fixes issues like 'الوح  حدة')
            content = re.sub(r' +', ' ', content)

            # 3) Heading De-duplication (CRITICAL)
            # If the content starts with the same heading (e.g. "## FAQ"), remove that line.
            content = content.strip()
            content_lines = content.split("\n")
            if content_lines:
                first_line = content_lines[0].strip()
                clean_first_line = re.sub(r"^#+\s*", "", first_line).strip().lower()
                clean_heading = heading.lower()
                
                if clean_heading and (clean_first_line == clean_heading or clean_first_line.startswith(clean_heading)):
                    logger.info(f"[Assembler] Removing duplicate heading from content: '{first_line}'")
                    content = "\n".join(content_lines[1:]).strip()

            # Skip heading logic (v3.2): 
            is_intro_type = (sec.get("section_type") == "introduction")
            is_intro_name = any(x in heading.lower() for x in ["introduction", "مقدمة", "مقدمه"])
            
            skip_heading = False
            
            # Rule: INTRO_HEADING_FORBIDDEN
            if is_intro_type:
                # Introduction must never have a heading in the final output.
                skip_heading = True
            elif not heading.strip():
                skip_heading = True
            else:
                # We skip only for pure, simple introductions to avoid duplicating H1
                has_table = "|" in content and "---" in content
                has_list = bool(re.search(r'^\s*[-*•]\s|^\s*\d+\.\s', content, re.MULTILINE))
                is_specific_heading = len(heading.strip()) > 35 and not is_intro_name
                
                if idx == 0 and is_intro_name and not has_table and not has_list and not is_specific_heading:
                    skip_heading = True

            if not skip_heading:
                # Use Markdown heading level (default to H2)
                level_num = int(sec.get("heading_level", "H2").replace("H", "")) if isinstance(sec.get("heading_level"), str) else 2
                final_parts.append(f"{'#' * level_num} {heading}")

            if sec.get("section_id"):
                final_parts.append(f"<!-- section_id: {sec['section_id']} -->")

            final_parts.append(content)

            # final_parts.append(f"{'#' * level_num} {heading}")
            # final_parts.append(content)

        final_markdown = "\n\n".join([p for p in final_parts if p])
        
        return {
            "final_markdown": final_markdown
        }
class FinalHumanizer:
    def __init__(self, ai_client: Any, template_path: str = "assets/prompts/templates/05_final_humanizer.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    async def humanize_section(
        self,
        full_article_context: str,
        target_section_content: str,
        target_section_heading: str,
        article_language: str,
        brand_name: str,
        brand_source_text: str,
        brand_advantages: str,
        section: Dict[str, Any] = None,
        is_introduction: bool = False,
        is_conclusion: bool = False,
        brand_mentions_total_count: int = 0,
        global_keyword_count: int = 0
    ) -> str:
        
        prompt = self.template.render(
            full_article_context=full_article_context,
            target_section_content=target_section_content,
            target_section_heading=target_section_heading,
            article_language=article_language or "Arabic",
            brand_name=brand_name or "",
            brand_source_text=brand_source_text or "",
            brand_advantages=brand_advantages or "",
            section=section or {},
            is_introduction=is_introduction,
            is_conclusion=is_conclusion,
            brand_mentions_total_count=brand_mentions_total_count,
            global_keyword_count=global_keyword_count
        )
        
        try:
            res = await self.ai_client.send(prompt=prompt, step="final_humanizer")
            data = recover_json(res["content"])
            
            if not data:
                # Handle non-JSON or broken JSON response
                return target_section_content
            
            extracted_content = data.get("content", target_section_content)
            
            # Clean JSON wrapping if the AI accidentally returns raw markdown wrapping inside JSON
            if extracted_content.startswith("```markdown"):
                extracted_content = extracted_content.replace("```markdown\n", "").replace("\n```", "")
            
            # Collapse multiple spaces
            extracted_content = re.sub(r' +', ' ', extracted_content)
                
            return extracted_content
        except Exception as e:
            logger.error(f"[FinalHumanizer] Failed to humanize section '{target_section_heading}': {e}")
            return target_section_content # Fallback to original
