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
        # Specialized templates for structural review mode (heading-only)
        self.heading_only_templates = {
            "brand_commercial": "01_outline_generator_heading_only_commercial_v2.txt",
            "informational": "01_outline_generator_heading_only_informational_v2.txt"
        }
        # Fallback for legacy calls or missing types
        self.heading_only_fallback = "01_outline_generator_heading_only_v2.txt"

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
        section.setdefault("depth_goal", "")
        section.setdefault("reader_takeaway", "")
        section.setdefault("must_include_details", [])
        section.setdefault("must_not_repeat", [])
        section.setdefault("practical_decision_value", "")
        section.setdefault("taxonomy_axis", "")
        section.setdefault("preferred_axis", "")
        section.setdefault("forbidden_taxonomy_axis", "")
        section.setdefault("observed_data_mentions", [])
        section.setdefault("evidence_expectation", "")
        section.setdefault("value_density_target", "high")
        section.setdefault("allowed_generality_level", "low")
        section.setdefault("subheading_policy", "direct_body")
        section.setdefault("subheadings", [])

        # --- Semantic Execution Layer ---
        section.setdefault("execution_mode", "")
        section.setdefault("semantic_goal", "")
        section.setdefault("decision_frame", "")
        section.setdefault("content_behavior", "")

        section.setdefault("content_type", content_type)
        section.setdefault("content_strategy", content_strategy)
        section.setdefault("area", area)
        section.setdefault("requires_table", False)
        section.setdefault("table_type", "none")
        section.setdefault("requires_list", False)
        section.setdefault("list_type", "none")
        section.setdefault("cta_position", "none")
        # --- Primary keyword enforcement for strategic sections ---
        sec_type = (section.get("section_type") or "").lower()
        if sec_type == "introduction" or idx == 0:
            section.setdefault("requires_primary_keyword", True)
        else:
            section.setdefault("requires_primary_keyword", section.get("requires_primary_keyword", False))

        # --- Apply Semantic Execution State ---
        self._apply_semantic_execution_state(section, idx)

    def _apply_semantic_execution_state(self, section: Dict[str, Any], idx: int):
        """
        Maps a section to a cognitive execution mode based on its type and taxonomy axis.
        """
        from src.services.strategy_service import SEMANTIC_EXECUTION_LAYER
        
        sec_type = (section.get("section_type") or "").lower()
        taxonomy = (section.get("taxonomy_axis") or "").lower()
        heading = (section.get("heading_text") or "").lower()
        
        mode_key = "taxonomy_breakdown"
        
        if sec_type == "introduction" or idx == 0:
            mode_key = "onboarding_context"
        elif sec_type == "conclusion":
            mode_key = "buyer_guidance"
        elif sec_type == "faq":
            mode_key = "buyer_guidance"
        elif any(kw in taxonomy or kw in heading for kw in ["price", "cost", "pricing", "أسعار", "تكلفة", "سعر"]):
            mode_key = "market_practical"
        elif any(kw in taxonomy or kw in heading for kw in ["location", "area", "neighborhood", "أحياء", "موقع"]):
            mode_key = "locality_analysis"
        elif "process" in sec_type or "how" in sec_type or "خطوات" in heading:
            mode_key = "buyer_guidance"
        elif any(kw in taxonomy or kw in heading for kw in ["comparison", "vs", "مقارنة"]):
            mode_key = "comparison_decision"
        elif sec_type == "proof" or "trust" in taxonomy or "دليل" in heading:
            mode_key = "trust_proof"
            
        state = SEMANTIC_EXECUTION_LAYER.get(mode_key, SEMANTIC_EXECUTION_LAYER["taxonomy_breakdown"])
        
        if not section.get("execution_mode"):
            section["execution_mode"] = state["execution_mode"]
        if not section.get("semantic_goal"):
            section["semantic_goal"] = state["semantic_goal"]
        if not section.get("decision_frame"):
            section["decision_frame"] = state["decision_frame"]
        if not section.get("content_behavior"):
            section["content_behavior"] = state["content_behavior"]

    def _validate_outline_schema(self, outline: List[Dict[str, Any]], heading_only_mode: bool = False) -> bool:
        if heading_only_mode:
            required_keys = {
                "section_id", "heading_level", "heading_text", "section_type", "section_intent"
            }
        else:
            required_keys = {
                "section_id", "heading_level", "heading_text", "section_intent",
                "section_promise", "reader_takeaway", "must_include_details",
                "must_not_repeat", "practical_decision_value", "evidence_expectation",
                "value_density_target", "allowed_generality_level", "subheading_policy"
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
            serp_outline_brief: Optional[Dict[str, Any]] = None,
            head_entity: str = "",
            entity_phrase: str = "",
            service_phrase: str = ""
        ) -> Dict[str, Any]:



        current_year = str(datetime.now().year)

        # Heading-Only Mode Isolation: Use specialized lightweight templates per content type
        if heading_only_mode:
            template_name = self.heading_only_templates.get(
                content_type,
                self.heading_only_fallback
            )
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
            serp_outline_brief=serp_outline_brief,
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

        if heading_only_mode and content_type == "informational":
            outline = self._repair_heading_only_informational_outline(outline, primary_keyword, title)
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

    def _repair_heading_only_informational_outline(
        self,
        outline: List[Dict[str, Any]],
        primary_keyword: str,
        title: str = "",
    ) -> List[Dict[str, Any]]:
        """Small deterministic repairs for heading-only informational outlines.

        Heading-only outlines are easy for the model to compress too much:
        it may use the intro as a definition, or merge location, access,
        timing, and ticketing into one broad visitor-info section. This
        keeps those high-value visitor intents as separate sections before
        the writer expands them.
        """
        if not outline:
            return outline

        def norm(value: Any) -> str:
            return re.sub(r"\s+", " ", str(value or "").strip().lower())

        def has_any(text: str, terms: List[str]) -> bool:
            normalized = norm(text)
            return any(term in normalized for term in terms)

        def is_h2(section: Dict[str, Any]) -> bool:
            return str(section.get("heading_level", "")).upper() == "H2"

        def is_definition_heading(text: str) -> bool:
            normalized = norm(text)
            keyword = norm(primary_keyword)
            if "definition" in normalized or "تعريف" in normalized or "ما المقصود" in normalized:
                return True
            if normalized.startswith("what is"):
                return True
            return normalized.startswith(f"ما هو {keyword}") or normalized.startswith(f"ما هي {keyword}")

        def category_count(text: str) -> int:
            return sum(
                1
                for terms in (location_terms, hours_terms, booking_terms)
                if has_any(text, terms)
            )

        topic_blob = " ".join([title, primary_keyword] + [str(s.get("heading_text", "")) for s in outline])
        experience_terms = [
            "visit", "visitor", "venue", "destination", "attraction", "event", "tickets",
            "mall", "museum", "park", "restaurant", "exhibition", "festival", "show", "city",
            "زيارة", "زوار", "وجهة", "ترفيه", "أنشطة", "انشطة", "تجارب", "فعاليات",
            "تذاكر", "حجز", "موسم", "مول", "متحف", "حديقة", "مطعم", "مطاعم",
            "مدينة", "منطقة", "منتزه", "معرض", "مسرح", "حفلات", "عروض",
            "الموقع", "الوصول", "أوقات", "اوقات", "مواعيد", "دخول", "رسوم",
        ]
        if not has_any(topic_blob, experience_terms):
            return outline

        intro = outline[0]
        intro_was_definition = False
        if norm(intro.get("section_type")) == "introduction":
            intro_text = norm(intro.get("heading_text"))
            if is_definition_heading(intro_text):
                intro_was_definition = True
                intro["heading_text"] = f"مدخل تمهيدي عن زيارة {primary_keyword}".strip()
                intro["subheadings"] = []

        location_terms = [
            "location", "access", "directions", "parking", "transport",
            "الموقع", "الوصول", "العنوان", "أين تقع", "اين تقع", "مواقف", "النقل",
        ]
        hours_terms = [
            "opening hours", "hours", "timing", "schedule",
            "أوقات", "اوقات", "مواعيد", "ساعات", "متى",
        ]
        booking_terms = [
            "tickets", "ticket", "booking", "reservation", "entry", "pricing", "prices",
            "تذاكر", "التذاكر", "حجز", "الحجز", "دخول", "رسوم", "أسعار", "اسعار",
        ]

        def h2_has_standalone(terms: List[str]) -> bool:
            return any(
                is_h2(s)
                and has_any(str(s.get("heading_text", "")), terms)
                and category_count(str(s.get("heading_text", ""))) <= 1
                for s in outline
            )

        repaired: List[Dict[str, Any]] = []
        inserted_hours = h2_has_standalone(hours_terms)
        inserted_booking = h2_has_standalone(booking_terms)
        has_definition_h2 = any(
            is_h2(section)
            and is_definition_heading(str(section.get("heading_text", "")))
            for section in outline
        )

        for idx, section in enumerate(outline):
            if idx == 1 and intro_was_definition and not has_definition_h2:
                repaired.append({
                    "heading_text": f"ما هو {primary_keyword}؟",
                    "heading_level": "H2",
                    "section_type": "core_or_benefits",
                    "section_intent": "informational",
                    "subheadings": [],
                })
                has_definition_h2 = True

            heading = str(section.get("heading_text", ""))
            cats = {
                "location": has_any(heading, location_terms),
                "hours": has_any(heading, hours_terms),
                "booking": has_any(heading, booking_terms),
            }

            if is_h2(section) and sum(1 for value in cats.values() if value) >= 2:
                if cats["location"]:
                    section["heading_text"] = f"أين تقع {primary_keyword} وكيف تصل إليها؟"
                    section["section_type"] = "location"
                    section["subheadings"] = []
                    repaired.append(section)
                else:
                    repaired.append(section)

                if cats["hours"] and not inserted_hours:
                    repaired.append({
                        "heading_text": f"مواعيد عمل {primary_keyword} وأفضل أوقات الزيارة",
                        "heading_level": "H2",
                        "section_type": "process_or_how",
                        "section_intent": "informational",
                        "subheadings": [],
                    })
                    inserted_hours = True

                if cats["booking"] and not inserted_booking:
                    repaired.append({
                        "heading_text": f"تذاكر {primary_keyword} وطريقة الحجز",
                        "heading_level": "H2",
                        "section_type": "process_or_how",
                        "section_intent": "informational",
                        "subheadings": [],
                    })
                    inserted_booking = True
                continue

            repaired.append(section)

        faq_has_booking = any(
            norm(s.get("section_type")) == "faq"
            and any(has_any(str(sub), booking_terms) for sub in s.get("subheadings", []) or [])
            for s in repaired
        )
        if faq_has_booking and not inserted_booking:
            insert_at = next(
                (
                    idx for idx, section in enumerate(repaired)
                    if norm(section.get("section_type")) in {"faq", "conclusion"}
                ),
                len(repaired),
            )
            repaired.insert(insert_at, {
                "heading_text": f"تذاكر {primary_keyword} وطريقة الحجز",
                "heading_level": "H2",
                "section_type": "process_or_how",
                "section_intent": "informational",
                "subheadings": [],
            })
            inserted_booking = True

        for idx, section in enumerate(repaired, start=1):
            section["section_id"] = f"sec_{idx:02d}"

        return repaired


    async def critique_outline(
        self,
        primary_keyword: str,
        title: str,
        outline: list,
        content_type: str,
        intent: str,
        area: str,
        entity_phrase: str,
        service_phrase: str,
        display_brand_name: str,
        content_strategy: dict,
        heading_quality_audit: dict
    ) -> dict:
        """AI-driven holistic critique of the outline. Diagnostic only."""
        try:
            template = self.env.get_template('01c_outline_critique.txt')
            prompt = template.render(
                primary_keyword=primary_keyword,
                title=title,
                outline=outline,
                content_type=content_type,
                intent=intent,
                area=area,
                entity_phrase=entity_phrase,
                service_phrase=service_phrase,
                display_brand_name=display_brand_name,
                content_strategy=content_strategy,
                heading_quality_audit=heading_quality_audit
            )
            
            res = await self.ai_client.send(prompt, step='outline_critique')
            raw = res['content']
            
            if not raw:
                return self._safe_critique_fallback('Empty response from AI critique.')
                
            json_text = self._extract_first_json_object(raw)
            from src.utils.json_utils import recover_json
            data = recover_json(json_text)
            
            if not isinstance(data, dict):
                return self._safe_critique_fallback('Invalid JSON structure in AI critique.')
                
            return data
            
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f'AI Outline Critique failed: {e}')
            return self._safe_critique_fallback(str(e))

    async def fix_outline_headings(
        self,
        primary_keyword: str,
        outline: list,
        content_type: str,
        area: str,
        entity_phrase: str,
        service_phrase: str,
        display_brand_name: str,
        content_strategy: dict,
        heading_quality_audit: dict,
        ai_outline_critique: dict
    ) -> dict:
        """Applies controlled fixes to headings based on audit and critique."""
        try:
            template = self.env.get_template('01d_heading_fix.txt')
            prompt = template.render(
                primary_keyword=primary_keyword,
                outline=outline,
                content_type=content_type,
                area=area,
                entity_phrase=entity_phrase,
                service_phrase=service_phrase,
                display_brand_name=display_brand_name,
                content_strategy=content_strategy,
                heading_quality_audit=heading_quality_audit,
                ai_outline_critique=ai_outline_critique
            )
            
            res = await self.ai_client.send(prompt, step='heading_fix')
            raw = res['content']
            
            if not raw:
                return {"outline": outline, "changes": [], "error": "Empty AI response"}
                
            json_text = self._extract_first_json_object(raw)
            from src.utils.json_utils import recover_json
            data = recover_json(json_text)
            
            if not isinstance(data, dict) or "outline" not in data:
                return {"outline": outline, "changes": [], "error": "Invalid JSON structure"}
                
            return data
            
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f'Heading fix failed: {e}')
            return {"outline": outline, "changes": [], "error": str(e)}

    def _safe_critique_fallback(self, error_msg: str) -> dict:
        return {
            'mode': 'critique_only',
            'overall_score': 0,
            'passed': True,
            'summary': f'Critique unavailable due to error: {error_msg}',
            'missing_sections': [],
            'redundant_sections': [],
            'weak_sections': [],
            'h3_issues': [],
            'repetition_issues': [],
            'seo_coverage_gaps': [],
            'brand_alignment_issues': [],
            'faq_issues': [],
            'accepted_variations': [],
            'top_recommendations': []
        }

    def _extract_first_json_object(self, text: str) -> str:
        if not text: return ''
        start = text.find('{')
        if start == -1: return text
        count = 0
        for i in range(start, len(text)):
            if text[i] == '{': count += 1
            elif text[i] == '}':
                count -= 1
                if count == 0: return text[start:i+1]
        return text[start:]

class SectionWriter:
    def __init__(self, ai_client: Any):
        self.ai_client = ai_client
        self.env = Environment(
            loader=FileSystemLoader("assets/prompts/templates"),
            undefined=StrictUndefined
        )

        self.templates = {
            "brand_commercial": "02_section_writer_brand_commercial_v2.txt",
            "informational": "02_section_writer_informational.txt",
            "comparison": "02_section_writer_comparison.txt",
        }

    def _build_operational_instructions(self, section: Dict[str, Any]) -> List[str]:
        """
        Converts descriptive section metadata into action-oriented writing instructions.
        """
        instructions = []
        mode = section.get("execution_mode", "taxonomy_breakdown")
        axis = section.get("taxonomy_axis", "")
        forbidden = section.get("forbidden_taxonomy_axis", "")
        observed = section.get("observed_data_mentions", [])
        must_include = section.get("must_include_details", [])
        
        # 1. Mode-Specific Operational Actions
        if mode == "market_practical":
            instructions.append("Explain clearly why prices vary and what drives the cost in this context.")
            if axis:
                instructions.append(f"Compare affordability primarily by the {axis} axis.")
            instructions.append("Provide the reader with a practical budget decision takeaway or rule of thumb.")
            if "category" in forbidden or "type" in forbidden:
                instructions.append("Avoid repeating generic type/category definitions; stay focused on price and value logic.")
                
        elif mode == "locality_analysis":
            instructions.append("Connect every mentioned area or location directly to specific resident lifestyle needs.")
            instructions.append("Explain practical living factors: accessibility, services, commute, or family/work fit.")
            instructions.append("Avoid pure geographic listing; prioritize 'vibe' and suitability over distance facts.")
            
        elif mode == "taxonomy_breakdown":
            instructions.append("Classify all options or categories with clear, non-overlapping logic.")
            instructions.append("Explain the practical differences between categories from a user's usage perspective.")
            instructions.append("Explicitly match each category to a specific user situation or persona.")
            if "price" not in (section.get("heading_text") or "").lower():
                instructions.append("Avoid pricing-first logic; focus on functional and structural fit.")
                
        elif mode == "comparison_decision":
            instructions.append("Evaluate specific trade-offs directly between the compared items.")
            instructions.append("Explain clearly 'when each option wins' based on user priorities.")
            instructions.append("Avoid generic summaries; ensure the comparison leads to a clear decision path.")
            
        elif mode == "buyer_guidance":
            instructions.append("Guide the reader through specific selection criteria or a practical process.")
            instructions.append("Address common points of confusion or hesitation directly.")
            instructions.append("Ensure every paragraph offers a practical tip or an 'if-then' recommendation.")
            
        # 2. Data Grounding Actions
        if observed:
            instructions.append("Treat the provided 'observed_data_mentions' as cautious grounding hints only.")
            instructions.append("Do NOT present these data points as definitive market statistics; use them to support relative logic.")
            
        # 3. Content Requirement Actions
        if must_include:
            for detail in must_include:
                # Convert descriptive detail into action instruction
                instructions.append(f"Integrate detail about: {detail} with a focus on practical utility.")
                
        # 4. Axis Enforcement Actions
        if forbidden:
            instructions.append(f"Strictly avoid using the '{forbidden}' axis for this section's logic.")

        return instructions

    def _build_cognitive_blueprint(self, section: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates an internal reasoning plan (blueprint) for the section.
        """
        mode = section.get("execution_mode")
        axis = section.get("taxonomy_axis", "")
        observed = section.get("observed_data_mentions", [])
        must_include = section.get("must_include_details", [])
        
        blueprint = {
            "section_thesis": "",
            "decision_logic": [],
            "evidence_plan": [],
            "reader_value": "",
            "avoid_patterns": ["generic explanatory prose", "repetitive intro filler"]
        }
        
        if mode == "market_practical":
            blueprint["section_thesis"] = "Clarify the realistic cost-to-value relationship for the reader's budget."
            blueprint["decision_logic"] = ["Explain budget tradeoffs", f"Compare affordability by {axis or 'market tiers'}"]
            blueprint["evidence_plan"] = ["Use relative price tiers", f"Ground logic in {observed or 'market standards'}"]
            blueprint["reader_value"] = "Confidence in financial expectations and budget planning."
            blueprint["avoid_patterns"].extend(["unsupported exact prices", "generic pricing filler"])
            
        elif mode == "locality_analysis":
            blueprint["section_thesis"] = "Connect specific locations directly to the resident's daily lifestyle and needs."
            blueprint["decision_logic"] = ["Map area vibes to personas", "Analyze service accessibility and commute fit"]
            blueprint["evidence_plan"] = ["Reference local anchors", f"Integrate {must_include or 'area highlights'}"]
            blueprint["reader_value"] = "Finding a neighborhood that matches their practical daily routine."
            blueprint["avoid_patterns"].extend(["pure geographic distance lists", "generic 'near services' claims"])
            
        elif mode == "taxonomy_breakdown":
            blueprint["section_thesis"] = "Differentiate available options based on functional and situational fit."
            blueprint["decision_logic"] = ["Group by user situation", "Highlight structural or service differences"]
            blueprint["evidence_plan"] = ["Match features to personas", "Use clear category distinctions"]
            blueprint["reader_value"] = "Choosing the specific type that solves their immediate need."
            blueprint["avoid_patterns"].extend(["pricing-first thinking", "overlapping category definitions"])
            
        elif mode == "comparison_decision":
            blueprint["section_thesis"] = "Identify the core tradeoffs to simplify a difficult choice between options."
            blueprint["decision_logic"] = ["Side-by-side suitability check", "Explain 'win' conditions for each option"]
            blueprint["evidence_plan"] = ["Contrast specific features", "Highlight situational winners"]
            blueprint["reader_value"] = "Clarity on which option is the objective best fit for their situation."
            blueprint["avoid_patterns"].extend(["neutral descriptive summaries", "vague 'both are good' conclusions"])
            
        elif mode == "buyer_guidance":
            blueprint["section_thesis"] = "Clarify the next practical step or decision criteria to reduce selection friction."
            blueprint["decision_logic"] = ["Process walk-through", "Objection handling via logic"]
            blueprint["evidence_plan"] = ["Checklists or 'if-then' tips", "Practical readiness markers"]
            blueprint["reader_value"] = "Actionable path forward with reduced decision anxiety."
            blueprint["avoid_patterns"].extend(["encyclopedic advice", "theoretical market commentary"])
            
        elif mode == "trust_proof":
            blueprint["section_thesis"] = "Establish the reliability and safety of the choice through concrete evidence."
            blueprint["decision_logic"] = ["Risk reduction analysis", "Validation of process transparency"]
            blueprint["evidence_plan"] = ["Trust signals", "Verification steps"]
            blueprint["reader_value"] = "Feeling safe and informed before committing resources."
            blueprint["avoid_patterns"].extend(["aggressive promotion", "unsubstantiated trust claims"])
            
        elif mode == "onboarding_context":
            blueprint["section_thesis"] = "Orient the reader by validating their problem and promising a specific solution path."
            blueprint["decision_logic"] = ["Identify the pain point", "Define the decision landscape"]
            blueprint["evidence_plan"] = ["High-level market context", "Brand mission alignment"]
            blueprint["reader_value"] = "Immediate clarity on why this article is the right resource for them."
            blueprint["avoid_patterns"].extend(["deep technical details", "premature calls to action"])
            
        else:
            blueprint["section_thesis"] = f"Explain the {axis or 'topic'} to help the reader understand their options."
            blueprint["decision_logic"] = ["Clear classification"]
            blueprint["reader_value"] = "General awareness and orientation."

        return blueprint

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
        supporting_keywords = global_keywords.get("lsi", []) + global_keywords.get("semantic", [])
        article_language = section.get("article_language") or "ar"
        allowed_flow = section.get("allowed_flow_steps", [])

        market_insights = seo_intelligence.get("market_analysis", {}).get("market_insights", {})
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
            "subheadings": section.get("subheadings", []),
            "section_contract": section.get("section_contract", {}),
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
            "cta_type": cta_type, 
            "cta_allowed": section.get("cta_eligible", section.get("cta_allowed", False)),
            "article_intent": article_intent,
            "content_angle": section.get("content_angle", ""),
            "localized_angle": section.get("localized_angle", ""),
            "content_goal": section.get("content_goal", ""),
            "section_type": section.get("section_type", "core"),
            "decision_layer": section.get("decision_layer", "Market Reality"),
            "sales_intensity": section.get("sales_intensity", "medium"),
            "questions": section.get("questions", []),
            "mandatory_facts": section.get("mandatory_facts", []),
            "requires_primary_keyword": requires_primary_keyword,
            "global_keyword_count": global_keyword_count,
            "content_strategy": section.get("content_strategy", {}),
            
            # --- Decision-Complete Writing Brief Fields (Hardened Defaults) ---
            "section_promise": section.get("section_promise", ""),
            "reader_takeaway": section.get("reader_takeaway", ""),
            "depth_goal": section.get("depth_goal", ""),
            "must_include_details": section.get("must_include_details", []),
            "must_not_repeat": section.get("must_not_repeat", []),
            "practical_decision_value": section.get("practical_decision_value", ""),
            "taxonomy_axis": section.get("taxonomy_axis", ""),
            "preferred_axis": section.get("preferred_axis", ""),
            "forbidden_taxonomy_axis": section.get("forbidden_taxonomy_axis", ""),
            "observed_data_mentions": section.get("observed_data_mentions", []),
            "evidence_expectation": section.get("evidence_expectation", ""),
            "allowed_generality_level": section.get("allowed_generality_level", "low"),
            "subheading_policy": section.get("subheading_policy", "direct_body"),
            
            # --- Semantic Execution Layer ---
            "execution_mode": section.get("execution_mode", ""),
            "semantic_goal": section.get("semantic_goal", ""),
            "decision_frame": section.get("decision_frame", ""),
            "content_behavior": section.get("content_behavior", "")
        }

        # --- Operational Contract Adapter Layer ---
        operational_instructions = self._build_operational_instructions(safe_section)
        safe_section["operational_instructions"] = operational_instructions

        # --- Cognitive Blueprint Layer ---
        cognitive_blueprint = self._build_cognitive_blueprint(safe_section)
        safe_section["cognitive_blueprint"] = cognitive_blueprint

        # --- Runtime Mode Injection ---
        from src.services.strategy_service import WRITER_MODE_PROFILES
        mode_key = safe_section["execution_mode"] or "taxonomy_breakdown"
        mode_instructions = WRITER_MODE_PROFILES.get(mode_key, WRITER_MODE_PROFILES["taxonomy_breakdown"])

        # --- Regional Arabic Adaptation Layer ---
        from src.services.strategy_service import REGIONAL_ARABIC_PROFILES
        regional_profile = ""
        if article_language == "ar":
            area_norm = (area or "").lower()
            if any(kw in area_norm for kw in ["مصر", "egypt", "cairo", "alexandria", "القاهرة", "الاسكندرية", "الجيزة"]):
                regional_profile = REGIONAL_ARABIC_PROFILES["egypt"]
            elif any(kw in area_norm for kw in ["السعودية", "saudi", "riyadh", "jeddah", "الرياض", "جدة", "الدمام", "مكة"]):
                regional_profile = REGIONAL_ARABIC_PROFILES["saudi"]
            elif any(kw in area_norm for kw in ["الامارات", "uae", "dubai", "abu dhabi", "دبي", "ابوظبي", "الشارقة"]):
                regional_profile = REGIONAL_ARABIC_PROFILES["uae"]

        current_year = str(datetime.now().year)
        template_name = self.templates.get(content_type, self.templates["informational"])
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
            mode_instructions=mode_instructions,
            regional_profile=regional_profile,
            operational_instructions=operational_instructions,
            cognitive_blueprint=cognitive_blueprint,
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

        try:
            res = await self.ai_client.send(prompt, step=f"section_{section_index+1}")
            response_content = res["content"]
            metadata = res["metadata"]

            if not response_content:
                return {"content": "", "used_links": [], "brand_link_used": False, "metadata": metadata}

            data = recover_json(response_content)
            if not data:
                content_match = re.search(r'"content":\s*"(.*?)"(?=,\s*"\w+":|\s*\})', response_content, re.DOTALL)
                if content_match:
                    extracted_content = content_match.group(1).encode().decode('unicode_escape', errors='ignore')
                    return {"content": extracted_content, "used_links": [], "brand_link_used": False, "metadata": metadata}
                
                cleaned_fallback = re.sub(r'\{.*?\}|\[.*?\]', '', response_content, flags=re.DOTALL).strip()
                return {"content": cleaned_fallback if cleaned_fallback else response_content, "used_links": [], "brand_link_used": False, "metadata": metadata}

            return {
                "content": data.get("content", ""),
                "used_links": data.get("used_links", []),
                "knowledge_units_established": data.get("knowledge_units_established", []),
                "topics_covered": data.get("topics_covered", []),
                "brand_link_used": data.get("brand_link_used", False),
                "metadata": metadata
            }

        except Exception as e:
            logger.error(f"SectionWriter failed for section {section_index + 1}: {e}")
            return {"content": "", "used_links": [], "brand_link_used": False, "metadata": {}}


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
