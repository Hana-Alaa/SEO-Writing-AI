import os
import logging
import json
import asyncio
import re
from typing import Dict, Any, List, Optional
from jinja2 import Template
from src.utils.json_utils import recover_json

from src.utils.style_extractor import StyleExtractor

logger = logging.getLogger(__name__)

LOCKED_BRAND_TARGET_READER_STATE = (
    "A buyer with little or no prior market knowledge who needs simple, practical "
    "guidance to understand the available options, compare them confidently, and "
    "take a clear next step without feeling overwhelmed."
)

LOCKED_BRAND_TONE_DIRECTION = (
    "Clear, confident, beginner-friendly, practical, and persuasive without pressure."
)

LOCKED_BRAND_CTA_PHILOSOPHY = (
    "Earn action through clarity and trust. A very soft CTA may appear at the end "
    "of the introduction only if the section has already delivered clear value. "
    "Reserve the main CTA for the conclusion."
)

LOCKED_BRAND_SECTION_ROLE_MAP = {
    "introduction": (
        "Start with a light, relevant hook that reflects the buyer's need. Naturally "
        "introduce the primary keyword. Briefly explain what the reader will "
        "understand or be able to decide after reading. Optionally include one soft "
        "brand mention and one very soft CTA only if it feels earned by the value "
        "already given. Avoid urgency, investment language, legal framing, or generic "
        "market commentary."
    ),
    "core_or_benefits": (
        "Combine offer clarity with key buyer-facing features. Explain what the "
        "offering is, what types or forms are available, and what the buyer "
        "practically gets, using simple and scannable language."
    ),
    "proof": (
        "Provide concrete product-tied proof such as pricing reality, value "
        "differences, availability, delivery status, or trust signals connected "
        "directly to the entity and location. Proof must stay tied to the product "
        "at the unit level or listing level, not abstract market conditions. Do "
        "not drift into broad market commentary, investment framing, or generic "
        "authority language unless the support is directly tied to the buyer's "
        "decision about the original entity."
    ),
    "process_or_how": (
        "Explain the practical buying journey step by step, from filtering and "
        "shortlisting to inquiry, viewing, and decision, without legal or "
        "contract-heavy framing unless explicitly justified."
    ),
    "faq": (
        "Answer beginner buyer questions and objections in simple language, "
        "especially around choosing, price, readiness, and the buying steps."
    ),
    "conclusion": (
        "Summarize the value clearly, reduce hesitation, and guide the reader to a "
        "confident next step with a direct but not pushy CTA."
    ),
}

STRATEGY_UNSAFE_PHRASES = [
    "performance-first execution",
    "comparing providers",
    "fear of losing leads",
    "business outcomes",
    "implementation path",
    "delivery model",
    "provider selection",
    "digital presence",
    "broad market opportunity",
]

INVESTMENT_HEAVY_PHRASES = [
    "roi",
    "investment return",
    "yield",
    "capital appreciation",
    "resale return",
    "investment opportunity",
    "investment",
]

LEGAL_HEAVY_PHRASES = [
    "legal verification",
    "compliance",
    "documentation checklist",
    "contract execution",
    "legal",
]

class StrategyService:
    """Service dedicated to intent detection, brand style analysis, and content strategy."""

    def __init__(self, ai_client, title_generator, jinja_env, intent_template=None):
        self.ai_client = ai_client
        self.title_generator = title_generator
        self.env = jinja_env
        self.intent_template = intent_template
        self.style_extractor = StyleExtractor(ai_client)
        
        self.strategy_map = {
            "brand_commercial": "00_content_strategy_brand_commercial_observed_v2.txt",
            "informational": "00_content_strategy_informational.txt",
            "comparison": "00_content_strategy_comparison.txt",
        }

    SUPPORTED_LANGS = {"ar", "en", "de", "fr", "es", "it", "tr", "pt"}
    LANG_ALIASES = {
        "arabic": "ar", "english": "en", "german": "de",
        "zh-cn": "zh", "zh-tw": "zh", "pt-br": "pt",
        "en-us": "en", "en-gb": "en"
    }

    def normalize_lang(self, lang: Optional[str]) -> Optional[str]:
        """Normalizes language codes."""
        if not lang:
            return None
        code = str(lang).strip().lower().replace("_", "-")
        code = self.LANG_ALIASES.get(code, code)
        code = code.split("-")[0]
        return code if code in self.SUPPORTED_LANGS else None

    def detect_title_language(self, raw_title: str) -> Optional[str]:
        """Detects language from title."""
        title = (raw_title or "").strip()
        if not title:
            return None

        # Heuristic for Arabic script
        if re.search(r"[\u0600-\u06FF]", title):
            return "ar"

        if len(re.findall(r"\w+", title)) < 2:
            return None

        try:
            from langdetect import detect_langs
            candidates = detect_langs(title)
            if not candidates:
                return None
            top = candidates[0]
            if float(top.prob) < 0.70:
                return None
            return self.normalize_lang(top.lang)
        except Exception as e:
            return None

    def resolve_article_language(self, raw_title: str, user_lang: Optional[str]) -> str:
        """Resolves the best article language."""
        normalized_user = self.normalize_lang(user_lang)
        if normalized_user:
            return normalized_user

        detected = self.detect_title_language(raw_title)
        if detected:
            return detected

        return "en"

    async def run_intent_title(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Classify user intent and refine the title via AI."""
        raw_title = state.get("raw_title") or "Untitled"
        primary_keyword = state.get("primary_keyword") or raw_title
        article_language = state.get("article_language") or "en"
        area = state.get("area")
        serp_data = state.get("serp_data", {})

        top_titles = [
            r.get("title", "")
            for r in serp_data.get("top_results", [])
            if isinstance(r, dict)
        ][:5]

        cta_styles = [
            r.get("cta_style", "")
            for r in serp_data.get("top_results", [])
            if isinstance(r, dict)
        ]

        res = await self.title_generator.generate(
            raw_title=raw_title,
            primary_keyword=primary_keyword,
            article_language=article_language,
            serp_titles=top_titles,
            serp_cta_styles=cta_styles,
            area=area,
            brand_name=state.get("brand_name", "")
        )
        
        if state.get("workflow_logger"):
            state["workflow_logger"].log_ai_call(
                step_name="intent_title",
                prompt=res.get("prompt"),
                response=res,
                tokens=res.get("metadata", {}),
                duration=res.get("metadata", {}).get("duration", 0)
            )

        intent_raw = res.get("intent", "Informational")
        optimized_title = res.get("optimized_title", raw_title)

        # Logic for local SEO intent refinement
        serp_confirmed = (
            state.get("seo_intelligence", {})
                .get("market_analysis", {})
                .get("intent_analysis", {})
                .get("confirmed_intent")
        )
        confidence = (
            state.get("seo_intelligence", {})
                .get("market_analysis", {})
                .get("intent_analysis", {})
                .get("intent_confidence_score", 0)
        )

        if confidence > 0.6 and serp_confirmed:
            intent_raw = serp_confirmed

        intent_normalized = intent_raw.strip().lower()
        state["intent"] = intent_normalized

        # 1. Run Strategic AI Classifier (Universal Thinking)
        detected_intent = await self.detect_intent_ai(raw_title, primary_keyword, state=state)
        
        # 2. Reconcile Intents (Combining Title Intent and Strategic Logic)
        # We look into both intent_normalized (from title) and detected_intent (from classifier)
        all_intents = f"{intent_normalized} {detected_intent}"
        
        # 3. Mode-based Content Type Selection (Prioritize Intelligent Logic)
        user_article_type = state.get("article_type")
        if state.get("workflow_mode") == "advanced" and user_article_type:
            if user_article_type == "commercial":
                state["content_type"] = "brand_commercial"
            elif user_article_type == "comparison":
                state["content_type"] = "comparison"
            else:
                state["content_type"] = "informational"
        else:
            if any(x in intent_normalized for x in ["commercial", "transactional"]):
                state["content_type"] = "brand_commercial"
            elif any(x in intent_normalized for x in ["comparison", "comparative"]):
                state["content_type"] = "comparison"
            else:
                # 5. Secondary Guard: If brand exists, it is Commercial
                brand_name = state.get("brand_name")
                if brand_name and brand_name.lower() not in ["not provided", "none", ""]:
                    state["content_type"] = "brand_commercial"
                else:
                    state["content_type"] = "informational"
        
        logger.info(f"Strategic Decision: TitleIntent='{intent_normalized}', ClassifierIntent='{detected_intent}' -> Final='{state['content_type']}'")

        state["input_data"]["title"] = optimized_title
        return state
        
        # Skip the redundant classifier step if we already deterministically mapped the type via Advanced Mode
        if not (state.get("workflow_mode") == "advanced" and user_article_type):
            await self.detect_intent_ai(raw_title, primary_keyword, state=state)
        
        return state

    async def run_style_analysis(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Analyzes the reference article/image to determine the brand's style."""
        input_data = state.get("input_data", {})
        ref_path = input_data.get("logo_reference_path")
        style_ref = input_data.get("style_reference")
        
        state["brand_visual_style"] = ""
        state["style_blueprint"] = {}

        # 1. Structural/Writing Style Analysis (from Article Reference)
        if style_ref:
            logger.info("Analyzing style reference article...")
            blueprint = await self.style_extractor.extract_blueprint(style_ref)
            state["style_blueprint"] = blueprint
            logger.info(f"Style Blueprint extracted: {list(blueprint.keys())}")

        # 2. Visual Style Analysis (from Logo Reference)
        if ref_path and isinstance(ref_path, str) and os.path.exists(ref_path):
            try:
                style_res = await self.ai_client.describe_image_style(ref_path)
                state["brand_visual_style"] = style_res.get("content", "") if isinstance(style_res, dict) else str(style_res)
            except Exception as e:
                logger.error(f"Failed to analyze reference image: {e}")
                state["brand_visual_style"] = "Professional, modern corporate identity"

        return state

    def _get_static_core_strategy(self, primary_keyword: str, content_type: str, area: str) -> Dict[str, Any]:
        return self._normalize_content_strategy({}, primary_keyword, content_type, area)

    async def run_content_strategy(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Step 0: Develop the content strategy based on SERP analysis and intent."""
        primary_keyword = state.get("primary_keyword")
        intent = state.get("intent")
        seo_intelligence = state.get("seo_intelligence", {})
        content_type = state.get("content_type")
        area = state.get("area") or "Global"
        
        full_intel = seo_intelligence.get("market_analysis", {})

        intent_layer = full_intel.get("intent_analysis", {})
        structural_layer = full_intel.get("structural_intelligence", {})
        market_insights = full_intel.get("market_insights", {})

        clusters = market_insights.get("keyword_clusters", [])
        if not clusters:
            semantic = full_intel.get("semantic_assets", {})
            lsi = semantic.get("lsi_keywords", [])
            related = semantic.get("related_searches", [])
            
            raw_fallback = [primary_keyword] + lsi[:5] + related[:5]
            safe_fallback = []
            for kw in raw_fallback:
                if isinstance(kw, dict):
                    safe_kw = kw.get("keyword") or kw.get("text", str(kw))
                    safe_fallback.append(str(safe_kw))
                else:
                    safe_fallback.append(str(kw))

            clusters = [{
                "cluster_name": "Semantic Keywords Cluster (Safety Fallback)",
                "keywords": list(dict.fromkeys(safe_fallback))
            }]

        template_name = self.strategy_map.get(
            content_type,
            self.strategy_map["informational"]
        )
        template = self.env.get_template(template_name)

        prompt = template.render(
            primary_keyword=primary_keyword,
            intent=intent,
            serp_intent_analysis=json.dumps(intent_layer),
            serp_structural_intelligence=json.dumps(structural_layer),
            serp_market_insights=json.dumps(market_insights),
            keyword_clusters=json.dumps(clusters),
            content_type=content_type,
            area=area,
            prohibited_competitors=state.get("prohibited_competitors", [])
        )

        final_data = None
        for attempt in range(3):
            res = await self.ai_client.send(prompt, step="content_strategy")
            raw = res["content"]
            metadata = res["metadata"]

            if state.get("workflow_logger"):
                state["workflow_logger"].log_ai_call(
                    step_name="content_strategy",
                    prompt=metadata.get("prompt"),
                    response=raw,
                    tokens=metadata.get("tokens"),
                    duration=metadata.get("duration", 0)
                )

            state["last_step_prompt"] = metadata["prompt"]
            state["last_step_response"] = metadata["response"]
            state["last_step_tokens"] = metadata["tokens"]
            state["last_step_model"] = metadata.get("model", "unknown")

            if not raw:
                logger.error("Content Strategy AI returned empty response")
                state["content_strategy"] = {}
                return state
            
            json_text = self._extract_first_json_object(raw)
            parsed = recover_json(json_text)

            if isinstance(parsed, dict) and parsed:
                normalized = self._normalize_content_strategy(
                    parsed, primary_keyword, content_type, area, seo_intelligence=seo_intelligence
                )
                if self._is_valid_content_strategy(normalized):
                    final_data = normalized
                    break

            logger.warning(f"Content Strategy invalid on attempt {attempt+1}/3. Retrying...")
            await asyncio.sleep(0.5)

        if final_data is None:
            logger.error("Content Strategy failed after retries. Using deterministic fallback.")
            final_data = self._normalize_content_strategy(
                {}, primary_keyword, content_type, area, seo_intelligence=seo_intelligence
            )

        state["content_strategy"] = final_data
        return state

    async def detect_intent_ai(self, raw_title: str, primary_keyword: str, state: Dict[str, Any] = None) -> str:
        """AI classifier to detect intent (informational, commercial, etc.) using strategic JSON logic."""
        from datetime import datetime
        import json
        import re

        if not hasattr(self, 'intent_template'):
            return "informational"
             
        prompt = self.intent_template.render(
            raw_title=raw_title,
            primary_keyword=primary_keyword,
            brand_name=state.get("brand_name", "Not provided") if state else "Not provided",
            current_year=str(datetime.now().year)
        )

        res = await self.ai_client.send(prompt, step="intent")
        content = res["content"]

        # Extract JSON from potential Markdown blocks
        try:
            json_str = re.search(r'\{.*\}', content, re.DOTALL).group(0)
            data = recover_json(json_str)
            if not data:
                # If extraction failed, try recovering from the full content
                data = recover_json(content)
            
            intent = (data or {}).get("intent", "informational").lower().strip()
            reasoning = (data or {}).get("reasoning", "")
            logger.info(f"[Intent_Intelligence] Classified as '{intent}' because: {reasoning}")
        except Exception as e:
            logger.warning(f"Failed to parse strategic intent JSON, falling back to raw: {e}")
            intent = content.strip().lower()

        if state is not None:
             state["last_step_prompt"] = res["metadata"]["prompt"]
             state["last_step_response"] = res["metadata"]["response"]
             state["last_step_tokens"] = res["metadata"]["tokens"]
             state["last_step_model"] = res["metadata"].get("model", "unknown")
             # NEW: Store the detected intent in state for the workflow router
             state["intent"] = intent
            
        return intent

    def _normalize_token(self, value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "").strip().lower())

    def _keyword_supports_heavy_framing(self, primary_keyword: str, seo_intelligence: Optional[Dict[str, Any]] = None) -> bool:
        keyword_norm = self._normalize_token(primary_keyword)
        heavy_terms = INVESTMENT_HEAVY_PHRASES + LEGAL_HEAVY_PHRASES
        if any(term in keyword_norm for term in heavy_terms):
            return True

        market_analysis = (seo_intelligence or {}).get("market_analysis", {}) if isinstance(seo_intelligence, dict) else {}
        market_insights = market_analysis.get("market_insights", {}) if isinstance(market_analysis, dict) else {}
        observations = market_insights.get("topic_observations", {}) if isinstance(market_insights, dict) else {}

        for bucket_name in ("core_recurring_topics", "secondary_mentions"):
            for topic in observations.get(bucket_name, []) or []:
                topic_text = self._normalize_token(topic.get("topic", ""))
                frequency = int(topic.get("frequency", 0) or 0)
                confidence = self._normalize_token(topic.get("confidence", ""))
                if any(term in topic_text for term in heavy_terms) and (frequency >= 2 or confidence == "high"):
                    return True

        return False

    def _derive_head_entity(self, primary_keyword: str, area: str = "") -> str:
        return self._derive_entity_terms(primary_keyword, area).get("head", "")

    def _normalize_arabic(self, text: str) -> str:
        if not text: return ""
        replacements = {"أ": "ا", "إ": "ا", "آ": "ا", "ة": "ه", "ى": "ي", "ئ": "ء", "ؤ": "ء"}
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text.lower()

    def _derive_entity_terms(self, primary_keyword: str, area: str = "") -> Dict[str, str]:
        text = str(primary_keyword or "").strip()
        if not text:
            return {"head": "", "phrase": ""}

        if area:
            text = re.sub(re.escape(area), " ", text, flags=re.IGNORECASE)

        tokens = re.findall(r"[\w\u0600-\u06FF]+", text, re.UNICODE)
        normalized_tokens = [self._normalize_arabic(token) for token in tokens]
        
        stop_tokens = {
            "for", "sale", "buy", "buying", "in", "vs", "best", "top", "cheap", "cheapest",
            "what", "how", "guide", "review", "compare", "comparison", "near", "new",
            "في", "فى", "للبيع", "شراء", "مقارنة", "افضل", "أفضل", "ارخص", "أرخص", "دليل",
            "ما", "كيف", "هل", "سعر", "اسعار", "أسعار",
        }
        strong_property_heads = {
            "شقه", "شقق", "عقار", "عقارات", "وحده", "وحدات", "محل", "محلات",
            "فيلا", "فلل", "فيلات", "شاليه", "شاليهات", "ارض", "اراضي",
            "apartment", "apartments", "flat", "flats", "villa", "villas", "chalet", "chalets",
            "shop", "shops", "store", "stores", "land", "lands", "plot", "plots",
        }
        ambiguous_property_heads = {"مكتب", "مكاتب", "office", "offices"}
        intent_tokens = {
            "بيع", "للبيع", "شراء", "ايجار", "للايجار", "استئجار", "حجز", "للحجز",
            "sale", "rent", "rental", "booking", "book",
        }
        boundary_tokens = {"في", "فى", "in", "near", "vs", "مقارنة", "مقارنه"}
        compound_service_heads = {"شركه", "مكتب", "عياده", "مركز", "وكاله", "مؤسسه", "منصه", "خدمه", "خدمات"}

        head = ""
        head_index = -1
        for idx, normalized in enumerate(normalized_tokens):
            if normalized and normalized not in stop_tokens:
                head = tokens[idx]
                head_index = idx
                break

        if not head:
            fallback = tokens[0] if tokens else text
            return {"head": fallback, "phrase": fallback}

        phrase_tokens = [head]
        normalized_head = self._normalize_arabic(head)
        has_property_intent = any(token in intent_tokens for token in normalized_tokens)
        is_property_like = normalized_head in strong_property_heads or (
            normalized_head in ambiguous_property_heads and has_property_intent
        )

        for idx in range(head_index + 1, len(tokens)):
            token = tokens[idx]
            normalized = normalized_tokens[idx]
            if not normalized:
                continue
            if normalized in boundary_tokens or re.fullmatch(r"\d{4}", normalized):
                break

            if is_property_like:
                break

            phrase_tokens.append(token)

        phrase = " ".join(phrase_tokens).strip() or head
        return {"head": head, "phrase": phrase}

    def _build_brand_market_angle(self, primary_keyword: str, area: str) -> str:
        entity = self._derive_entity_terms(primary_keyword, area).get("phrase") or primary_keyword
        place = area or "the target area"
        return (
            f"Help the reader compare {entity} in {place} by practical decision factors "
            f"such as available options, fit, price or value, proof, and the clearest next step."
        )

    def _build_brand_primary_angle(self, primary_keyword: str, area: str) -> str:
        entity = self._derive_entity_terms(primary_keyword, area).get("phrase") or primary_keyword
        place = area or "the target area"
        return (
            f"Help the reader decide how to compare and choose {entity} in {place} "
            f"based on practical buying factors."
        )

    def _build_brand_conversion_strategy(self) -> str:
        return (
            "Clarify the offer -> show buyer-facing features -> provide practical proof "
            "-> help compare real options -> reduce friction in the buying path -> "
            "answer objections -> close with a confident final CTA."
        )

    def _build_brand_local_strategy(self, primary_keyword: str, area: str) -> str:
        entity = self._derive_entity_terms(primary_keyword, area).get("phrase") or primary_keyword
        place = area or "the target area"
        return (
            f"Keep local references focused on {place} only when they help the reader "
            f"compare, choose, or buy {entity} more confidently."
        )

    def _build_brand_emotional_trigger(self) -> str:
        return "Confidence from understanding the options clearly and avoiding the wrong fit."

    def _contains_forbidden_strategy_phrase(self, text: str, allow_heavy_framing: bool = False) -> bool:
        normalized = self._normalize_token(text)
        phrases = list(STRATEGY_UNSAFE_PHRASES)
        if not allow_heavy_framing:
            phrases += INVESTMENT_HEAVY_PHRASES + LEGAL_HEAVY_PHRASES
        return any(phrase in normalized for phrase in phrases)

    def _sanitize_brand_strategy_list(self, values: Any, allow_heavy_framing: bool = False) -> List[str]:
        if not isinstance(values, list):
            return []

        sanitized = []
        for value in values:
            text = str(value or "").strip()
            if not text:
                continue
            if self._contains_forbidden_strategy_phrase(text, allow_heavy_framing=allow_heavy_framing):
                continue
            sanitized.append(text)
        return sanitized

    def _sanitize_brand_scalar(
        self,
        value: Any,
        fallback: str = "",
        allow_heavy_framing: bool = False,
    ) -> str:
        text = str(value or "").strip()
        if not text:
            return fallback
        if self._contains_forbidden_strategy_phrase(text, allow_heavy_framing=allow_heavy_framing):
            return fallback
        return text

    def _brand_commercial_defaults(self, primary_keyword: str, area: str) -> Dict[str, Any]:
        return {
            "primary_angle": self._build_brand_primary_angle(primary_keyword, area),
            "market_angle": self._build_brand_market_angle(primary_keyword, area),
            "target_reader_state": LOCKED_BRAND_TARGET_READER_STATE,
            "pain_point_focus": [],
            "emotional_trigger": self._build_brand_emotional_trigger(),
            "depth_level": "comprehensive",
            "authority_strategy": [],
            "eeat_signals_to_include": [],
            "differentiation_focus": [],
            "conversion_strategy": self._build_brand_conversion_strategy(),
            "cta_philosophy": LOCKED_BRAND_CTA_PHILOSOPHY,
            "local_strategy": self._build_brand_local_strategy(primary_keyword, area),
            "cultural_peer_areas": [],
            "tone_direction": LOCKED_BRAND_TONE_DIRECTION,
            "section_role_map": dict(LOCKED_BRAND_SECTION_ROLE_MAP),
        }

    def _apply_brand_commercial_contract(
        self,
        strategy: Dict[str, Any],
        primary_keyword: str,
        area: str,
        seo_intelligence: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        contracted = dict(strategy)
        defaults = self._brand_commercial_defaults(primary_keyword, area)
        allow_heavy_framing = self._keyword_supports_heavy_framing(primary_keyword, seo_intelligence)

        contracted["target_reader_state"] = defaults["target_reader_state"]
        contracted["tone_direction"] = defaults["tone_direction"]
        contracted["cta_philosophy"] = defaults["cta_philosophy"]
        contracted["section_role_map"] = dict(defaults["section_role_map"])
        contracted["depth_level"] = defaults["depth_level"]
        contracted["cultural_peer_areas"] = []
        contracted["market_angle"] = defaults["market_angle"]
        contracted["local_strategy"] = self._sanitize_brand_scalar(
            contracted.get("local_strategy"),
            fallback=defaults["local_strategy"],
            allow_heavy_framing=allow_heavy_framing,
        )
        contracted["emotional_trigger"] = self._sanitize_brand_scalar(
            contracted.get("emotional_trigger"),
            fallback=defaults["emotional_trigger"],
            allow_heavy_framing=allow_heavy_framing,
        )

        candidate_primary_angle = contracted.get("primary_angle", "")
        entity = self._derive_entity_terms(primary_keyword, area).get("phrase") or self._derive_head_entity(primary_keyword, area)
        area_present = not area or area in str(candidate_primary_angle)
        entity_present = not entity or entity in str(candidate_primary_angle)
        decision_present = any(
            token in self._normalize_token(candidate_primary_angle)
            for token in ("decide", "compare", "choose", "buy")
        )
        if (
            not candidate_primary_angle
            or not area_present
            or not entity_present
            or not decision_present
            or self._contains_forbidden_strategy_phrase(candidate_primary_angle, allow_heavy_framing=allow_heavy_framing)
        ):
            contracted["primary_angle"] = defaults["primary_angle"]
        else:
            contracted["primary_angle"] = str(candidate_primary_angle).strip()

        candidate_conversion = contracted.get("conversion_strategy", "")
        required_markers = ("offer", "features", "proof", "compare", "buying", "objection", "cta")
        conversion_normalized = self._normalize_token(candidate_conversion)
        if (
            not candidate_conversion
            or self._contains_forbidden_strategy_phrase(candidate_conversion, allow_heavy_framing=allow_heavy_framing)
            or not all(marker in conversion_normalized for marker in required_markers)
        ):
            contracted["conversion_strategy"] = defaults["conversion_strategy"]
        else:
            contracted["conversion_strategy"] = str(candidate_conversion).strip()

        contracted["pain_point_focus"] = self._sanitize_brand_strategy_list(
            contracted.get("pain_point_focus"), allow_heavy_framing=allow_heavy_framing
        )
        contracted["authority_strategy"] = self._sanitize_brand_strategy_list(
            contracted.get("authority_strategy"), allow_heavy_framing=allow_heavy_framing
        )
        contracted["eeat_signals_to_include"] = self._sanitize_brand_strategy_list(
            contracted.get("eeat_signals_to_include"), allow_heavy_framing=allow_heavy_framing
        )
        contracted["differentiation_focus"] = self._sanitize_brand_strategy_list(
            contracted.get("differentiation_focus"), allow_heavy_framing=allow_heavy_framing
        )

        return contracted

    def _normalize_content_strategy(
        self,
        data: Dict[str, Any],
        primary_keyword: str,
        content_type: str,
        area: str,
        seo_intelligence: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        defaults = {
            "primary_angle": f"{primary_keyword} with performance-first execution",
            "market_angle": "Practical, conversion-focused, locally adapted",
            "target_reader_state": "Comparing providers and ready to shortlist",
            "pain_point_focus": [],
            "emotional_trigger": "Fear of losing leads due to weak digital presence",
            "depth_level": "comprehensive",
            "authority_strategy": [],
            "eeat_signals_to_include": [],
            "differentiation_focus": [],
            "conversion_strategy": "Intro CTA bridge -> proof -> close CTA",
            "cta_philosophy": "One clear CTA early, one decisive CTA in conclusion",
            "local_strategy": f"Reflect market behavior, trust factors, and payment context in {area}" if area else "No local constraint",
            "cultural_peer_areas": [],
            "tone_direction": "Confident, direct, benefit-led",
            "section_role_map": {
                "introduction": "Hook with local market urgency + primary CTA",
                "core_or_benefits": "Show service value and business outcomes",
                "proof": "Use metrics, case-style evidence, trust signals",
                "process_or_how": "Clear implementation path and delivery model",
                "faq": "Handle objections and clarify buying concerns",
                "conclusion": "Reinforce value + final strong CTA"
            }
        }
        if content_type == "brand_commercial":
            defaults = self._brand_commercial_defaults(primary_keyword, area)

        out = defaults.copy()
        if isinstance(data, dict):
            out.update(data)

        for list_key in ["pain_point_focus", "authority_strategy", "eeat_signals_to_include", "differentiation_focus"]:
            if not isinstance(out.get(list_key), list):
                out[list_key] = []

        if not isinstance(out.get("section_role_map"), dict):
            out["section_role_map"] = defaults["section_role_map"]
        else:
            # Deep merge role map
            out["section_role_map"] = {**defaults["section_role_map"], **out["section_role_map"]}

        if content_type == "brand_commercial":
            out = self._apply_brand_commercial_contract(
                out,
                primary_keyword,
                area,
                seo_intelligence=seo_intelligence,
            )

        return out

    def _is_valid_content_strategy(self, data: Dict[str, Any]) -> bool:
        required = [
            "primary_angle", "market_angle", "target_reader_state",
            "pain_point_focus", "emotional_trigger", "depth_level",
            "authority_strategy", "eeat_signals_to_include", "differentiation_focus",
            "conversion_strategy", "cta_philosophy", "local_strategy", "cultural_peer_areas",
            "tone_direction", "section_role_map"
        ]
        if not isinstance(data, dict) or not data:
            return False
        return all(k in data for k in required)

    def _extract_first_json_object(self, text: str) -> str:
        if not text:
            return ""
        cleaned = re.sub(r"```json|```", "", text, flags=re.IGNORECASE).strip()
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return cleaned
        return cleaned[start:end+1]
