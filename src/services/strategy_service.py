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
                    parsed, primary_keyword, content_type, area
                )
                if self._is_valid_content_strategy(normalized):
                    final_data = normalized
                    break

            logger.warning(f"Content Strategy invalid on attempt {attempt+1}/3. Retrying...")
            await asyncio.sleep(0.5)

        if final_data is None:
            logger.error("Content Strategy failed after retries. Using deterministic fallback.")
            final_data = self._normalize_content_strategy(
                {}, primary_keyword, content_type, area
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

    def _normalize_content_strategy(self, data: Dict[str, Any], primary_keyword: str, content_type: str, area: str) -> Dict[str, Any]:
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
