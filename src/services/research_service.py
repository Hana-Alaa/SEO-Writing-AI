import os
import logging
import json
import asyncio
import shutil
import uuid
import re
import hashlib
import requests
import httpx
from typing import Dict, Any, List, Optional
from collections import Counter
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from PIL import Image
from io import BytesIO
from jinja2 import Template

from src.utils.link_manager import LinkManager
from src.utils.json_utils import recover_json
from src.utils.scraper_utils import ScraperUtils
from src.services.serp_topic_miner import SERPTopicMiner

logger = logging.getLogger(__name__)

class ResearchService:
    """Service dedicated to brand discovery, web research, and SERP analysis."""

    def __init__(self, ai_client, work_dir: str):
        self.ai_client = ai_client
        self.work_dir = work_dir
        self.upload_dir = os.path.join(work_dir, "uploads")
        os.makedirs(self.upload_dir, exist_ok=True)
        self.topic_miner = SERPTopicMiner()

    def _compose_search_query(self, primary_keyword: str, area: Optional[str], lang: str) -> str:
        """Build a clean search query without duplicating the area phrase."""
        keyword = re.sub(r"\s+", " ", (primary_keyword or "")).strip()
        area_text = re.sub(r"\s+", " ", (area or "")).strip()
        if not area_text:
            return keyword

        if area_text.lower() in keyword.lower():
            return keyword

        in_map = {"ar": "في", "en": "in", "fr": "en", "es": "en", "de": "in"}
        in_word = in_map.get(lang, "in")
        return f"{keyword} {in_word} {area_text}".strip()

    def _humanize_domain_brand(self, url: str) -> str:
        root = (LinkManager.domain(url) or "").split(".")[0].strip().lower()
        if not root:
            return "The Brand"

        root = re.sub(r"[_\-]+", " ", root)
        for suffix in ("host", "stay", "rent", "home", "booking", "travel", "group"):
            if root.endswith(suffix) and len(root) > len(suffix) + 2 and " " not in root:
                root = f"{root[:-len(suffix)]} {suffix}"
                break

        return " ".join(part.capitalize() for part in root.split())

    def _brand_candidate_score(self, candidate: str, brand_url: str, primary_keyword: str = "") -> int:
        normalized = re.sub(r"\s+", " ", (candidate or "")).strip()
        if not normalized:
            return -999

        lowered = normalized.lower()
        domain_root = (LinkManager.domain(brand_url) or "").split(".")[0].lower()
        collapsed = re.sub(r"[\s_\-]+", "", lowered)
        pk_lower = (primary_keyword or "").strip().lower()

        marketing_verbs = {"احجز", "book", "reserve", "rent", "find", "search", "browse", "discover"}
        generic_labels = {"home", "homepage", "الرئيسية", "home page"}
        property_terms = {
            "شقق", "شاليهات", "فلل", "عقارات", "وحدات", "apartments", "villas", "chalets",
            "properties", "units", "rent", "sale", "للإيجار", "للايجار", "للبيع",
        }

        words = normalized.split()
        score = 0

        if domain_root and domain_root in collapsed:
            score += 30
        if 1 <= len(words) <= 4:
            score += 20
        if len(normalized) <= 28:
            score += 10
        if lowered in generic_labels:
            score -= 40
        if pk_lower and pk_lower in lowered:
            score -= 35
        if words and words[0].lower() in marketing_verbs:
            score -= 30

        property_hits = sum(1 for term in property_terms if term.lower() in lowered)
        if property_hits >= 2:
            score -= 35
        elif property_hits == 1:
            score -= 10

        if len(words) >= 6 or len(normalized) > 40:
            score -= 30

        return score

    def _aggregate_serp_structural_stats(self, serp_data: Dict[str, Any]) -> Dict[str, Any]:
        """Computes deterministic stats from observed headings in top_results."""
        top_results = serp_data.get("top_results", [])
        total_h2 = 0
        total_h3 = 0
        valid_results_count = 0
        observed_word_counts = []
        
        for res in top_results:
            h2_count = 0
            h3_count = 0
            
            if isinstance(res, dict):
                word_count = res.get("estimated_word_count")
                if isinstance(word_count, (int, float)) and word_count > 0:
                    observed_word_counts.append(int(word_count))

                if "headings" in res and isinstance(res["headings"], dict):
                    h2_count = len(res["headings"].get("h2", []))
                    h3_count = len(res["headings"].get("h3", []))
                elif "structure" in res and isinstance(res["structure"], list):
                    h2_count = sum(1 for h in res["structure"] if h.get("tag") == "H2")
                    h3_count = sum(1 for h in res["structure"] if h.get("tag") == "H3")
            
            if h2_count > 0 or h3_count > 0:
                total_h2 += h2_count
                total_h3 += h3_count
                valid_results_count += 1
        
        avg_h2 = round(total_h2 / valid_results_count, 1) if valid_results_count > 0 else 0
        avg_h3 = round(total_h3 / valid_results_count, 1) if valid_results_count > 0 else 0
        avg_word_count = (
            round(sum(observed_word_counts) / len(observed_word_counts), 1)
            if observed_word_counts
            else None
        )
        
        return {
            "avg_h2_count": avg_h2,
            "avg_h3_count": avg_h3,
            "total_h2_count": total_h2,
            "total_h3_count": total_h3,
            "heading_data_missing": valid_results_count == 0,
            "avg_word_count": avg_word_count,
            "word_count_data_missing": not bool(observed_word_counts),
            "avg_word_count_reliable": bool(observed_word_counts),
            "observed_word_count_results": len(observed_word_counts)
        }

    def _annotate_word_count_missing(self, serp_data: Dict[str, Any]) -> Dict[str, Any]:
        """Mark unavailable word counts explicitly instead of treating 0 as observed data."""
        for result in serp_data.get("top_results", []) or []:
            if not isinstance(result, dict):
                continue
            word_count = result.get("estimated_word_count")
            if not isinstance(word_count, (int, float)) or word_count <= 0:
                result["word_count_data_missing"] = True
                result["avg_word_count_reliable"] = False
            else:
                result["word_count_data_missing"] = False
                result["avg_word_count_reliable"] = True
        return serp_data

    def _extract_lsi_from_page_data(self, serp_data: Dict[str, Any]) -> List[str]:
        """Extracts repeated phrases from observed headings, titles, and snippets."""
        text_corpus = []
        top_results = serp_data.get("top_results", [])
        
        for res in top_results:
            if not isinstance(res, dict): continue
            
            text_corpus.append(res.get("title") or "")
            text_corpus.append(res.get("meta_title") or "")
            text_corpus.append(res.get("meta_description") or "")
            text_corpus.append(res.get("snippet") or "")
            
            headings = res.get("headings", {})
            if isinstance(headings, dict):
                h1 = headings.get("h1")
                if isinstance(h1, list): text_corpus.extend(h1)
                elif h1: text_corpus.append(h1)
                text_corpus.extend(headings.get("h2", []))
                text_corpus.extend(headings.get("h3", []))
            
            structure = res.get("structure", [])
            if isinstance(structure, list):
                text_corpus.extend([h.get("text", "") for h in structure if h.get("text")])

        phrases = []
        for text in text_corpus:
            if not text or len(text) < 10: continue
            cleaned = re.sub(r'[^\w\s\u0600-\u06FF]', ' ', text.lower())
            parts = [p.strip() for p in cleaned.split() if len(p.strip()) > 2]
            
            for i in range(len(parts) - 1):
                phrases.append(f"{parts[i]} {parts[i+1]}")
            for i in range(len(parts) - 2):
                phrases.append(f"{parts[i]} {parts[i+1]} {parts[i+2]}")

        counts = Counter(phrases)
        lsi = [phrase for phrase, count in counts.most_common(40) if count >= 2 and len(phrase) > 10]
        return list(dict.fromkeys(lsi))[:15]

    def _sanitize_lsi_keywords(self, serp_data: Dict[str, Any], state: Optional[Dict[str, Any]] = None) -> List[str]:
        """Remove competitor/brand leakage while preserving domain-relevant service terms."""
        raw_keywords = serp_data.get("lsi_keywords") or []
        if not isinstance(raw_keywords, list):
            return []

        competitor_roots = set()
        for result in serp_data.get("top_results", []) or []:
            if not isinstance(result, dict):
                continue
            domain = LinkManager.domain(result.get("url", ""))
            if domain:
                root = domain.split(".")[0].lower()
                if root:
                    competitor_roots.add(root)

        brand_terms = set()
        if state:
            for value in [
                state.get("brand_name"),
                state.get("display_brand_name"),
                state.get("official_brand_name"),
                state.get("domain_brand_name"),
                *(state.get("brand_aliases") or []),
            ]:
                if isinstance(value, str) and value.strip():
                    brand_terms.add(value.strip().lower())

        service_tokens = {
            "تصميم", "مواقع", "موقع", "برمجة", "تطوير", "سيو", "تقني",
            "تحسين", "محركات", "البحث", "تسويق", "رقمي", "استضافة",
            "web", "design", "development", "seo", "marketing", "hosting",
        }
        brand_suffixes = ("للبرمجيات", "للتقنية", "للخدمات", "للتسويق", "للحلول")

        cleaned = []
        seen = set()
        for item in raw_keywords:
            text = str(item or "").strip()
            if not text:
                continue
            normalized = re.sub(r"[_\-]+", " ", text)
            normalized = re.sub(r"\s+", " ", normalized).strip()
            lowered = normalized.lower()
            collapsed = re.sub(r"\s+", "", lowered)
            tokens = [token for token in re.split(r"\s+", lowered) if token]

            if lowered in brand_terms or collapsed in {re.sub(r'\s+', '', term) for term in brand_terms}:
                continue
            if any(root and (root in lowered or root in collapsed) for root in competitor_roots):
                continue
            if any(lowered.endswith(suffix) for suffix in brand_suffixes):
                first_token = tokens[0] if tokens else ""
                if first_token and first_token not in service_tokens:
                    continue
            if "_" in text:
                non_service_tokens = [token for token in tokens if token not in service_tokens]
                if len(non_service_tokens) >= 1 and any(token.startswith("لل") for token in tokens):
                    continue

            if lowered not in seen:
                seen.add(lowered)
                cleaned.append(normalized)

        return cleaned

    def _enrich_serp_enrichment_signals(self, serp_data: Dict[str, Any], state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Processes enrichment signals with strict source labeling.
        Accepts AI-provided sources if present, otherwise calculates them.
        """
        provided_sources = serp_data.get("serp_enrichment_sources")
        if isinstance(provided_sources, dict) and any(provided_sources.values()):
            # Use AI-provided sources as primary source of truth
            sources = provided_sources
        else:
            sources = {
                "paa_questions": "not_observed",
                "related_searches": "not_observed",
                "autocomplete_suggestions": "not_observed",
                "lsi_keywords": "not_observed"
            }
        
        paa = serp_data.get("paa_questions")
        if paa and isinstance(paa, list) and len(paa) > 0:
            if sources.get("paa_questions") in ("not_observed", ""):
                sources["paa_questions"] = "google_serp"
        
        related = serp_data.get("related_searches")
        if related and isinstance(related, list) and len(related) > 0:
            if sources.get("related_searches") in ("not_observed", ""):
                sources["related_searches"] = "google_serp"
        
        auto = serp_data.get("autocomplete_suggestions")
        if auto and isinstance(auto, list) and len(auto) > 0:
            if sources.get("autocomplete_suggestions") in ("not_observed", ""):
                sources["autocomplete_suggestions"] = "google_autocomplete"
        
        lsi = serp_data.get("lsi_keywords")
        if lsi and isinstance(lsi, list) and len(lsi) > 0:
            if sources.get("lsi_keywords") in ("not_observed", ""):
                sources["lsi_keywords"] = "google_serp"
        else:
            extracted_lsi = self._extract_lsi_from_page_data(serp_data)
            if extracted_lsi:
                serp_data["lsi_keywords"] = extracted_lsi
                sources["lsi_keywords"] = "page_content"

        serp_data["lsi_keywords"] = self._sanitize_lsi_keywords(serp_data, state=state)

        serp_data["serp_enrichment_sources"] = sources
        return serp_data

    def _commercial_intent_floor_applies(self, primary_keyword: str) -> bool:
        normalized = (primary_keyword or "").lower()
        tokens = {token for token in re.split(r"[^\w\u0600-\u06FF]+", normalized) if token}
        if not tokens: return False

        quality_signals = {"best", "top", "cheapest", "compare", "review", "reviews", "alternative", "alternatives", "افضل", "أفضل", "احسن", "أحسن", "ارخص", "أرخص", "مقارنة", "بدائل", "تقييم", "مراجعة"}
        provider_signals = {"company", "agency", "provider", "providers", "office", "clinic", "firm", "شركة", "شركات", "وكالة", "وكالات", "مزود", "مزودين", "مكتب", "عيادة", "مؤسسة", "مركز"}
        service_signals = {"service", "services", "price", "prices", "cost", "quote", "pricing", "خدمة", "خدمات", "سعر", "أسعار", "اسعار", "تكلفة", "تكلفه", "عرض", "تصميم", "تنظيف", "محاماة", "محاماه", "تسويق", "برمجة", "برمجه", "صيانة", "صيانه", "علاج", "استضافة", "استضافه"}
        informational_starters = {"ما", "ماذا", "كيف", "لماذا", "why", "what", "how"}

        quality_signals.update({"أفضل", "افضل", "أحسن", "احسن", "أرخص", "ارخص", "مقارنة"})
        provider_signals.update({"شركة", "شركات", "وكالة", "وكالات", "مزود", "مزودين", "مكتب", "عيادة", "مؤسسة", "مركز"})
        service_signals.update({"خدمة", "خدمات", "سعر", "أسعار", "اسعار", "تكلفة", "تكلفه", "عرض", "تصميم", "تنظيف", "محاماة", "محاماه", "تسويق", "برمجة", "برمجه", "صيانة", "صيانه", "علاج", "استضافة", "استضافه"})
        informational_starters.update({"ما", "ماذا", "كيف", "لماذا"})

        has_quality = bool(tokens & quality_signals)
        has_provider = bool(tokens & provider_signals)
        has_service = bool(tokens & service_signals)

        if has_quality and has_provider: return True
        if has_provider and has_service: return True
        if has_service and any(token in tokens for token in {"سعر", "أسعار", "اسعار", "تكلفة", "تكلفه", "price", "cost", "quote"}): return True
        if tokens & informational_starters and not has_provider and not has_quality: return False
        return False

    def _apply_serp_intent_firewall(self, serp_insights: Dict[str, Any], primary_keyword: str) -> Dict[str, Any]:
        if not isinstance(serp_insights, dict): serp_insights = {}
        if not self._commercial_intent_floor_applies(primary_keyword): return serp_insights

        intent_layer = serp_insights.setdefault("intent_analysis", {})
        intent_layer["confirmed_intent"] = "commercial"
        intent_layer["commercial_signal_strength"] = max(float(intent_layer.get("commercial_signal_strength") or 0.0), 0.7)
        intent_layer["informational_signal_strength"] = max(float(intent_layer.get("informational_signal_strength") or 0.0), 0.2)
        
        structural_layer = serp_insights.setdefault("structural_intelligence", {})
        if not structural_layer.get("dominant_page_type"):
            structural_layer["dominant_page_type"] = intent_layer.get("dominant_page_type") or "mixed"

        notes = serp_insights.setdefault("observed_notes", [])
        note = "Intent overridden to commercial due to explicit provider-selection keyword signals."
        if isinstance(notes, list) and note not in notes:
            notes.append(note)
        elif not isinstance(notes, list):
            serp_insights["observed_notes"] = [str(notes), note]

        return serp_insights

    def _looks_like_display_brand_name(self, candidate: str, primary_keyword: str = "") -> bool:
        normalized = re.sub(r"\s+", " ", (candidate or "")).strip()
        if not normalized: return False
        lowered = normalized.lower()
        pk_lower = (primary_keyword or "").lower()
        words = normalized.split()
        if pk_lower and pk_lower in lowered: return False
        if not (1 <= len(words) <= 4): return False
        if self._is_generic_brand_descriptor(normalized, primary_keyword): return False
        return True

    def _extract_explicit_brand_inputs(self, state: Dict[str, Any]) -> List[str]:
        input_data = state.get("input_data", {})
        urls = input_data.get("urls", [])
        preferred = []
        fallback = []
        for item in urls:
            if not isinstance(item, dict): continue
            for key in ("text", "brand_name", "name", "label", "anchor"):
                value = item.get(key)
                if isinstance(value, str):
                    cleaned = value.strip()
                    if not cleaned:
                        continue
                    if item.get("is_brand"):
                        preferred.append(cleaned)
                    else:
                        fallback.append(cleaned)
        explicit = preferred or fallback
        return list(dict.fromkeys(explicit))

    def _is_generic_brand_descriptor(self, candidate: str, primary_keyword: str = "") -> bool:
        normalized = re.sub(r"\s+", " ", (candidate or "")).strip()
        if not normalized: return True
        lowered = normalized.lower()
        tokens = [t for t in re.split(r"[^\w\u0600-\u06FF]+", lowered) if t]
        if not tokens: return True
        stop_tokens = {"the", "a", "an", "and", "for", "of", "in", "best", "top", "leading", "official", "global", "local", "modern", "افضل", "أفضل", "احسن"}
        generic_service_tokens = {"company", "agency", "service", "services", "solution", "solutions", "platform", "group", "systems", "technology", "digital", "development", "web", "design", "marketing", "software", "شركة", "وكالة", "خدمة", "حل", "منصة", "مجموعة", "تقنية", "تطوير", "تصميم", "موقع"}
        stop_tokens.update({"افضل", "أفضل", "احسن", "أحسن"})
        generic_service_tokens.update({
            "شركة", "شركات", "وكالة", "خدمة", "خدمات", "حل", "حلول", "منصة",
            "مجموعة", "تقنية", "تطوير", "تصميم", "موقع", "مواقع", "برمجة", "تسويق",
        })
        content_tokens = [t for t in tokens if t not in stop_tokens]
        if not content_tokens: return True
        keyword_tokens = {t for t in re.split(r"[^\w\u0600-\u06FF]+", (primary_keyword or "").lower()) if t and len(t) > 2}
        if all(t in generic_service_tokens or t in keyword_tokens for t in content_tokens): return True
        return False

    def _extract_mentions_heuristic(self, text: str) -> List[str]:
        if not text: return []
        phrases = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\b', text)
        counts = Counter(phrases)
        return [p for p, count in counts.most_common(5) if len(p) > 3]

    def _split_brand_candidate(self, candidate: str) -> List[Dict[str, str]]:
        """Split title-like candidates and keep acronym aliases from Name (ABC)."""
        parts = []
        for raw_part in re.split(r"\s*[|–—»]\s*", candidate or ""):
            clean = re.sub(r"\s+", " ", raw_part).strip()
            if not clean:
                continue
            match = re.search(r"^(.*?)\s*\(([^)]{1,18})\)\s*$", clean)
            if match:
                parts.append({
                    "name": match.group(1).strip(),
                    "official": clean,
                    "alias": match.group(2).strip(),
                })
            else:
                parts.append({"name": clean, "official": clean, "alias": ""})
        return parts

    def _canonicalize_brand_name(self, candidates_by_source: Dict[str, List[str]], brand_url: str, primary_keyword: str = "") -> Dict[str, Any]:
        domain_derived = self._humanize_domain_brand(brand_url)
        priority_order = ["explicit_input", "visible", "metadata", "mentions", "domain"]
        scored_candidates = []
        seen = set()
        duplicate_aliases = set()

        for source in priority_order:
            for cand in candidates_by_source.get(source, []):
                if not cand:
                    continue
                for split in self._split_brand_candidate(str(cand)):
                    name = split["name"]
                    if not name:
                        continue
                    if name.lower() in seen:
                        if split.get("alias"):
                            duplicate_aliases.add(split["alias"])
                        continue
                    seen.add(name.lower())
                    if source != "domain" and self._is_generic_brand_descriptor(name, primary_keyword):
                        continue
                    score = self._brand_candidate_score(name, brand_url, primary_keyword)
                    if source == "explicit_input": score += 160
                    elif source == "visible": score += 100
                    elif source == "metadata": score += 50
                    elif source == "mentions": score += 20
                    scored_candidates.append({
                        "name": name,
                        "official": split["official"],
                        "alias": split["alias"],
                        "source": source,
                        "score": score,
                    })

        scored_candidates.sort(key=lambda x: x["score"], reverse=True)
        best = scored_candidates[0] if scored_candidates else {
            "name": domain_derived,
            "official": domain_derived,
            "alias": "",
        }
        aliases = set()
        if best.get("alias"):
            aliases.add(best["alias"])
        aliases.update(duplicate_aliases)
        for candidate in scored_candidates[1:]:
            if candidate["name"].lower() != best["name"].lower():
                aliases.add(candidate["name"])
            if candidate.get("alias") and candidate["alias"].lower() != best["name"].lower():
                aliases.add(candidate["alias"])
        if domain_derived and domain_derived.lower() != best["name"].lower():
            aliases.add(domain_derived)
        return {
            "display_brand_name": best["name"],
            "official_brand_name": best.get("official") or best["name"],
            "brand_aliases": sorted(alias for alias in aliases if alias),
            "domain_brand_name": domain_derived
        }

    def _sanitize_brand_context(self, raw_context: str, brand_name: str, primary_keyword: str) -> str:
        return f"Official brand: {brand_name}. Use the brand as a supporting platform for {primary_keyword}. Keep the article buyer-first and entity-focused."

    def _sync_brand_state_from_sources(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Guarantee stable brand fields from explicit input without inventing a brand."""
        brand_url = state.get("brand_url")
        if not brand_url:
            return state

        primary_keyword = state.get("primary_keyword", "")
        explicit_inputs = self._extract_explicit_brand_inputs(state)
        current_candidates = [
            state.get("display_brand_name"),
            state.get("brand_name"),
            state.get("official_brand_name"),
        ]
        candidates = {
            "explicit_input": explicit_inputs,
            "visible": [value for value in current_candidates if value],
            "metadata": [],
            "mentions": [],
            "domain": [state.get("domain_brand_name") or self._humanize_domain_brand(brand_url)],
        }
        brand_data = self._canonicalize_brand_name(candidates, brand_url, primary_keyword)
        display_name = brand_data.get("display_brand_name")

        if not display_name or self._is_generic_brand_descriptor(display_name, primary_keyword):
            return state

        current_display = state.get("display_brand_name") or state.get("brand_name")
        explicit_wins = bool(explicit_inputs)
        current_missing = not current_display
        current_generic = bool(current_display) and self._is_generic_brand_descriptor(current_display, primary_keyword)
        if explicit_wins or current_missing or current_generic:
            state["display_brand_name"] = display_name
            state["official_brand_name"] = brand_data.get("official_brand_name") or display_name
            state["brand_name"] = display_name
            state["brand_aliases"] = brand_data.get("brand_aliases", [])
            state["domain_brand_name"] = brand_data.get("domain_brand_name") or self._humanize_domain_brand(brand_url)
            state["brand_source"] = "explicit_input" if explicit_inputs else "domain_fallback"

        if state.get("brand_name") and not state.get("brand_context"):
            state["brand_context"] = self._sanitize_brand_context(
                "",
                state["brand_name"],
                primary_keyword,
            )
        return state

    async def run_brand_discovery(self, state: Dict[str, Any]) -> Dict[str, Any]:
        brand_url = state.get("brand_url")
        if not brand_url: return state

        state = self._sync_brand_state_from_sources(state)
        
        # Identity Discovery
        brand_assets = await self._discover_logo_and_colors(brand_url, state)
        if brand_assets:
            brand_data = brand_assets.get("brand_data", {})
            if brand_data:
                explicit_inputs = self._extract_explicit_brand_inputs(state)
                merged = self._canonicalize_brand_name(
                    {
                        "explicit_input": explicit_inputs,
                        "visible": [brand_data.get("display_brand_name")],
                        "metadata": [brand_data.get("official_brand_name")],
                        "mentions": brand_data.get("brand_aliases", []),
                        "domain": [brand_data.get("domain_brand_name")],
                    },
                    brand_url,
                    state.get("primary_keyword", ""),
                )
                state["display_brand_name"] = merged.get("display_brand_name")
                state["official_brand_name"] = merged.get("official_brand_name")
                state["brand_name"] = state["display_brand_name"]
                state["brand_aliases"] = merged.get("brand_aliases", [])
                state["domain_brand_name"] = merged.get("domain_brand_name")
                state["brand_source"] = "explicit_input" if explicit_inputs else "brand_discovery"
                state["brand_context"] = self._sanitize_brand_context("Fact sheet", state["brand_name"], state.get("primary_keyword", ""))
        return self._sync_brand_state_from_sources(state)

    async def run_brand_discovery_light(self, state: Dict[str, Any]) -> Dict[str, Any]:
        brand_url = state.get("brand_url")
        if brand_url:
            domain_brand = self._humanize_domain_brand(brand_url)
            state["brand_name"] = domain_brand
            state["display_brand_name"] = domain_brand
            state["brand_context"] = self._sanitize_brand_context("", domain_brand, state.get("primary_keyword", ""))
        return state

    async def run_web_research(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Step 0: Perform deep web research for topic grounding."""
        # --- MOCK BYPASS ---
        if type(self.ai_client).__name__ == "MockAIClient":
            logger.info("MockAIClient detected: Skipping real web research.")
            serp_data = {
                "top_results": [{"title": "Mock Competitor 1", "url": "https://comp1.com", "snippet": "A mock snippet."}],
                "paa_questions": ["What is simulation?", "Why test SEO?"],
                "lsi_keywords": ["automated testing", "mocking", "dry run"],
                "intent": "informational"
            }
            state["serp_data"] = serp_data
            state["seo_intelligence"] = serp_data
            return state
        # -------------------
        primary_keyword = state["primary_keyword"]
        area = state.get("area")
        lang = state.get("article_language", "ar")
        search_query = self._compose_search_query(primary_keyword, area, lang)

        with open("assets/prompts/templates/seo_web_research.txt") as f:
            template = Template(f.read())

        attempts = []

        async def _do_serp_call(query: str, reason: str):
            research_prompt = template.render(primary_keyword=query)
            max_results = state.get("competitor_count", 3)
            res = await self.ai_client.send_with_web(prompt=research_prompt, max_results=max_results)
            raw = res["content"]
            metadata = res["metadata"]
            if state.get("workflow_logger"):
                state["workflow_logger"].log_ai_call(step_name="web_research", prompt=research_prompt, response=raw, tokens=metadata.get("tokens", {}), duration=metadata.get("duration", 0))
            parsed = recover_json(raw) or {}
            attempts.append({
                "web_research_attempt": len(attempts) + 1,
                "query": query,
                "reason": reason,
                "top_results_count": len(parsed.get("top_results", []) or []),
                "parsed_successfully": bool(parsed),
            })
            return parsed

        serp_data = await _do_serp_call(search_query, "primary_query")
        fallback_used = False
        if not serp_data.get("top_results") and area:
            fallback_used = True
            serp_data = await _do_serp_call(primary_keyword, "fallback_after_empty_or_unparsed_results")
        
        if not serp_data.get("top_results"):
            raise RuntimeError("SERP returned no top results")

        # Aggregate stats and enrich
        serp_data = self._annotate_word_count_missing(serp_data)
        serp_data["structural_stats"] = self._aggregate_serp_structural_stats(serp_data)
        serp_data = self._enrich_serp_enrichment_signals(serp_data, state=state)
        serp_data["web_research_attempts"] = attempts
        serp_data["fallback_search_used"] = fallback_used
        serp_data["first_query"] = search_query
        serp_data["fallback_query"] = primary_keyword if fallback_used else ""

        state["serp_data"] = serp_data
        state["web_research_attempts"] = attempts
        state["fallback_search_used"] = fallback_used
        state["seo_intelligence"] = serp_data
        return state

    async def run_hybrid_research(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Step 0: Hybrid SERP + Strategy Research."""
        primary_keyword = state["primary_keyword"]
        area = state.get("area")
        lang = state.get("article_language", "ar")
        search_query = self._compose_search_query(primary_keyword, area, lang)
        
        logger.info(f"Running Hybrid SERP+Strategy Research for: {search_query}")
        
        try:
            with open("assets/prompts/templates/seo_hybrid_research.txt") as f:
                template = Template(f.read())
        except FileNotFoundError:
            with open("assets/prompts/templates/seo_web_research.txt") as f:
                template = Template(f.read())

        research_prompt = template.render(primary_keyword=search_query)
        max_results = state.get("competitor_count", 3)
        res = await self.ai_client.send_with_web(prompt=research_prompt, max_results=max_results)
        raw = res["content"]
        metadata = res["metadata"]
        
        if state.get("workflow_logger"):
            state["workflow_logger"].log_ai_call(step_name="hybrid_research", prompt=research_prompt, response=raw, tokens=metadata.get("tokens", {}), duration=metadata.get("duration", 0))
            
        serp_data = recover_json(raw) or {}
        if not serp_data.get("top_results"):
             serp_data = {"top_results": [{"title": primary_keyword, "url": "", "snippet": "Manual Fallback"}], "intent": "informational"}

        # Aggregate stats and enrich
        serp_data = self._annotate_word_count_missing(serp_data)
        serp_data["structural_stats"] = self._aggregate_serp_structural_stats(serp_data)
        serp_data = self._enrich_serp_enrichment_signals(serp_data, state=state)

        state["serp_data"] = serp_data
        state["seo_intelligence"] = {"serp_raw": serp_data, "market_analysis": serp_data}
        return state

    async def run_serp_analysis(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Neutral market observation phase. Explicitly isolated from brand identity."""
        serp_data = state.get("serp_data", {})
        primary_keyword = state.get("primary_keyword")
        top_results = serp_data.get("top_results", [])[:3]
        
        # 1. Build Neutral SERP Payload (Client brand fields excluded)
        light_serp = {
            "paa": [q for q in serp_data.get("paa_questions", [])[:10]],
            "lsi": serp_data.get("lsi_keywords", [])[:20],
            "related": serp_data.get("related_searches", [])[:15],
            "structural_stats": serp_data.get("structural_stats", {})
        }
        
        # 2. Extract Competitor Structures (Observed Reality)
        async def fetch_headers(res):
            url = res.get("url")
            if url:
                headers = await ScraperUtils.fetch_headings_from_url(url)
                if headers: return {"url": url, "title": res.get("title"), "structure": headers}
            return None

        results = await asyncio.gather(*[fetch_headers(res) for res in top_results])
        competitor_headers = [r for r in results if r]
        
        # 3. Perform Analysis (Brand-Unaware Prompt)
        with open("assets/prompts/templates/seo_serp_analysis_observed_v2.txt") as f:
            template = Template(f.read())
        
        analysis_prompt = template.render(
            primary_keyword=primary_keyword, 
            serp_data=json.dumps(light_serp), 
            competitor_structures=competitor_headers
        )
        
        res = await self.ai_client.send(analysis_prompt, step="serp_analysis")
        serp_insights = recover_json(res["content"]) or {}
        
        # 4. Intent Firewall (Deterministic overrides via keyword signals only)
        serp_insights = self._apply_serp_intent_firewall(serp_insights, primary_keyword or "")
        
        # 4.1 Enforce Deterministic Structural Stats (Override AI hallucinations)
        intelligence = serp_insights.setdefault("structural_intelligence", {})
        if light_serp.get("structural_stats"):
            stats = light_serp["structural_stats"]
            intelligence["avg_h2_count"] = stats.get("avg_h2_count", 0)
            intelligence["avg_h3_count"] = stats.get("avg_h3_count", 0)
            intelligence["total_h2_count"] = stats.get("total_h2_count", 0)
            intelligence["total_h3_count"] = stats.get("total_h3_count", 0)
            intelligence["heading_data_missing"] = stats.get("heading_data_missing", False)
            intelligence["avg_word_count"] = stats.get("avg_word_count")
            intelligence["word_count_data_missing"] = stats.get("word_count_data_missing", True)
            intelligence["avg_word_count_reliable"] = stats.get("avg_word_count_reliable", False)

        # 5. Merge Insights (Preserving original brand state in parent object)
        serp_insights["semantic_assets"] = {k: (serp_data.get(k) or []) for k in ["paa_questions", "lsi_keywords", "related_searches", "autocomplete_suggestions"]}
        serp_insights["serp_enrichment_sources"] = serp_data.get("serp_enrichment_sources", {})
        state["seo_intelligence"] = {"serp_raw": serp_data, "market_analysis": serp_insights, "competitor_structures": competitor_headers}
        return state

    def build_serp_outline_brief(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Converts observed SERP data into a compact structural brief for heading generation."""
        seo_intelligence = state.get("seo_intelligence", {})
        serp_data = seo_intelligence.get("serp_raw", {})
        market_analysis = seo_intelligence.get("market_analysis", {})
        lang = state.get("article_language", "ar")
        
        intent_analysis = market_analysis.get("intent_analysis", {})
        structural = market_analysis.get("structural_intelligence", {})
        market_insights = market_analysis.get("market_insights", {})
        topic_obs = market_insights.get("topic_observations", {})
        
        # 1. Observed Topics
        core_topics = [t.get("topic") for t in topic_obs.get("core_recurring_topics", []) if t.get("topic")]
        secondary_topics = [t.get("topic") for t in topic_obs.get("secondary_mentions", []) if t.get("topic")]

        # TASK 1: Enrich with SERP Topic Mining
        primary_keyword = state.get("primary_keyword", "")
        brand_name = state.get("brand_name")
        mining_results = self.topic_miner.mine_topics(serp_data, primary_keyword, brand_name)
        
        mined_topics = mining_results.get("topics", [])
        mined_labels = [t["topic"] for t in mined_topics]
        secondary_phrases = mining_results.get("secondary_keyword_phrases", [])
        heading_candidates = mining_results.get("heading_candidates", [])
        mining_guidance = mining_results.get("guidance", [])
        
        # Merge topics, keeping core first, then mined, then secondary
        combined_topics = list(dict.fromkeys(core_topics + mined_labels + secondary_topics))
        
        # 2. Page Type and Intent
        observed_page_type = structural.get("dominant_page_type") or intent_analysis.get("dominant_page_type", "")
        dominant_intent = intent_analysis.get("confirmed_intent", "")
        
        # 3. Must Consider Sections (Structural grounded)
        must_consider = []
        # If it's a place/destination topic and we see practical signals in core topics
        experience_keywords = {"location", "access", "tickets", "pricing", "hours", "booking", "events", "activities", "attractions", "visitor info"}
        for topic in core_topics:
            if any(kw in topic.lower() for kw in experience_keywords):
                must_consider.append(topic)
        
        # 4. Heading Patterns
        patterns = []
        if structural.get("dominant_heading_pattern"):
            patterns.append(structural["dominant_heading_pattern"])
        
        # 5. FAQ Source Status
        paa = serp_data.get("paa_questions", [])
        related = serp_data.get("related_searches", [])
        auto = serp_data.get("autocomplete_suggestions", [])
        
        faq_status = {
            "paa_observed": bool(paa),
            "related_observed": bool(related),
            "autocomplete_observed": bool(auto)
        }
        
        # 6. Brand Utility Candidates
        brand_utility_candidates = []
        if dominant_intent == "informational" and state.get("brand_context") and brand_name:
            brand_utility_candidates = self.topic_miner.generate_brand_utility_candidates(mining_results.get("topics", []), brand_name, lang)

        # 7. Generation Guidance
        guidance = market_insights.get("writing_guide", "")
        guidance_list = [guidance] if guidance else []
        guidance_list.extend(mining_guidance)
        
        logger.debug(f"[ResearchService] Raw mining results: {mining_results}")
        
        brief = {
            "observed_page_type": observed_page_type,
            "dominant_search_intent": dominant_intent,
            "observed_heading_patterns": patterns,
            "observed_topics": combined_topics[:25],
            "secondary_keyword_phrases": secondary_phrases,
            "heading_candidates": heading_candidates,
            "brand_utility_candidates": brand_utility_candidates,
            "must_consider_sections": list(dict.fromkeys(must_consider)),
            "avoid_sections": market_insights.get("avoid_sections", []),
            "faq_source_status": faq_status,
            "heading_generation_guidance": list(dict.fromkeys(guidance_list))
        }
        
        logger.debug(f"[ResearchService] Final SERP Outline Brief: {brief}")
        return brief

    async def _discover_logo_and_colors(self, url: str, state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return None
