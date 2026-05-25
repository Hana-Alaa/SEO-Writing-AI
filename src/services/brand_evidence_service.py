# -*- coding: utf-8 -*-
import logging
import re
import asyncio
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

_BRAND_EVIDENCE_DATE_RE = re.compile(
    r"^\s*(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2}|"
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{2,4})\s*$",
    re.IGNORECASE,
)

_BRAND_EVIDENCE_JUNK_RE = re.compile(
    r"^\s*(?:\d+\+?|projects?\s*0\+?|0\+?|check|brief|final version|"
    r"arabic|العربية|faqs?|most questions asked|subscribe newsletters?|"
    r"scroll to top|lets talk|let's talk|view project|view projects|"
    r"read more|learn more|skip to content|technologies used|our portfolio|"
    r"portfolio|our work|our success stories!?|history|our mission|our vision|"
    r"home|homepage|contact|contact us|about us|how it works!?|how it works)\s*$",
    re.IGNORECASE,
)

_BRAND_EVIDENCE_PROMO_RE = re.compile(
    r"\b(?:top[-\s]?rated|why you should choose|fast turnaround|"
    r"client[-\s]?centric|long[-\s]?term partnership|happy clients?|"
    r"winning awards?|countries served|best|leading|safest|most logical|"
    r"experienced team|innovative thinking|custom solutions|end[-\s]?to[-\s]?end services)\b",
    re.IGNORECASE,
)

_BRAND_EVIDENCE_FRAGMENT_RE = re.compile(
    r"(?:\binto\s*software\b|we turn your|we create\.?\s+we innovate|"
    r"project with the newest technologies|skip to content|creative and tale|"
    r"our latest projects|success stories|"
    r"^\s*(?:us|our|your|the|management|delivering|on time)\s*$|"
    r"^\s*s(?:\s*[-–]\s*|\s+of\s+|\s*\d|\s*$))",
    re.IGNORECASE,
)

_SERVICE_HINT_RE = re.compile(
    r"\b(?:service|services|solution|solutions|software|platform|system|systems|"
    r"design|development|marketing|branding|hosting|seo|app|apps|mobile|"
    r"e-?commerce|commerce|dashboard|erp|crm|pos|wms|integration|consulting|"
    r"management|production|photography|animation|website|websites|store|shop)\b|"
    r"(?:خدمة|خدمات|حلول|برمجيات|برنامج|منصة|نظام|أنظمة|تصميم|تطوير|برمجة|"
    r"تسويق|استضافة|تطبيق|تطبيقات|متجر|مواقع|موقع|إدارة|ادارة|هوية|محتوى)",
    re.IGNORECASE,
)

_PROCESS_HINT_RE = re.compile(
    r"\b(?:planning|consultation|design|development|execution|delivery|testing|launch|"
    r"discovery|implementation|training|support)\b|"
    r"(?:استشارة|تخطيط|تصميم|تطوير|تنفيذ|تسليم|اختبار|إطلاق|اطلاق|تدريب|دعم)",
    re.IGNORECASE,
)

_PROJECT_CONTEXT_RE = re.compile(
    r"\b(?:project|case study|client|portfolio|work|app|website|platform)\b|"
    r"(?:مشروع|عميل|تطبيق|موقع|منصة|دراسة حالة|أعمال|اعمال)",
    re.IGNORECASE,
)

_PRICING_CONTEXT_RE = re.compile(
    r"\b(?:price|pricing|package|packages|plan|plans|fee|fees|cost|starts? from|starting at|from)\b|"
    r"(?:سعر|أسعار|اسعار|تكلفة|باقات|باقة|خطة|رسوم|تبدأ من|ابتداء)",
    re.IGNORECASE,
)

_CURRENCY_RE = re.compile(
    r"(?:[$€£]\s*\d|\d[\d,.]*\s*(?:sar|usd|aed|egp|ريال|ر\.س|درهم|جنيه))",
    re.IGNORECASE,
)

_GEOGRAPHY_CONTEXT_RE = re.compile(
    r"\b(?:address|location|located in|based in|office|branch|service area|serving|serves|"
    r"available in|city|country|headquarters)\b|"
    r"(?:عنوان|موقع|يقع|تقع|مقر|فرع|فروع|نخدم|خدماتنا في|متوفر في|داخل|المدينة|الدولة)",
    re.IGNORECASE,
)

_TRUST_CONTEXT_RE = re.compile(
    r"\b(?:testimonial|testimonials|review|reviews|rating|ratings|certified|certification|"
    r"licensed|award|awards|partner|partnership|trusted by|verified|verification|iso|accredited)\b|"
    r"(?:شهادة|شهادات|آراء|اراء|تقييم|تقييمات|معتمد|مرخص|جائزة|جوائز|شريك|شراكة)",
    re.IGNORECASE,
)


def _normalize_evidence_item(value: Any) -> str:
    item = re.sub(r"\s+", " ", str(value or "")).strip(" -–—:،؛|")
    return item


def _is_structured_noise(item: str) -> bool:
    if not item:
        return True
    if len(item) < 3 or len(item) > 140:
        return True
    if _BRAND_EVIDENCE_DATE_RE.match(item) or _BRAND_EVIDENCE_JUNK_RE.match(item):
        return True
    if _BRAND_EVIDENCE_FRAGMENT_RE.search(item):
        return True
    if re.fullmatch(r"[\W_]+", item, re.UNICODE):
        return True
    return False


def _sanitize_evidence_item(
    value: Any,
    category: str = "claim",
    *,
    allow_promotional: bool = False,
) -> str:
    """Category-aware cleanup for structured evidence buckets."""
    item = _normalize_evidence_item(value)
    if _is_structured_noise(item):
        return ""

    folded = item.casefold()
    category = (category or "claim").casefold()

    if category != "snippet" and _BRAND_EVIDENCE_PROMO_RE.search(item) and not allow_promotional:
        return ""

    if category in {"service", "capability"}:
        if re.search(r"\b(?:transform your brand|our latest projects|why make|what is|degree|high school|certificates?)\b", item, re.IGNORECASE):
            return ""
        if _PRICING_CONTEXT_RE.search(item) and not _SERVICE_HINT_RE.search(item):
            return ""
        # Preserve product names without service terms, but reject obvious headings/slogans.
        if not _SERVICE_HINT_RE.search(item) and len(item.split()) > 7:
            return ""
        return item

    if category in {"project", "project_explicit"}:
        project_metadata_labels = {
            "name",
            "project name",
            "client name",
            "publish date",
            "published date",
            "publication date",
            "objective",
            "objectives",
            "creation",
            "created",
            "scope of work",
            "deliverables",
            "technology stack",
            "technologies used",
            "quality assurance",
            "details",
            "project details",
        }
        if folded in project_metadata_labels:
            return ""
        item = re.sub(
            r"^(?:name|project\s+name|client\s+name|client|case\s+study|creation|created|objective|objectives)\b\s*[:\-]?\s*",
            "",
            item,
            flags=re.IGNORECASE,
        ).strip(" :-|")
        item = re.sub(
            r"^(?:project)\s*[:\-]\s*",
            "",
            item,
            flags=re.IGNORECASE,
        ).strip(" :-|")
        folded = item.casefold()
        if not item or folded in project_metadata_labels:
            return ""
        if re.match(r"^(?:to|for|with|using|by)\s+\w+", item, re.IGNORECASE):
            return ""
        if len(item.split()) > 8:
            return ""
        if _PRICING_CONTEXT_RE.search(item) or _TRUST_CONTEXT_RE.search(item):
            return ""
        if any(noise in folded for noise in [
            "project with the newest", "our latest projects", "success stories",
            "handled with precision", "within budget", "to your satisfaction",
            "helping us build", "client success", "our client's success",
            "why you should choose", "top rated agency", "technologies used",
            "design servicesmobile", "mobile app mobile app", "seo websitesall",
        ]):
            return ""
        has_proper_name = len(re.findall(r"[A-Z][A-Za-z0-9]+", item)) >= 2
        has_explicit_project_name = (
            category == "project_explicit"
            and len(item.split()) <= 8
            and bool(re.search(r"[A-Z][A-Za-z0-9]{2,}", item))
        )
        has_arabic_name = bool(re.search(r"[\u0600-\u06FF]", item)) and len(item.split()) <= 8
        if not (_PROJECT_CONTEXT_RE.search(item) or has_proper_name or has_explicit_project_name or has_arabic_name):
            return ""
        return item

    if category == "pricing":
        if not (_PRICING_CONTEXT_RE.search(item) or _CURRENCY_RE.search(item)):
            return ""
        return item

    if category == "geography":
        # The caller must source-qualify geography. This only removes noisy labels.
        if _PRICING_CONTEXT_RE.search(item) or _SERVICE_HINT_RE.search(item):
            return ""
        return item

    if category == "trust":
        allowed_trust_labels = {
            "transparent pricing",
            "price transparency",
            "price shown",
            "verified listings",
            "clear property images",
            "visible contact method",
            "service guarantee",
            "certified credentials",
            "verified partnerships",
            "defined delivery timelines",
        }
        count_proof = re.search(
            r"\b\d+\+?\s+(?:[\w\-]+\s+){0,3}(?:projects|clients|years|customers|employees)\b",
            folded,
        )
        if folded not in allowed_trust_labels and not _TRUST_CONTEXT_RE.search(item) and not count_proof:
            return ""
        return item

    if category == "process":
        if not _PROCESS_HINT_RE.search(item):
            return ""
        return item

    if category == "cta":
        if _BRAND_EVIDENCE_JUNK_RE.match(item):
            return ""
        return item

    if category == "snippet":
        # Weak context snippets can keep positioning prose, but never become allowed claims.
        return item[:240]

    return item


def _is_noise_label(value: Any) -> bool:
    """Strict label noise guard for writer-facing evidence buckets."""
    item = _normalize_evidence_item(value)
    if _is_structured_noise(item):
        return True
    folded = item.casefold()
    generic = {
        "introduction",
        "brief",
        "technologies used",
        "portfolio",
        "our portfolio",
        "projects",
        "our projects",
        "our work",
        "case studies",
        "arabic",
        "العربية",
        "subscribe",
        "subscribe newsletters",
        "contact us",
        "about us",
        "home",
        "homepage",
        "top rated agency",
        "fast turnaround",
        "why you should choose us",
        "intosoftware",
        "lets talk",
        "let's talk",
    }
    if folded in generic:
        return True
    if re.fullmatch(r"(?:let'?s\s+talk\s*){1,5}", folded):
        return True
    if _BRAND_EVIDENCE_PROMO_RE.search(item) or _BRAND_EVIDENCE_FRAGMENT_RE.search(item):
        return True
    if re.search(r"\b(?:your vision matters|centric approach|on time|delivering|management)\b", item, re.IGNORECASE):
        return True
    return False


def _has_explicit_pricing_evidence(text: str, page_type: str = "") -> bool:
    """True only for concrete pricing/package proof, not vague pricing FAQ copy."""
    text = str(text or "")
    page_type = str(page_type or "").casefold()
    if _CURRENCY_RE.search(text):
        return True
    if page_type == "pricing" and re.search(
        r"\b(?:package|packages|plan|plans|tier|starter|growth|enterprise|pricing)\b",
        text,
        re.IGNORECASE,
    ):
        return True
    if re.search(
        r"\b(?:package|packages|plan|plans|tier)\s*[:\-]\s*(?:[$€£]?\s*\d|[A-Z][\w\s]{2,40})",
        text,
        re.IGNORECASE,
    ):
        return True
    if re.search(
        r"(?:باقة|باقات|خطة)\s*[:\-]\s*(?:\d|[\u0600-\u06FF]{2,40})",
        text,
    ):
        return True
    return False


def _extract_explicit_brand_geography(text: str, page_type: str = "") -> List[str]:
    """
    Extract brand presence geography only.

    Project metadata like "Location: Egypt Sector: ..." is intentionally not
    brand geography and must not unlock location claims.
    """
    if str(page_type or "").casefold() in {"portfolio", "projects", "case_study", "case-study"}:
        return []
    candidates: List[str] = []
    patterns = [
        r"\b(?:address|based in|located in|office in|branch in|headquarters in|service area|serving|serves)\s*:?\s*([A-Z][A-Za-z\s.'-]{2,70})",
        r"(?:عنوان|مقر|فرع|فروع|نخدم|خدماتنا في|يقع في|تقع في)\s*:?\s*([\u0600-\u06FF\s]{2,70})",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text or "", re.IGNORECASE):
            value = re.split(r"[.,;|\n]|\s+\band\b\s+", match.group(1).strip(), maxsplit=1, flags=re.IGNORECASE)[0]
            value = re.sub(
                r"\b(?:sector|audience|expertise|support|products?|services?|and status).*$",
                "",
                value,
                flags=re.IGNORECASE,
            )
            cleaned = _sanitize_evidence_item(value.strip(" -:"), category="geography")
            if cleaned and 1 <= len(cleaned.split()) <= 5:
                candidates.append(cleaned)
    return list(dict.fromkeys(candidates))[:8]


def _clean_evidence_items(
    values: Any,
    *,
    limit: int = 12,
    allow_promotional: bool = False,
    category: str = "claim",
) -> List[str]:
    """Return compact evidence labels and drop crawler/menu/promo noise."""
    if not isinstance(values, list):
        values = [values] if isinstance(values, str) else []

    cleaned: List[str] = []
    seen = set()
    for raw in values:
        item = _sanitize_evidence_item(raw, category=category, allow_promotional=allow_promotional)
        if not item:
            continue
        key = item.casefold()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(item)
        if len(cleaned) >= limit:
            break
    return cleaned


def _collect_card_values(
    cards: Any,
    keys: List[str],
    *,
    limit: int = 16,
    allow_promotional: bool = False,
    category: str = "claim",
) -> List[str]:
    """Collect cleaned values from evidence cards without mutating inputs."""
    if not isinstance(cards, list):
        return []
    values: List[str] = []
    for card in cards:
        if not isinstance(card, dict) or card.get("excluded_reason"):
            continue
        for key in keys:
            values.extend(card.get(key) or [])
    return _clean_evidence_items(values, limit=limit, allow_promotional=allow_promotional, category=category)


def build_brand_evidence_inventory(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a domain-neutral availability map for brand evidence.

    The inventory is a router/index, not a claim source. It answers whether
    page-backed categories exist and where they came from, while keeping raw
    snippets and broad card labels out of allowed claims.
    """
    cards = state.get("brand_evidence_cards")
    if cards is None:
        try:
            cards = build_brand_evidence_cards(state)
        except Exception:
            cards = []
    if not isinstance(cards, list):
        cards = []

    chunks = state.get("brand_source_chunks")
    if chunks is None:
        try:
            chunks = build_brand_source_chunks(state)
        except Exception:
            chunks = []
    if not isinstance(chunks, list):
        chunks = []

    inventory = {
        "services_available": False,
        "projects_available": False,
        "pricing_available": False,
        "process_available": False,
        "trust_available": False,
        "explicit_geography": [],
        "service_page_urls": [],
        "project_page_urls": [],
        "pricing_page_urls": [],
        "process_page_urls": [],
        "trust_page_urls": [],
        "confidence": "low",
    }

    def clean_url(value: Any) -> str:
        return str(value or "").strip()

    def add_url(bucket: str, url: Any) -> None:
        url_text = clean_url(url)
        if url_text and url_text not in inventory[bucket]:
            inventory[bucket].append(url_text)

    def page_type_of(item: Dict[str, Any]) -> str:
        url = clean_url(item.get("url") or item.get("link"))
        title = str(item.get("title") or item.get("page_title") or item.get("heading") or "")
        headings = item.get("headings") if isinstance(item.get("headings"), list) else [item.get("heading")]
        url_classified = classify_page_type(url, title, headings)
        if url_classified != "other":
            return url_classified
        return str(item.get("page_type") or "").strip().lower()

    def card_url(card: Dict[str, Any]) -> str:
        return clean_url(card.get("url") or card.get("link"))

    def chunk_url(chunk: Dict[str, Any]) -> str:
        return clean_url(chunk.get("url") or chunk.get("link"))

    def chunk_text(chunk: Dict[str, Any]) -> str:
        return "\n".join(
            str(part or "")
            for part in [
                chunk.get("heading"),
                chunk.get("page_title"),
                chunk.get("text"),
                chunk.get("body_text"),
            ]
            if part
        )

    def card_text(card: Dict[str, Any]) -> str:
        pieces: List[str] = []
        for key in [
            "title",
            "page_type",
            "headings",
            "visible_products_or_services",
            "visible_features_or_capabilities",
            "visible_project_or_case_study_examples",
            "visible_process_steps",
            "visible_pricing_or_packages",
            "visible_trust_signals",
            "visible_geography",
            "usable_snippets",
        ]:
            value = card.get(key)
            if isinstance(value, list):
                pieces.extend(str(item or "") for item in value)
            elif value:
                pieces.append(str(value))
        return "\n".join(pieces)

    chunks_by_url: Dict[str, List[Dict[str, Any]]] = {}
    for chunk in chunks:
        if isinstance(chunk, dict):
            chunks_by_url.setdefault(chunk_url(chunk), []).append(chunk)

    def raw_text_for_url(url: str) -> str:
        return "\n".join(chunk_text(chunk) for chunk in chunks_by_url.get(url, []))

    def has_raw_pricing_evidence(text: str, page_type: str) -> bool:
        if page_type == "pricing":
            return True
        if _CURRENCY_RE.search(text):
            return True
        if re.search(r"\b(?:package|packages|plan|plans)\s*[:\-]\s*\w", text, re.IGNORECASE):
            return True
        if re.search(r"(?:Ø¨Ø§Ù‚Ø©|Ø¨Ø§Ù‚Ø§Øª|Ø®Ø·Ø©)\s*[:\-]\s*\S", text):
            return True
        return False

    def extract_explicit_geography(text: str) -> List[str]:
        candidates: List[str] = []
        patterns = [
            r"\b(?:address|location|based in|located in|office in|branch in|headquarters in|service area|serving|serves)\s*:?\s*([A-Z][A-Za-z\s.'-]{2,70})",
            r"(?:Ø¹Ù†ÙˆØ§Ù†|Ù…ÙˆÙ‚Ø¹|Ù…Ù‚Ø±|ÙØ±Ø¹|Ù†Ø®Ø¯Ù…|Ø®Ø¯Ù…Ø§ØªÙ†Ø§ ÙÙŠ|ÙŠÙ‚Ø¹ ÙÙŠ|ØªÙ‚Ø¹ ÙÙŠ)\s*:?\s*([\u0600-\u06FF\s]{2,70})",
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, text or "", re.IGNORECASE):
                value = re.split(r"[.,;|\n]|\s+\band\b\s+", match.group(1).strip(), maxsplit=1, flags=re.IGNORECASE)[0]
                value = re.sub(r"\s+", " ", value).strip(" -")
                cleaned = _sanitize_evidence_item(value, category="geography")
                if cleaned:
                    candidates.append(cleaned)
        return list(dict.fromkeys(candidates))[:8]

    for card in cards:
        if not isinstance(card, dict) or card.get("excluded_reason"):
            continue

        url = card_url(card)
        page_type = page_type_of(card)
        raw_text = "\n".join([card_text(card), raw_text_for_url(url)])

        services = _clean_evidence_items(
            (card.get("visible_products_or_services") or []) + (card.get("visible_features_or_capabilities") or []),
            category="service",
            limit=18,
        )
        if services and page_type not in {"blog", "other"}:
            inventory["services_available"] = True
            add_url("service_page_urls", url)

        projects = _clean_evidence_items(
            card.get("visible_project_or_case_study_examples") or [],
            category="project_explicit",
            limit=18,
        )
        has_project_page = page_type in {"portfolio", "projects", "case_study", "case-study"}
        if (has_project_page and projects) or (has_project_page and _PROJECT_CONTEXT_RE.search(raw_text)):
            inventory["projects_available"] = True
            add_url("project_page_urls", url)

        pricing = _clean_evidence_items(card.get("visible_pricing_or_packages") or [], category="pricing", limit=12)
        if pricing or _has_explicit_pricing_evidence(raw_text, page_type):
            inventory["pricing_available"] = True
            add_url("pricing_page_urls", url)

        process = _clean_evidence_items(card.get("visible_process_steps") or [], category="process", limit=12)
        if process:
            inventory["process_available"] = True
            add_url("process_page_urls", url)

        trust = _clean_evidence_items(card.get("visible_trust_signals") or [], category="trust", limit=12)
        if trust:
            inventory["trust_available"] = True
            add_url("trust_page_urls", url)

        inventory["explicit_geography"].extend(_extract_explicit_brand_geography(raw_text_for_url(url), page_type))

    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        url = chunk_url(chunk)
        page_type = page_type_of(chunk)
        text = chunk_text(chunk)

        if page_type in {"services", "product", "home"} and _SERVICE_HINT_RE.search(text):
            inventory["services_available"] = True
            add_url("service_page_urls", url)
        if page_type in {"portfolio", "projects", "case_study", "case-study"} and _PROJECT_CONTEXT_RE.search(text):
            inventory["projects_available"] = True
            add_url("project_page_urls", url)
        if _has_explicit_pricing_evidence(text, page_type):
            inventory["pricing_available"] = True
            add_url("pricing_page_urls", url)
        if _PROCESS_HINT_RE.search(text) and page_type in {"services", "about", "home", "process"}:
            inventory["process_available"] = True
            add_url("process_page_urls", url)
        if _TRUST_CONTEXT_RE.search(text) and page_type != "blog":
            inventory["trust_available"] = True
            add_url("trust_page_urls", url)
        inventory["explicit_geography"].extend(_extract_explicit_brand_geography(text, page_type))

    inventory["explicit_geography"] = _clean_evidence_items(
        list(dict.fromkeys(inventory["explicit_geography"])),
        category="geography",
        limit=8,
    )

    for key in [
        "service_page_urls",
        "project_page_urls",
        "pricing_page_urls",
        "process_page_urls",
        "trust_page_urls",
    ]:
        inventory[key] = sorted(inventory[key])

    available_count = sum(
        bool(inventory[key])
        for key in [
            "services_available",
            "projects_available",
            "pricing_available",
            "process_available",
            "trust_available",
            "explicit_geography",
        ]
    )
    source_count = len({
        url
        for key in [
            "service_page_urls",
            "project_page_urls",
            "pricing_page_urls",
            "process_page_urls",
            "trust_page_urls",
        ]
        for url in inventory[key]
    })
    if available_count >= 4 and source_count >= 2:
        inventory["confidence"] = "high"
    elif available_count >= 1:
        inventory["confidence"] = "medium"

    return inventory


def get_empty_brand_offer_contract(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Safely returns a complete empty brand offer contract schema.
    No logging, no input mutation.
    """
    return {
        "brand_identity": {
            "brand_name": state.get("brand_name") or state.get("display_brand_name"),
            "category": None,
            "business_model": None,
            "target_users": [],
            "geographic_focus": [],
            "positioning": None,
            "confidence": "low"
        },
        "offer_mechanics": {
            "discovery_features": [],
            "search_or_filter_features": [],
            "comparison_features": [],
            "listing_features": [],
            "contact_or_conversion_flow": [],
            "supporting_services": [],
            "confidence": "low"
        },
        "value_propositions": [],
        "trust_signals": [],
        "conversion_actions": [],
        "keyword_fit": {
            "target_keyword": state.get("primary_keyword"),
            "relevant_brand_capabilities": [],
            "commercial_angle": None,
            "confidence": "low"
        },
        "supported_user_intents": [],
        "brand_limitations": [],
        "evidence_summary": {
            "used_sources": [],
            "strong_evidence": [],
            "weak_or_inferred_evidence": [],
            "missing_evidence": []
        }
    }


class BrandEvidenceService:
    def __init__(self):
        pass

    async def enrich_brand_internal_resources(self, state: Dict[str, Any], max_pages: int = 6) -> Dict[str, Any]:
        """
        Enriches state["internal_resources"] by crawling high-value same-domain pages of the brand site.
        Non-mutating on errors and fully self-contained.
        """
        brand_url = state.get("brand_url")
        if not brand_url:
            logger.info("No brand URL provided. Skipping brand site enrichment.")
            return state

        logger.info(f"[brand_site_evidence] Starting enrichment for: {brand_url} | max_pages={max_pages}")
        
        # 1. Canonicalize homepage URL
        def canonicalize_url(url: str) -> str:
            try:
                parsed = urlparse(url)
                netloc = parsed.netloc.lower()
                path = parsed.path
                if path.endswith('/') and len(path) > 1:
                    path = path[:-1]
                return f"{parsed.scheme}://{netloc}{path}"
            except Exception:
                return url

        homepage = canonicalize_url(brand_url)
        parsed_home = urlparse(homepage)
        home_domain = parsed_home.netloc.lower().replace("www.", "")

        # 2. Ignored extensions and keywords
        ignored_extensions = {
            '.pdf', '.jpg', '.jpeg', '.png', '.svg', '.webp', '.gif', '.zip', '.tar', '.gz',
            '.css', '.js', '.mp4', '.mp3', '.webm', '.avi', '.mov', '.doc', '.docx', '.xls',
            '.xlsx', '.ppt', '.pptx', '.xml', '.json', '.txt'
        }
        ignored_keywords = {
            'admin', 'login', 'signin', 'signup', 'register', 'cart', 'checkout', 'basket',
            'privacy', 'terms', 'policy', 'legal', 'cookie', 'cookies', 'settings', 'account'
        }

        def is_valid_internal(url: str) -> bool:
            try:
                parsed = urlparse(url)
                # Check same-domain
                domain = parsed.netloc.lower().replace("www.", "")
                if domain and domain != home_domain:
                    return False
                # Check path
                path_lower = parsed.path.lower()
                if any(path_lower.endswith(ext) for ext in ignored_extensions):
                    return False
                if any(kw in url.lower() for kw in ignored_keywords):
                    return False
                return True
            except Exception:
                return False

        topic_text = " ".join(
            str(value or "")
            for value in [
                state.get("primary_keyword"),
                state.get("raw_title"),
                state.get("article_type"),
                state.get("content_type"),
                state.get("brand_name"),
                state.get("brand_crawl_focus"),
                " ".join(state.get("keywords") or []),
                " ".join(state.get("secondary_keywords") or []),
                " ".join(
                    key.replace("_", " ")
                    for key, enabled in (state.get("outline_evidence_requirements") or {}).items()
                    if isinstance(enabled, bool) and enabled
                ),
            ]
        ).casefold()
        outline_requirements = state.get("outline_evidence_requirements") or {}
        stop_tokens = {
            "the", "and", "for", "with", "from", "your", "best", "company",
            "service", "services", "brand", "article", "commercial",
            "افضل", "أفضل", "شركة", "خدمة", "خدمات", "في", "من", "على",
        }
        topic_tokens = {
            token
            for token in re.findall(r"[\w\u0600-\u06FF]+", topic_text)
            if len(token) >= 3 and token not in stop_tokens
        }

        def score_url(url: str, anchor_text: str = "") -> int:
            url_lower = url.lower()
            path = urlparse(url_lower).path
            haystack = re.sub(r"[-_/]+", " ", f"{url_lower} {anchor_text or ''}").casefold()
            page_type = classify_page_type(url, anchor_text, [])
            score = sum(6 for token in topic_tokens if token in haystack)

            if page_type in {"services", "product"}:
                score += 36
            if page_type in {"portfolio", "projects", "case_study", "case-study"}:
                score += 34
            if page_type == "pricing":
                score += 18 if any(token in topic_tokens for token in {"price", "pricing", "cost", "package", "packages"}) else 8
            if page_type == "about":
                score += 6
            if page_type == "contact":
                score += 4
            if page_type == "blog":
                score -= 12

            # Detail portfolio/case-study URLs usually carry the examples the writer needs.
            if re.search(r"/(?:portfolio|project|projects|case-study|case-studies)/[^/]+", path):
                score += 42
            if outline_requirements.get("needs_projects") and page_type in {"portfolio", "projects", "case_study", "case-study"}:
                score += 30
            if outline_requirements.get("needs_services") and page_type in {"services", "product"}:
                score += 18
            if outline_requirements.get("needs_pricing") and page_type == "pricing":
                score += 28
            if outline_requirements.get("needs_process") and any(term in haystack for term in ["process", "workflow", "steps", "how it works", "delivery"]):
                score += 22
            if outline_requirements.get("needs_technologies") and any(term in haystack for term in ["technology", "technologies", "tech", "stack", "software", "systems"]):
                score += 18
            if any(term in haystack for term in ["web design", "webdesign", "website design", "web development"]):
                score += 16
            if any(term in haystack for term in ["video", "production", "blog", "news"]) and not any(
                token in haystack for token in topic_tokens
            ):
                score -= 10
            return score

        crawled_resources = []
        crawled_urls = set()

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

        # 3. Helper to scrape a single page
        async def scrape_page(url: str, anchor_text: str = "") -> Optional[dict]:
            logger.info(f"[brand_site_evidence] Fetching: {url}")
            try:
                async with httpx.AsyncClient(timeout=8.0, follow_redirects=True, verify=False) as client:
                    r = await client.get(url, headers=headers)
                    if r.status_code != 200:
                        logger.warning(f"[brand_site_evidence] Status code {r.status_code} for {url}")
                        return None

                    link_soup = BeautifulSoup(r.text, "html.parser")
                    soup = BeautifulSoup(r.text, "html.parser")
                    
                    # Page properties
                    title = soup.title.string.strip() if soup.title else ""
                    
                    # Meta description
                    meta_desc = ""
                    meta_tag = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
                    if meta_tag:
                        meta_desc = meta_tag.get("content", "").strip()

                    # Links extraction can use raw soup; content extraction below uses cleaned DOM.
                    extracted_links = []
                    for a in link_soup.find_all("a", href=True):
                        href = a["href"].strip()
                        txt = re.sub(r"\s+", " ", a.get_text(" ", strip=True)).strip()
                        full_url = urljoin(homepage, href)
                        canon = canonicalize_url(full_url)
                        if is_valid_internal(canon) and canon != homepage:
                            extracted_links.append((canon, txt))

                    # Remove non-content chrome before headings, sections, and page text.
                    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "noscript", "iframe"]):
                        tag.decompose()

                    # Extract headings
                    headings = []
                    for h in soup.find_all(["h1", "h2", "h3"]):
                        h_text = h.get_text(strip=True)
                        if h_text and len(h_text) < 250:
                            headings.append(h_text)

                    # Extract CTAs
                    ctas = []
                    for btn in soup.find_all(["button", "a", "input"]):
                        label = ""
                        if btn.name in ["a", "button"]:
                            label = btn.get_text(strip=True)
                        elif btn.name == "input" and btn.get("type") == "submit":
                            label = btn.get("value", "")
                        label = re.sub(r"\s+", " ", label).strip()
                        if label and 2 <= len(label) <= 40:
                            ctas.append(label)

                    # Classify page type and extract semantic sections
                    p_type = classify_page_type(url, title, headings[:15])
                    semantic_sections = extract_semantic_sections_from_soup(soup, url, title, p_type)

                    # Clean visible body text
                    visible_text = soup.get_text(separator=" ")
                    clean_text = re.sub(r"\s+", " ", visible_text).strip()
                    truncated_text = clean_text[:4000]
                    full_text = clean_text[:100000]

                    return {
                        "link": url,
                        "text": anchor_text or title,
                        "title": title,
                        "headings": headings[:15],
                        "cta_labels": list(dict.fromkeys(ctas))[:15],
                        "page_text": truncated_text,
                        "page_text_full": full_text,
                        "meta_description": meta_desc,
                        "is_brand_crawled": True,
                        "evidence_source": "brand_site_crawl",
                        "semantic_sections": semantic_sections,
                        "extracted_links": extracted_links,
                        "read_stats": {
                            "text_chars": len(full_text),
                            "semantic_sections_count": len(semantic_sections),
                            "extracted_links_count": len(extracted_links),
                        },
                        "page_type": p_type
                    }
            except Exception as e:
                logger.error(f"[brand_site_evidence] Failed scraping {url}: {e}")
                return None

        # 4. Fetch homepage first
        home_res = await scrape_page(homepage)
        internal_links_map = {}
        if home_res:
            crawled_resources.append(home_res)
            crawled_urls.add(homepage)
            for link, txt in home_res.pop("extracted_links", []):
                if link not in crawled_urls and link not in internal_links_map:
                    internal_links_map[link] = txt

        # 5. Topic-aware crawl queue. Keep the page limit, but spend it on pages
        # that can actually help the current article and on project/service details.
        sorted_links_snapshot = []
        while len(crawled_resources) < max_pages and internal_links_map:
            ranked_links = sorted(
                (
                    (score_url(link, txt), len(link), link, txt)
                    for link, txt in internal_links_map.items()
                    if link not in crawled_urls
                ),
                key=lambda item: (-item[0], item[1], item[2]),
            )
            if not ranked_links:
                break
            sorted_links_snapshot = [item[2] for item in ranked_links]
            _, _, next_link, anchor_text = ranked_links[0]
            internal_links_map.pop(next_link, None)
            logger.info("[brand_site_evidence] Topic-selected page for crawling: %s", next_link)
            res = await scrape_page(next_link, anchor_text)
            if not res:
                continue
            crawled_resources.append(res)
            crawled_urls.add(res["link"])
            for link, txt in res.pop("extracted_links", []):
                if link not in crawled_urls and link not in internal_links_map:
                    internal_links_map[link] = txt

        # 7. Merge with existing internal_resources in state, deduplicating by canonical link
        existing_resources = state.get("internal_resources", []) or []
        merged_resources = []
        seen_links = set()

        for res in crawled_resources:
            seen_links.add(res["link"])
            merged_resources.append(res)

        for res in existing_resources:
            if isinstance(res, dict) and res.get("link"):
                canon = canonicalize_url(res["link"])
                if canon not in seen_links:
                    seen_links.add(canon)
                    merged_resources.append(res)

        state["internal_resources"] = merged_resources
        state["brand_crawl_report"] = {
            "crawled_urls": list(crawled_urls),
            "candidate_urls_count": len(internal_links_map) + len(crawled_urls),
            "topic_tokens": sorted(topic_tokens)[:30],
            "page_read_stats": [
                {
                    "url": res.get("link"),
                    "page_type": res.get("page_type"),
                    **(res.get("read_stats") or {}),
                }
                for res in crawled_resources
            ],
        }
        
        logger.info(f"[brand_site_evidence] crawled_pages_count={len(crawled_urls)} | extracted_links_count={len(sorted_links_snapshot)} | skipped=false")
        return state

    async def run_brand_evidence_map(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Builds a comprehensive, robust, and defensive brand_evidence_map.
        Inspects links, headings, cta_labels, page_text, snippets, and descriptions defensively.
        Categorizes evidence by strength levels (strong, medium, weak).
        Uses raw unicode escape characters for Arabic to prevent mojibake/encoding corruption.
        """
        evidence_map = {
            "strong_signals": [],
            "medium_signals": [],
            "weak_signals": [],
            "strong_source_urls": [],
            "source_counts": {
                "headings": 0,
                "cta_labels": 0,
                "anchors": 0,
                "urls": 0
            },
            "missing_evidence": []
        }
        
        internal_resources = state.get("internal_resources", [])
        brand_context = state.get("brand_context", "")
        primary_keyword = state.get("primary_keyword", "")

        # Define keyword matchers (using unicode escape sequences for Arabic characters)
        # discovery: browse, explore, discover, properties, listings, apartments, تصفح, اكتشف, معروضات, عقارات, شقق, فلل
        discovery_kws = ["browse", "explore", "discover", "properties", "listings", "apartments", 
                         "\u062a\u0635\u0641\u062d", "\u0627\u0643\u062a\u0634\u0641", "\u0645\u0639\u0631\u0648\u0636\u0627\u062a", 
                         "\u0639\u0642\u0627\u0631\u0627\u062a", "\u0634\u0642\u0642", "\u0645\u0641\u0631\u0648\u0634", "\u0641\u0644\u0644"]
                         
        # search: search, filter, find, ابحث, بحث, تصفية, فلتر, فلترة
        search_kws = ["search", "filter", "find", 
                      "\u0627\u0628\u062d\u062b", "\u0628\u062d\u062b", "\u062a\u0635\u0641\u064a\u0629", "\u0641\u0644\u062a\u0631", "\u0641\u0644\u062a\u0631\u0629"]
                      
        # compare: compare, comparison, versus, vs, مقارنة, قارن, الفرق
        compare_kws = ["compare", "comparison", "versus", "vs", 
                       "\u0645\u0642\u0627\u0631\u0646\u0629", "\u0642\u0627\u0631\u0646", "\u0627\u0644\u0641\u0631\u0642"]
                       
        # contact: contact, whatsapp, call, book, reserve, inquiry, تواصل, اتصل, واتساب, احجز, حجز, استفسار, طلب
        contact_kws = ["contact", "whatsapp", "call", "book", "reserve", "inquiry", 
                       "\u062a\u0648\u0627\u0635\u0644", "\u0627\u062a\u0635\u0644", "\u0648\u0627\u062a\u0633\u0627\u0628", 
                       "\u0627\u062d\u062c\u0632", "\u062d\u062c\u0632", "\u0627\u0633\u062a\u0641\u0633\u0627\u0631", "\u0637\u0644\u0628"]
        
        # location: location, address, district, city, map, موقع, عنوان, حي, الرياض, جدة, منطقة
        location_kws = ["location", "address", "district", "city", "map", 
                        "\u0645\u0648\u0642\u0639", "\u0639\u0646\u0648\u0627\u0646", "\u062d\u064a", 
                        "\u0627\u0644\u0631\u064a\u0627\u0636", "\u062c\u062f\u0629", "\u0645\u0646\u0637\u0642\u0629"]
                        
        # image: image, photo, gallery, صورة, صور, معرض
        image_kws = ["image", "photo", "gallery", "\u0635\u0648\u0631\u0629", "\u0635\u0648\u0631", "\u0645\u0639\u0631\u0636"]
        
        # price: price, pricing, cost, fee, rate, سعر, أسعار, تكلفة, ريال
        price_kws = ["price", "pricing", "cost", "fee", "rate", 
                     "\u0633\u0639\u0631", "\u0623\u0633\u0639\u0627\u0631", "\u062a\u0643\u0644\u0641\u0629", "\u0631\u064a\u0627\u0644"]
                     
        # verified: verified, licensed, certified, trusted, موثوق, مرخص, معتمد, مضمون
        verified_kws = ["verified", "licensed", "certified", "trusted", 
                        "\u0645\u0648\u062b\u0648\u0642", "\u0645\u0631\u062e\u0635", "\u0645\u0639\u062a\u0645\u062f", "\u0645\u0636\u0645\u0648\u0646"]

        # Parse defensively
        for resource in internal_resources:
            if not isinstance(resource, dict):
                continue
                
            link = str(resource.get("link", "")).lower()
            text = str(resource.get("text", "")).lower()
            anchor = str(resource.get("anchor", "")).lower()
            title = str(resource.get("title", "")).lower()
            headings = [str(h).lower() for h in resource.get("headings", []) if h]
            cta_labels = [str(c).lower() for c in resource.get("cta_labels", []) if c]
            page_text = str(resource.get("page_text", "")).lower()
            snippet = str(resource.get("snippet", "")).lower()
            meta_description = str(resource.get("meta_description", "")).lower()

            # Helper to check if keyword is in a collection of strings
            def any_in(kws, *texts):
                return any(any(kw in str(t) for kw in kws) for t in texts if t)

            # 1. Evaluate Strong Signals (explicit page headings, page text, explicit CTA labels)
            strong_signal_for_resource = False
            if any_in(discovery_kws, headings, cta_labels, page_text):
                evidence_map["strong_signals"].append("browse listings")
                strong_signal_for_resource = True
            if any_in(search_kws, headings, cta_labels, page_text):
                evidence_map["strong_signals"].append("search or filter")
                strong_signal_for_resource = True
            if any_in(compare_kws, headings, cta_labels, page_text):
                evidence_map["strong_signals"].append("comparison tools")
                strong_signal_for_resource = True
            if any_in(contact_kws, headings, cta_labels, page_text):
                evidence_map["strong_signals"].append("contact provider")
                strong_signal_for_resource = True
            if any_in(location_kws, headings, cta_labels, page_text):
                evidence_map["strong_signals"].append("location info")
                strong_signal_for_resource = True
            if any_in(image_kws, headings, cta_labels, page_text):
                evidence_map["strong_signals"].append("images shown")
                strong_signal_for_resource = True
            if any_in(price_kws, headings, cta_labels, page_text):
                evidence_map["strong_signals"].append("price shown")
                strong_signal_for_resource = True
            if any_in(verified_kws, headings, cta_labels, page_text):
                evidence_map["strong_signals"].append("verified status")
                strong_signal_for_resource = True
            if strong_signal_for_resource and link:
                evidence_map["strong_source_urls"].append(link)

            # 2. Evaluate Medium Signals (internal resource anchor text, title, snippet, meta description)
            if any_in(discovery_kws, [text, anchor, title, snippet, meta_description]):
                evidence_map["medium_signals"].append("browse listings")
            if any_in(search_kws, [text, anchor, title, snippet, meta_description]):
                evidence_map["medium_signals"].append("search or filter")
            if any_in(compare_kws, [text, anchor, title, snippet, meta_description]):
                evidence_map["medium_signals"].append("comparison tools")
            if any_in(contact_kws, [text, anchor, title, snippet, meta_description]):
                evidence_map["medium_signals"].append("contact provider")
            if any_in(location_kws, [text, anchor, title, snippet, meta_description]):
                evidence_map["medium_signals"].append("location info")
            if any_in(image_kws, [text, anchor, title, snippet, meta_description]):
                evidence_map["medium_signals"].append("images shown")
            if any_in(price_kws, [text, anchor, title, snippet, meta_description]):
                evidence_map["medium_signals"].append("price shown")
            if any_in(verified_kws, [text, anchor, title, snippet, meta_description]):
                evidence_map["medium_signals"].append("verified status")

            # 3. Evaluate Weak Signals (URL link slugs)
            if any(kw in link for kw in discovery_kws):
                evidence_map["weak_signals"].append("browse listings")
            if any(kw in link for kw in search_kws):
                evidence_map["weak_signals"].append("search or filter")
            if any(kw in link for kw in compare_kws):
                evidence_map["weak_signals"].append("comparison tools")
            if any(kw in link for kw in contact_kws):
                evidence_map["weak_signals"].append("contact provider")
            if any(kw in link for kw in location_kws):
                evidence_map["weak_signals"].append("location info")
            if any(kw in link for kw in image_kws):
                evidence_map["weak_signals"].append("images shown")
            if any(kw in link for kw in price_kws):
                evidence_map["weak_signals"].append("price shown")
            if any(kw in link for kw in verified_kws):
                evidence_map["weak_signals"].append("verified status")

            # Source counters
            if headings:
                evidence_map["source_counts"]["headings"] += 1
            if cta_labels:
                evidence_map["source_counts"]["cta_labels"] += 1
            if anchor or text:
                evidence_map["source_counts"]["anchors"] += 1
            if link:
                evidence_map["source_counts"]["urls"] += 1

        # Brand context is inherently a weak signal (descriptive, not observed)
        if brand_context:
            evidence_map["weak_signals"].append("context inference")

        # Compile missing evidence list
        all_observables = {
            "browse listings": "no listing browsing observed",
            "search or filter": "no explicit search or filter detected",
            "comparison tools": "no explicit comparison tool observed",
            "contact provider": "no explicit booking flow detected",
            "location info": "no explicit location or address observed",
            "images shown": "no explicit image assets observed",
            "price shown": "no verified pricing evidence found",
            "verified status": "no explicit verification badges observed"
        }
        
        all_signals = set(evidence_map["strong_signals"] + evidence_map["medium_signals"] + evidence_map["weak_signals"])
        for req_sig, missing_msg in all_observables.items():
            if req_sig not in all_signals:
                evidence_map["missing_evidence"].append(missing_msg)

        # Deduplicate signals
        evidence_map["strong_signals"] = list(dict.fromkeys(evidence_map["strong_signals"]))
        evidence_map["medium_signals"] = list(dict.fromkeys(evidence_map["medium_signals"]))
        evidence_map["weak_signals"] = list(dict.fromkeys(evidence_map["weak_signals"]))
        evidence_map["strong_source_urls"] = list(dict.fromkeys(evidence_map.get("strong_source_urls", [])))
        evidence_map["missing_evidence"] = list(dict.fromkeys(evidence_map["missing_evidence"]))

        state["brand_evidence_map"] = evidence_map
        return state


def build_brand_offer_contract(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pure builder function to derive a structured commercial contract.
    No network calls. No logging inside the builder. No state mutation.
    Deterministic, always returns the full required schema.
    Strictly follows source priority / evidence weighting.
    Uses raw unicode escape characters for Arabic to prevent mojibake/encoding corruption.
    """
    # 1. Resolve priority checklist (cards check first)
    cards = state.get("brand_evidence_cards")
    if not cards:
        cards = build_brand_evidence_cards(state)

    brand_context = state.get("brand_context", "") or ""
    target_keyword = state.get("primary_keyword", "") or ""

    # Initialize empty clean contract
    contract = get_empty_brand_offer_contract(state)

    # 2. Score and classify business domain
    # --- Evidence Priority: cards-first classifier ---
    # Build a cards-only text for classification when useful cards exist.
    # Only fall back to internal_resources / brand_context / target_keyword when cards are absent.

    re_kws = ["property", "properties", "listings", "listing", "rent", "sale", "apartment", "apartments", "agent", "owner", "villa", "real estate", "\u0639\u0642\u0627\u0631\u0627\u062a", "\u0634\u0642\u0642", "\u0641\u0644\u0644", "\u0627\u064a\u062c\u0627\u0631", "\u0625\u064a\u062c\u0627\u0631"]
    ds_kws = ["web", "design", "app", "software", "marketing", "branding", "hosting", "development", "consulting", "seo", "agency", "firm", "\u062a\u0635\u0645\u064a\u0645", "\u0645\u0648\u0627\u0642\u0639", "\u062a\u0637\u0648\u064a\u0631", "\u0628\u0631\u0645\u062c\u0629", "\u062a\u0633\u0648\u064a\u0642"]
    ec_kws = ["products", "product", "cart", "shop", "checkout", "categories", "ecommerce", "e-commerce", "store", "online store", "retail", "\u0645\u062a\u062c\u0631", "\u0634\u0631\u0627\u0621", "\u0645\u0646\u062a\u062c\u0627\u062a", "\u0633\u0644\u0629"]
    ls_kws = ["booking", "appointment", "service area", "cleaning", "plumber", "salon", "laundry", "\u062d\u062c\u0632", "\u062d\u062c\u0648\u0632\u0627\u062a", "\u062e\u062f\u0645\u0629 \u0645\u0646\u0632\u0644\u064a\u0629"]

    def _score_text(text_blob: str) -> dict:
        scores = {"real_estate": 0, "digital_services": 0, "ecommerce": 0, "local_service": 0}
        for kw in re_kws:
            scores["real_estate"] += text_blob.count(kw)
        for kw in ds_kws:
            scores["digital_services"] += text_blob.count(kw)
        for kw in ec_kws:
            scores["ecommerce"] += text_blob.count(kw)
        for kw in ls_kws:
            scores["local_service"] += text_blob.count(kw)
        return scores

    def _pick_domain(scores: dict, threshold: int = 2) -> str:
        best, best_score = "unknown", 0
        for dom, score in scores.items():
            if score > best_score:
                best_score = score
                best = dom
        return best if best_score >= threshold else "unknown"

    # --- cards-first classification ---
    useful_cards_for_classify = [c for c in cards if not c.get("excluded_reason")]

    if useful_cards_for_classify:
        # Classify using card content only
        card_text_parts = []
        for card in useful_cards_for_classify:
            card_text_parts.append(card.get("url", ""))
            card_text_parts.append(card.get("title") or "")
            card_text_parts.extend(card.get("headings", []))
            card_text_parts.extend(card.get("visible_products_or_services", []))
            card_text_parts.extend(card.get("visible_features_or_capabilities", []))
            card_text_parts.extend(card.get("cta_labels", []))
            card_text_parts.extend(card.get("usable_snippets", []))
        cards_only_text = " ".join(card_text_parts).lower()
        detected_domain = _pick_domain(_score_text(cards_only_text), threshold=2)
    else:
        # Fallback 1: internal_resources + brand_context (weak fallback)
        fallback_parts = []
        for res in state.get("internal_resources", []) or []:
            fallback_parts.append(res.get("link", ""))
            fallback_parts.append(res.get("text", ""))
            fallback_parts.append(res.get("title") or "")
            fallback_parts.extend(res.get("headings", []))
            fallback_parts.extend(res.get("cta_labels", []))
            fallback_parts.append(res.get("page_text", ""))
        fallback_parts.append(brand_context)
        fallback_text = " ".join(fallback_parts).lower()
        detected_domain = _pick_domain(_score_text(fallback_text), threshold=2)

        if detected_domain == "unknown":
            # Fallback 2: primary_keyword as absolute last resort
            kw_text = target_keyword.lower()
            detected_domain = _pick_domain(_score_text(kw_text), threshold=2)

    # Build combined_text for later mechanics (cards + resources + context, NOT re-used for classification)
    combined_text_parts = []
    for card in cards:
        combined_text_parts.append(card.get("url", ""))
        combined_text_parts.append(card.get("title") or "")
        combined_text_parts.extend(card.get("headings", []))
        combined_text_parts.extend(card.get("visible_products_or_services", []))
        combined_text_parts.extend(card.get("visible_features_or_capabilities", []))
        combined_text_parts.extend(card.get("cta_labels", []))
        combined_text_parts.extend(card.get("usable_snippets", []))
    for res in state.get("internal_resources", []) or []:
        combined_text_parts.append(res.get("link", ""))
        combined_text_parts.append(res.get("text", ""))
        combined_text_parts.append(res.get("title") or "")
        combined_text_parts.extend(res.get("headings", []))
        combined_text_parts.extend(res.get("cta_labels", []))
        combined_text_parts.append(res.get("page_text", ""))
    combined_text_parts.append(brand_context)
    combined_text_parts.append(target_keyword)
    combined_text = " ".join(combined_text_parts).lower()

    # Store detected_domain in evidence summary
    contract["evidence_summary"]["detected_domain"] = detected_domain

    # 3. Extract explicit evidence from cards
    explicit_geography = []
    explicit_pricing = []
    explicit_guarantees = []
    explicit_certifications = []
    explicit_partnerships = []
    explicit_delivery_timelines = []
    
    for card in cards:
        if card.get("excluded_reason"):
            continue
        explicit_geography.extend(_clean_evidence_items(card.get("visible_geography", []), category="geography", limit=8))
        explicit_pricing.extend(_clean_evidence_items(card.get("visible_pricing_or_packages", []), category="pricing", limit=8))

        trust_text = " ".join(_clean_evidence_items(card.get("visible_trust_signals", []), category="trust", limit=12)).lower()
        if any(w in trust_text for w in ["guarantee", "guaranteed", "ضمان", "مضمون"]):
            explicit_guarantees.append("service guarantee")
        if any(w in trust_text for w in ["certified", "certification", "license", "licensed", "iso", "معتمد", "مرخص", "شهادة"]):
            explicit_certifications.append("certified credentials")
        if any(w in trust_text for w in ["partner", "partnership", "شريك", "شراكة"]):
            explicit_partnerships.append("verified partnerships")
        if any(w in trust_text for w in ["delivery in", "delivered in", "تسليم خلال"]):
            explicit_delivery_timelines.append("defined delivery timelines")
            
    explicit_geography = list(dict.fromkeys(explicit_geography))
    explicit_pricing = list(dict.fromkeys(explicit_pricing))
    explicit_guarantees = list(dict.fromkeys(explicit_guarantees))
    explicit_certifications = list(dict.fromkeys(explicit_certifications))
    explicit_partnerships = list(dict.fromkeys(explicit_partnerships))
    explicit_delivery_timelines = list(dict.fromkeys(explicit_delivery_timelines))

    # Populate explicit values
    if explicit_geography:
        contract["brand_identity"]["geographic_focus"] = explicit_geography
    if explicit_pricing:
        contract["trust_signals"].append("transparent pricing")
    if explicit_guarantees:
        contract["trust_signals"].append("service guarantee")
    if explicit_certifications:
        contract["trust_signals"].append("certified credentials")
    if explicit_partnerships:
        contract["trust_signals"].append("verified partnerships")
    if explicit_delivery_timelines:
        contract["trust_signals"].append("defined delivery timelines")

    has_contact = any(len(card.get("visible_support_or_contact_methods", [])) > 0 for card in cards)
    if has_contact:
        contract["trust_signals"].append("visible contact method")

    # Signals
    strong_signals = set(state.get("brand_evidence_map", {}).get("strong_signals", []))
    medium_signals = set(state.get("brand_evidence_map", {}).get("medium_signals", []))
    weak_signals = set(state.get("brand_evidence_map", {}).get("weak_signals", []))
    all_signals = strong_signals.union(medium_signals).union(weak_signals)

    # 4. Map Industry-Specific Mechanics
    if detected_domain == "real_estate":
        contract["brand_identity"]["category"] = "real estate listing platform"
        contract["brand_identity"]["business_model"] = "property discovery marketplace"
        
        has_listings_c = any(card.get("page_type") in ["home", "services"] for card in cards)
        has_search_c = any("search" in " ".join(card.get("headings", [])).lower() for card in cards)
        
        if "browse listings" in all_signals or has_listings_c:
            contract["offer_mechanics"]["discovery_features"].append("browse property listings")
            contract["conversion_actions"].append("browse listings")
            contract["supported_user_intents"].append("discover_listings")
            
        if "search or filter" in all_signals or has_search_c:
            has_explicit_district = "district" in combined_text or "riyadh" in combined_text or "jeddah" in combined_text or "\u062d\u064a" in combined_text or "\u0627\u0644\u0631\u064a\u0627\u0636" in combined_text or "\u062c\u062f\u0629" in combined_text
            if has_explicit_district:
                contract["offer_mechanics"]["search_or_filter_features"].append("search apartments by district")
                contract["conversion_actions"].append("search apartments by district")
            else:
                contract["offer_mechanics"]["search_or_filter_features"].append("search listings")
                contract["conversion_actions"].append("search listings")
            contract["supported_user_intents"].append("discover_listings")
            
        if "comparison tools" in all_signals:
            contract["offer_mechanics"]["comparison_features"].append("compare listing details")
            contract["supported_user_intents"].append("compare_options")
            
        if "contact provider" in all_signals:
            contract["offer_mechanics"]["contact_or_conversion_flow"].append("contact advertiser / owner / agent")
            contract["conversion_actions"].append("contact agent")
            contract["supported_user_intents"].append("contact_provider")
            
        contract["value_propositions"].append("reduce apartment search time")
        contract["value_propositions"].append("evaluate listings using visible property details")
        
        if "verified status" in all_signals or explicit_certifications:
            contract["trust_signals"].append("verified listings")
        if "price shown" in all_signals or explicit_pricing:
            contract["trust_signals"].append("price shown")
        if "images shown" in all_signals:
            contract["trust_signals"].append("clear property images")
            
        if explicit_geography:
            geo_name = explicit_geography[0]
            contract["keyword_fit"]["relevant_brand_capabilities"].append(f"{geo_name} rental listings")
        else:
            contract["keyword_fit"]["relevant_brand_capabilities"].append("rental listings")
        contract["keyword_fit"]["relevant_brand_capabilities"].append("apartment browsing")
        contract["keyword_fit"]["commercial_angle"] = "direct property discovery marketplace matching search intent"

    elif detected_domain == "digital_services":
        has_web_design = "web design" in combined_text or "تصميم مواقع" in combined_text
        if has_web_design:
            contract["brand_identity"]["category"] = "web design agency"
            contract["brand_identity"]["business_model"] = "b2b web design and development services"
        else:
            contract["brand_identity"]["category"] = "digital agency"
            contract["brand_identity"]["business_model"] = "b2b services"
            
        observed_services = []
        observed_capabilities = []
        for card in cards:
            if not card.get("excluded_reason"):
                observed_services.extend(card.get("visible_products_or_services", []))
                observed_capabilities.extend(card.get("visible_features_or_capabilities", []))
        observed_services = _clean_evidence_items(observed_services, category="service", limit=18)
        observed_capabilities = _clean_evidence_items(observed_capabilities, category="capability", limit=18)
        observed_services = list(dict.fromkeys(observed_services + observed_capabilities))
        
        contract["offer_mechanics"]["supporting_services"] = observed_services
        contract["keyword_fit"]["relevant_brand_capabilities"] = observed_services
        contract["offer_mechanics"]["discovery_features"].append("explore agency portfolio")
        
        actions = []
        for card in cards:
            for act in card.get("visible_conversion_actions", []):
                actions.append(act.lower())
        actions = list(dict.fromkeys(actions))

        # Fallback: scan explicit contact/support evidence from cards only
        # Do NOT scan combined_text (would pick up article keyword noise).
        # Do NOT add generic "contact provider" as a default.
        if not actions:
            has_contact_evidence = any(
                len(card.get("visible_support_or_contact_methods", [])) > 0
                for card in cards if not card.get("excluded_reason")
            )
            if has_contact_evidence:
                # Scan card text for specific intent signals
                card_cta_text = " ".join(
                    " ".join(card.get("cta_labels", []))
                    for card in cards if not card.get("excluded_reason")
                ).lower()
                if any(kw in card_cta_text for kw in ["quote", "request", "\u062a\u0633\u0639\u064a\u0631"]):
                    actions.append("request quote")
                if any(kw in card_cta_text for kw in ["consultation", "book", "meeting", "\u0627\u0633\u062a\u0634\u0627\u0631\u0629"]):
                    actions.append("book consultation")
                if any(kw in card_cta_text for kw in ["inquiry", "form", "submit", "\u0627\u0633\u062a\u0641\u0633\u0627\u0631", "\u0637\u0644\u0628", "\u0646\u0645\u0648\u0630\u062c"]):
                    actions.append("submit inquiry")
                if any(kw in card_cta_text for kw in ["whatsapp", "call", "phone", "contact", "\u062a\u0648\u0627\u0635\u0644", "\u0648\u0627\u062a\u0633\u0627\u0628"]):
                    actions.append("contact provider")
        contract["conversion_actions"] = actions
        contract["supported_user_intents"].append("consult_agency")
        contract["keyword_fit"]["commercial_angle"] = "b2b professional design and digital solutions provider"

    elif detected_domain == "ecommerce":
        contract["brand_identity"]["category"] = "e-commerce store"
        contract["brand_identity"]["business_model"] = "b2c retail marketplace"
        
        observed_products = []
        observed_capabilities = []
        for card in cards:
            if not card.get("excluded_reason"):
                observed_products.extend(card.get("visible_products_or_services", []))
                observed_capabilities.extend(card.get("visible_features_or_capabilities", []))
        observed_products = list(dict.fromkeys(
            _clean_evidence_items(observed_products, category="service", limit=18)
            + _clean_evidence_items(observed_capabilities, category="capability", limit=18)
        ))
        
        contract["offer_mechanics"]["supporting_services"] = observed_products
        contract["keyword_fit"]["relevant_brand_capabilities"] = observed_products
        # Only add discovery if product evidence exists
        if observed_products:
            contract["offer_mechanics"]["discovery_features"].append("browse products")
            contract["offer_mechanics"]["search_or_filter_features"].append("filter by product categories")

        actions = []
        for card in cards:
            for act in card.get("visible_conversion_actions", []):
                actions.append(act.lower())
        actions = list(dict.fromkeys(actions))
        # Only add cart/checkout mechanics if explicit CTA evidence supports it
        has_cart_cta = any(
            kw in act for act in actions
            for kw in ["cart", "checkout", "buy", "order", "purchase", "\u0634\u0631\u0627\u0621", "\u0633\u0644\u0629"]
        )
        if has_cart_cta:
            contract["offer_mechanics"]["contact_or_conversion_flow"].append("add to cart / checkout")
        contract["conversion_actions"] = actions
        contract["supported_user_intents"].append("purchase_products")
        contract["keyword_fit"]["commercial_angle"] = "direct ecommerce online storefront matching purchasing intent"

    elif detected_domain == "local_service":
        contract["brand_identity"]["category"] = "local service provider"
        contract["brand_identity"]["business_model"] = "booking-based services"
        
        observed_services = []
        observed_capabilities = []
        for card in cards:
            if not card.get("excluded_reason"):
                observed_services.extend(card.get("visible_products_or_services", []))
                observed_capabilities.extend(card.get("visible_features_or_capabilities", []))
        observed_services = list(dict.fromkeys(
            _clean_evidence_items(observed_services, category="service", limit=18)
            + _clean_evidence_items(observed_capabilities, category="capability", limit=18)
        ))
        
        contract["offer_mechanics"]["supporting_services"] = observed_services
        contract["keyword_fit"]["relevant_brand_capabilities"] = observed_services
        contract["offer_mechanics"]["discovery_features"].append("view local service details")
        # Only add booking flow if explicit CTA evidence supports it
        actions = []
        for card in cards:
            for act in card.get("visible_conversion_actions", []):
                actions.append(act.lower())
        actions = list(dict.fromkeys(actions))
        has_booking_cta = any(
            kw in act for act in actions
            for kw in ["book", "appointment", "schedule", "reserve", "\u062d\u062c\u0632"]
        )
        if has_booking_cta:
            contract["offer_mechanics"]["contact_or_conversion_flow"].append("book online / submit appointment request")
        contract["conversion_actions"] = actions
        contract["supported_user_intents"].append("book_service")
        contract["keyword_fit"]["commercial_angle"] = "local service scheduling and fulfillment"

    else:
        # unknown - conservative fallback, no default conversion actions
        contract["brand_identity"]["category"] = "service or platform"
        contract["brand_identity"]["business_model"] = "direct channel"
        contract["supported_user_intents"].append("general_inquiry")
        # Preserve explicit actions only; fall back to contact provider only with contact/support evidence.
        unknown_actions = []
        has_contact_support = False
        for card in cards:
            if card.get("excluded_reason"):
                continue
            for action in card.get("visible_conversion_actions", []):
                unknown_actions.append(action.lower())
            if card.get("visible_support_or_contact_methods", []):
                has_contact_support = True
        if unknown_actions:
            contract["conversion_actions"] = list(dict.fromkeys(unknown_actions))
        elif has_contact_support:
            contract["conversion_actions"].append("contact provider")

    # Sources checklist
    used_sources = []
    if cards:
        used_sources.append("brand_evidence_cards")
    if state.get("brand_evidence_map"):
        used_sources.append("brand_evidence_map")
    if brand_context:
        used_sources.append("brand_context")
    if not used_sources:
        used_sources.append("none")
    contract["evidence_summary"]["used_sources"] = used_sources
    
    sig_map = {
        "location info": "location information",
        "images shown": "clear property images",
        "price shown": "price shown",
        "verified status": "verified listings",
        "contact provider": "contact flow",
        "browse listings": "browse listings",
        "search or filter": "search functionality",
        "comparison tools": "comparison tools"
    }
    real_estate_only_signal_keys = {
        "browse listings",
        "comparison tools",
        "verified status",
        "images shown",
        "price shown",
    }
    
    for sig in strong_signals:
        if detected_domain != "real_estate" and sig in real_estate_only_signal_keys:
            continue
        mapped = sig_map.get(sig, sig)
        contract["evidence_summary"]["strong_evidence"].append(mapped)
    for sig in medium_signals:
        if detected_domain != "real_estate" and sig in real_estate_only_signal_keys:
            continue
        mapped = sig_map.get(sig, sig)
        contract["evidence_summary"]["weak_or_inferred_evidence"].append(mapped)

    # 5. Determine Confidence Level
    useful_cards_count = 0
    strong_clean_fact_pages = 0
    for card in cards:
        if not card.get("excluded_reason"):
            clean_services = _clean_evidence_items(card.get("visible_products_or_services", []), category="service", limit=8)
            clean_capabilities = _clean_evidence_items(card.get("visible_features_or_capabilities", []), category="capability", limit=8)
            clean_projects = _clean_evidence_items(card.get("visible_project_or_case_study_examples", []), category="project_explicit", limit=8)
            clean_process = _clean_evidence_items(card.get("visible_process_steps", []), category="process", limit=8)
            has_facts = (
                bool(clean_services) or
                bool(clean_capabilities) or
                bool(clean_projects) or
                bool(clean_process) or
                bool(_clean_evidence_items(card.get("visible_trust_signals", []), category="trust", limit=4)) or
                bool(_clean_evidence_items(card.get("visible_geography", []), category="geography", limit=4)) or
                bool(_clean_evidence_items(card.get("visible_pricing_or_packages", []), category="pricing", limit=4))
            )
            if has_facts:
                useful_cards_count += 1
            if len(clean_services + clean_capabilities + clean_projects + clean_process) >= 2:
                strong_clean_fact_pages += 1
                
    clean_capability_count = len(contract["offer_mechanics"].get("supporting_services", []))
    if useful_cards_count >= 2 and strong_clean_fact_pages >= 2 and clean_capability_count >= 3:
        overall_conf = "high"
    elif useful_cards_count >= 1 or clean_capability_count >= 1:
        overall_conf = "medium"
    else:
        overall_conf = "low"
        
    contract["brand_identity"]["confidence"] = overall_conf
    contract["offer_mechanics"]["confidence"] = overall_conf
    contract["keyword_fit"]["confidence"] = overall_conf

    # Limitations & Missing Evidence - domain-aware
    # Real-estate-specific missing items must not bleed into non-RE domains
    _re_missing_phrases = [
        "no listing browsing observed",
        "no explicit comparison tool observed",
        "no explicit verification badges observed",
    ]

    # Domain-specific default missing evidence when brand_evidence_map has none
    _default_missing_by_domain = {
        "real_estate": [
            "no listing browsing observed",
            "no explicit search or filter detected",
            "no explicit comparison tool observed",
            "no explicit booking flow detected",
            "no explicit location or address observed",
            "no explicit image assets observed",
            "no verified pricing evidence found",
            "no explicit verification badges observed",
        ],
        "digital_services": [
            "no explicit portfolio or project evidence found",
            "no explicit quote/contact CTA found",
            "no explicit pricing evidence found",
            "no explicit certification or partnership evidence found",
        ],
        "ecommerce": [
            "no explicit checkout or cart action found",
            "no explicit pricing evidence found",
            "no explicit shipping or fulfillment evidence found",
        ],
        "local_service": [
            "no explicit booking flow detected",
            "no explicit location or address observed",
            "no explicit pricing evidence found",
        ],
        "unknown": [
            "no explicit product or service evidence found",
            "no explicit conversion action found",
            "no explicit trust evidence found",
            "no explicit geography evidence found",
        ],
    }

    raw_missing = state.get("brand_evidence_map", {}).get("missing_evidence", [])
    if raw_missing:
        # Filter out real-estate-specific items when domain is not real_estate
        if detected_domain != "real_estate":
            missing = [
                m for m in raw_missing
                if m not in _re_missing_phrases
                and "listing" not in str(m).lower()
                and "comparison tool" not in str(m).lower()
                and "verification badge" not in str(m).lower()
            ]
        else:
            missing = list(raw_missing)
    else:
        missing = list(_default_missing_by_domain.get(detected_domain, _default_missing_by_domain["unknown"]))

    contract["evidence_summary"]["missing_evidence"] = missing

    # Map missing_evidence to brand_limitations (neutral, domain-aware)
    for m in missing:
        if "comparison" in m and detected_domain == "real_estate":
            contract["brand_limitations"].append("no explicit comparison tool observed")
        elif "search" in m and detected_domain == "real_estate":
            contract["brand_limitations"].append("no clear search or filter detected")
        elif "booking" in m:
            contract["brand_limitations"].append("no explicit booking flow detected")
        elif "pricing" in m or m == "no verified pricing evidence found" or m == "no explicit pricing evidence found":
            contract["brand_limitations"].append("no verified pricing evidence found")
        elif "portfolio" in m or "project" in m:
            contract["brand_limitations"].append("no explicit portfolio or project evidence found")
        elif "quote" in m or "cta" in m.lower():
            contract["brand_limitations"].append("no explicit quote/contact CTA found")
        elif "checkout" in m or "cart" in m:
            contract["brand_limitations"].append("no explicit checkout or cart action found")
        elif "shipping" in m or "fulfillment" in m:
            contract["brand_limitations"].append("no explicit shipping or fulfillment evidence found")
        elif "conversion action" in m:
            contract["brand_limitations"].append("no explicit conversion action found")
        elif "trust" in m:
            contract["brand_limitations"].append("no explicit trust evidence found")
        elif "geography" in m or "location" in m:
            contract["brand_limitations"].append("no explicit geography evidence found")
        elif "product or service" in m:
            contract["brand_limitations"].append("no explicit product or service evidence found")
        elif "certification" in m or "partnership" in m:
            contract["brand_limitations"].append("no explicit certification or partnership evidence found")

    # Clean target keyword
    contract["keyword_fit"]["target_keyword"] = target_keyword

    contract["offer_mechanics"]["supporting_services"] = _clean_evidence_items(
        contract["offer_mechanics"].get("supporting_services", []),
        category="service",
        limit=20,
    )
    contract["keyword_fit"]["relevant_brand_capabilities"] = _clean_evidence_items(
        contract["keyword_fit"].get("relevant_brand_capabilities", []),
        category="service",
        limit=20,
    )
    contract["trust_signals"] = _clean_evidence_items(
        contract.get("trust_signals", []),
        category="trust",
        limit=10,
    )
    contract["conversion_actions"] = _clean_evidence_items(
        contract.get("conversion_actions", []),
        category="cta",
        limit=10,
        allow_promotional=True,
    )
    contract["brand_identity"]["geographic_focus"] = _clean_evidence_items(
        contract["brand_identity"].get("geographic_focus", []),
        category="geography",
        limit=8,
    )

    # Deduplicate all lists
    for key, val in contract.items():
        if isinstance(val, dict):
            for sub_k, sub_v in val.items():
                if isinstance(sub_v, list):
                    contract[key][sub_k] = list(dict.fromkeys(sub_v))
        elif isinstance(val, list):
            contract[key] = list(dict.fromkeys(val))

    return contract


def build_brand_generation_guardrails(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pure builder function to generate brand guardrails.
    Deterministic, no state mutation, no network calls.
    Uses state["brand_offer_contract"].
    """
    contract = state.get("brand_offer_contract") or {}
    brand_identity = contract.get("brand_identity", {})
    confidence = brand_identity.get("confidence", "low")
    
    # 1. Base structure
    guardrails = {
        "brand_confidence": confidence,
        "brand_usage_mode": "soft_context_only" if confidence == "low" else "standard_context",
        "allowed_brand_claims": [],
        "allowed_brand_capabilities": [],
        "allowed_conversion_actions": [],
        "forbidden_brand_claims": [
            "delivery timelines",
            "response time guarantees",
            "payment gateway support",
            "testimonials or client proof",
            "portfolio claims",
            "verified/award claims",
            "local team claims",
            "custom/no-template process claims"
        ],
        "brand_section_policy": "do_not_create_dedicated_brand_proof_or_why_choose_sections"
    }

    if confidence == "low":
        # Low confidence guardrails: highly restricted
        guardrails["brand_usage_mode"] = "soft_context_only"
        guardrails["allowed_brand_claims"] = []
        guardrails["allowed_brand_capabilities"] = []
        guardrails["allowed_conversion_actions"] = []
        guardrails["brand_section_policy"] = "do_not_create_dedicated_brand_proof_or_why_choose_sections"
    else:
        # Medium/high confidence guardrails
        guardrails["brand_usage_mode"] = "standard_context"
        
        # Allowed claims from contract. Services/capabilities are operational
        # facts, not standalone proof claims, so keep them in a separate bucket.
        allowed_claims = []
        # Value propositions
        for vp in contract.get("value_propositions", []):
            allowed_claims.append(vp)
        # Trust signals
        for ts in contract.get("trust_signals", []):
            allowed_claims.append(ts)
        offer_mechanics = contract.get("offer_mechanics", {})
        allowed_capabilities = _clean_evidence_items(
            offer_mechanics.get("supporting_services", []),
            category="service",
            limit=16,
        )
            
        guardrails["allowed_brand_claims"] = _clean_evidence_items(
            list(dict.fromkeys(allowed_claims)),
            category="claim",
            limit=18,
        )
        guardrails["allowed_brand_capabilities"] = allowed_capabilities
        
        # Allowed conversion actions
        guardrails["allowed_conversion_actions"] = _clean_evidence_items(
            list(dict.fromkeys(contract.get("conversion_actions", []))),
            category="cta",
            limit=8,
            allow_promotional=True,
        )
        
        # Forbidden brand claims are anything NOT supported in allowed claims
        forbidden = []
        all_allowed_str = " ".join(allowed_claims + allowed_capabilities).lower()
        
        # Guarantees or response times
        if "response" not in all_allowed_str:
            forbidden.append("response time guarantees")
        if "delivery" not in all_allowed_str:
            forbidden.append("delivery timelines")
        if "gateway" not in all_allowed_str and "payment" not in all_allowed_str:
            forbidden.append("payment gateway support")
        if "testimonial" not in all_allowed_str and "client" not in all_allowed_str and "review" not in all_allowed_str:
            forbidden.append("testimonials or client proof")
        if "portfolio" not in all_allowed_str and "project" not in all_allowed_str:
            forbidden.append("portfolio claims")
        if "award" not in all_allowed_str and "certif" not in all_allowed_str:
            forbidden.append("verified/award claims")
        if "local" not in all_allowed_str and "team" not in all_allowed_str:
            forbidden.append("local team claims")
        if "custom" not in all_allowed_str and "no-template" not in all_allowed_str:
            forbidden.append("custom/no-template process claims")
            
        guardrails["forbidden_brand_claims"] = forbidden
        guardrails["brand_section_policy"] = "allow_brand_proof_and_why_choose_sections" if confidence == "high" else "do_not_create_dedicated_brand_proof_or_why_choose_sections"

    return guardrails


def build_brand_writing_brief(state: dict) -> dict:
    """
    Pure builder that constructs a deterministic Brand Writing Brief from state.
    Non-mutating, zero network calls, completely deterministic.
    """
    if not state:
        state = {}

    contract = state.get("brand_offer_contract") or {}
    guardrails = state.get("brand_generation_guardrails") or {}

    # 1. Derive brand name safely
    brand_name = (
        state.get("brand_name")
        or state.get("display_brand_name")
        or contract.get("brand_identity", {}).get("brand_name")
        or None
    )

    # 2. Confidence and usage policy
    confidence = guardrails.get("brand_confidence") or contract.get("brand_identity", {}).get("confidence", "low")
    usage_mode = guardrails.get("brand_usage_mode", "soft_context_only")
    section_policy = guardrails.get("brand_section_policy", "do_not_create_dedicated_brand_proof_or_why_choose_sections")

    # 3. Claim strength based on confidence
    if confidence == "low":
        allowed_claim_strength = "contextual"
    elif confidence == "medium":
        allowed_claim_strength = "operational"
    elif confidence == "high":
        allowed_claim_strength = "differentiation"
    else:
        allowed_claim_strength = "contextual"

    cards = state.get("brand_evidence_cards") or []
    page_briefs = state.get("brand_page_briefs") or []
    page_briefs = [brief for brief in page_briefs if isinstance(brief, dict)]

    # 4. Extract allowed capabilities/services. Prefer page-level grounded
    # briefs when available; they are raw-source compression and avoid the old
    # card/contract path that can promote headings or slogans into claims.
    page_services: List[str] = []
    page_capabilities: List[str] = []
    page_trust: List[str] = []
    page_ctas: List[str] = []
    if page_briefs:
        for brief in page_briefs:
            page_services.extend(str(item).strip() for item in brief.get("observed_services") or [] if str(item).strip())
            page_capabilities.extend(str(item).strip() for item in brief.get("observed_technologies") or [] if str(item).strip())
            page_capabilities.extend(str(item).strip() for item in brief.get("observed_process_steps") or [] if str(item).strip())
            page_trust.extend(str(item).strip() for item in brief.get("observed_trust_signals") or [] if str(item).strip())
            page_ctas.extend(str(item).strip() for item in brief.get("observed_ctas") or [] if str(item).strip())

    if page_briefs:
        service_source = page_services
        capability_source = page_capabilities
        trust_source = page_trust
        cta_source = page_ctas
        value_prop_source: List[str] = []
    else:
        service_source = (
            (contract.get("offer_mechanics", {}).get("supporting_services", []) or [])
            + _collect_card_values(cards, ["visible_products_or_services"], limit=12, category="service")
        )
        capability_source = (
            (contract.get("keyword_fit", {}).get("relevant_brand_capabilities", []) or [])
            + _collect_card_values(cards, ["visible_features_or_capabilities"], limit=12, category="capability")
        )
        trust_source = contract.get("trust_signals", []) or []
        cta_source = contract.get("conversion_actions", []) or []
        value_prop_source = contract.get("value_propositions", []) or []

    allowed_services = _clean_evidence_items(
        service_source,
        limit=12,
        category="service",
    )
    allowed_capabilities = _clean_evidence_items(
        capability_source,
        limit=12,
        category="capability",
    )
    allowed_value_props = _clean_evidence_items(value_prop_source, limit=8)
    allowed_trust_signals = _clean_evidence_items(
        trust_source,
        limit=8,
        category="trust",
        allow_promotional=False,
    )
    allowed_conversion_actions = _clean_evidence_items(cta_source, limit=8, category="cta", allow_promotional=True)

    # Combine allowed claims (limited in low confidence)
    if confidence == "low":
        allowed_claims = []
    else:
        allowed_claims = list(dict.fromkeys(
            allowed_services + allowed_capabilities + allowed_value_props + allowed_trust_signals
        ))
    if confidence == "high" and not (allowed_value_props or allowed_trust_signals):
        allowed_claim_strength = "operational"

    # Forbidden claim categories
    forbidden_claim_categories = [
        "guarantees",
        "delivery timelines",
        "customer satisfaction claims",
        "testimonials",
        "response times",
        "project counts",
        "certifications",
        "partnerships",
        "pricing claims",
        "success metrics",
        "unsupported technical capabilities",
        "awards",
        "verified claims unless explicitly supported"
    ]

    preferred_brand_tone = [
        "service-focused",
        "evidence-aware",
        "non-promotional",
        "operational"
    ]

    # Dynamic allowed heading patterns
    keyword = state.get("primary_keyword", "")
    allowed_heading_patterns = []
    if confidence in ["medium", "high"] and brand_name:
        allowed_heading_patterns = [
            f"How {brand_name} Supports Businesses Looking For {keyword}",
            f"Services Offered By {brand_name} For {keyword}",
            f"What {brand_name} Provides For {keyword}"
        ]
        if allowed_claim_strength == "differentiation" and (allowed_value_props or allowed_trust_signals):
            allowed_heading_patterns.extend([
                f"Evidence-backed advantages available from {brand_name}",
                f"Observed trust signals for {brand_name}"
            ])

    forbidden_heading_patterns = []
    if brand_name:
        forbidden_heading_patterns = [
            f"Why choose {brand_name}?",
            f"Why customers trust {brand_name}?",
            f"Best {brand_name} for {keyword}",
            f"{brand_name}: the leading choice",
            f"\u0644\u0645\u0627\u0630\u0627 \u062a\u062e\u062a\u0627\u0631 {brand_name}\u061f",
            f"\u0644\u0645\u0627\u0630\u0627 \u064a\u062b\u0642 \u0627\u0644\u0639\u0645\u0644\u0627\u0621 \u0641\u064a {brand_name}\u061f",
            f"\u0623\u0641\u0636\u0644 {brand_name} \u0644\u0640 {keyword}",
            f"{brand_name} \u0627\u0644\u062e\u064a\u0627\u0631 \u0627\u0644\u0623\u0641\u0636\u0644"
        ]

    # Section guidance
    section_guidance = []
    if confidence == "low":
        section_guidance.append("Do not create dedicated brand sections. Keep brand mentions contextual and neutral.")
    elif confidence == "medium":
        section_guidance.append("Focus on operational alignment. Highlight evidence-supported capabilities and services using neutral phrasing.")
    elif allowed_claim_strength == "operational":
        section_guidance.append("Use operational service framing only. Do not use differentiation or trust framing without trust signals or value propositions.")
    else:
        section_guidance.append("Use evidence-grounded differentiation only. Do not use generic promotional headings like Why choose, leading choice, or best brand claims.")
    section_guidance.append("Brand headings must match observed evidence. Do not claim geography, trust, pricing, guarantees, timelines, certifications, partnerships, testimonials, or project counts unless explicitly supported.")

    # Evidence notes
    internal_resources = state.get("internal_resources") or []
    evidence_notes = [
        f"Crawled {len(internal_resources)} resources successfully.",
        f"Confidence is graded as {confidence} based on explicit signal count."
    ]

    return {
        "brand_name": brand_name,
        "evidence_confidence": confidence,
        "brand_usage_mode": usage_mode,
        "brand_section_policy": section_policy,
        "allowed_claim_strength": allowed_claim_strength,
        "preferred_brand_tone": preferred_brand_tone,
        "allowed_services": allowed_services,
        "allowed_capabilities": allowed_capabilities,
        "allowed_value_props": allowed_value_props,
        "allowed_trust_signals": allowed_trust_signals,
        "allowed_conversion_actions": allowed_conversion_actions,
        "allowed_claims": allowed_claims,
        "forbidden_claim_categories": forbidden_claim_categories,
        "allowed_heading_patterns": allowed_heading_patterns,
        "forbidden_heading_patterns": forbidden_heading_patterns,
        "section_guidance": section_guidance,
        "claim_strength_rules": {
            "low": "Only mention brand contextually. Do not make claims about operational metrics, timelines, guarantees, or custom capabilities.",
            "medium": "Allow evidence-grounded service/workflow claims. Heading phrasing must remain operational, not promotional.",
            "high": "Allow differentiation framing when explicitly backed by observed trust signals or value props. Promotional metrics are still blocked."
        },
        "evidence_notes": evidence_notes
    }


def format_brand_writing_brief_context(brief: dict) -> str:
    """
    Formats the writing brief into a readable string clearly marked for prompt boundaries.
    """
    if not brief:
        return ""

    brand_name = brief.get("brand_name") or "N/A"
    confidence = brief.get("evidence_confidence", "low")
    usage_mode = brief.get("brand_usage_mode", "soft_context_only")
    section_policy = brief.get("brand_section_policy", "")
    allowed_strength = brief.get("allowed_claim_strength", "contextual")
    tone = ", ".join(brief.get("preferred_brand_tone", []))
    
    allowed_services = ", ".join(_clean_evidence_items(brief.get("allowed_services", []), limit=10)) or "None"
    allowed_claims = ", ".join(_clean_evidence_items(brief.get("allowed_claims", []), limit=10)) or "None"
    allowed_ctas = ", ".join(_clean_evidence_items(brief.get("allowed_conversion_actions", []), limit=6)) or "None"
    forbidden = ", ".join(brief.get("forbidden_claim_categories", []))
    headings = "; ".join(brief.get("allowed_heading_patterns", [])) or "None"
    forbidden_headings = "; ".join(brief.get("forbidden_heading_patterns", [])) or "None"
    guidance = " ".join(brief.get("section_guidance", []))

    return f"""
[BRAND WRITING BRIEF - USE AS CLAIM BOUNDARY, NOT BRAND DESCRIPTION]
- Brand Name: {brand_name}
- Evidence Confidence: {confidence}
- Brand Usage Mode: {usage_mode}
- Brand Section Policy: {section_policy}
- Allowed Claim Strength: {allowed_strength}
- Preferred Tone: {tone}
- Allowed Services: {allowed_services}
- Allowed Claims: {allowed_claims}
- Allowed CTAs: {allowed_ctas}
- Forbidden Claim Categories: {forbidden}
- Allowed Heading Patterns: {headings}
- Forbidden Heading Patterns: {forbidden_headings}
- Section Guidance: {guidance}
"""


def _legacy_apply_brand_claim_gate_unpatched(text: str, brief: dict) -> str:
    """
    Deterministic claim gate that removes or softens unsupported brand claims.
    Acts sentence-by-sentence inside paragraph blocks to preserve formatting and newlines.
    Section-level aware: tracks markdown headings to gate sentences inside brand-sensitive sections.
    """
    if not text or not brief:
        return text

    brand_name = brief.get("brand_name")
    if not brand_name:
        return text

    import re

    # Recursive helper to extract all string values from nested dict/list
    def extract_dict_values(d) -> list[str]:
        vals = []
        if isinstance(d, dict):
            for k, v in d.items():
                vals.extend(extract_dict_values(v))
        elif isinstance(d, list):
            for item in d:
                vals.extend(extract_dict_values(item))
        elif isinstance(d, (str, int, float)):
            vals.append(str(d))
        return vals

    # Build combined allowed string for searching supported claims
    allowed_sources = []
    
    # 1. brief allowed fields
    for field in ["allowed_claims", "allowed_services", "allowed_capabilities", "allowed_trust_signals", "allowed_conversion_actions"]:
        val = brief.get(field)
        if isinstance(val, list):
            allowed_sources.extend([str(item).lower() for item in val if item])
        elif isinstance(val, str):
            allowed_sources.append(val.lower())
            
    # 2. brand_offer_contract recursively extracted values
    contract = brief.get("brand_offer_contract") or {}
    allowed_sources.extend([val.lower() for val in extract_dict_values(contract) if val])
        
    # 3. section_source_text
    section_source_text = brief.get("section_source_text") or ""
    if section_source_text:
        allowed_sources.append(section_source_text.lower())
        
    combined_allowed_str = " ".join(allowed_sources)

    # Patterns mapped to validation search terms in combined_allowed_str
    forbidden_categories = [
        {
            "name": "guarantees",
            "patterns": [
                r"\bguarante[esd]*\b", r"\bwarrant[iesy]*\b", r"\b100%\b",
                r"يضمن", r"ضمان"
            ],
            "validation_terms": ["guarante", "warrant", "100%", "يضمن", "ضمان", "satisfaction"]
        },
        {
            "name": "delivery timelines",
            "patterns": [
                r"\bwithin \d+ (?:day|hour|week|month)s?\b",
                r"\bin \d+ (?:day|hour|week|month)s?\b",
                r"\bfast delivery\b",
                r"خلال \d+", r"سرعة التسليم"
            ],
            "validation_terms": ["delivery", "timeline", "خلال", "تسليم", "within", "day", "hour", "week", "month", "in ", "fast"]
        },
        {
            "name": "response times",
            "patterns": [
                r"\b24/7\b", r"\bresponse time[s]?\b", r"\brespond within\b",
                r"\bfast response\b", r"سرعة الاستجابة", r"الاستجابة السريعة"
            ],
            "validation_terms": ["response", "24/7", "respond", "fast", "استجابة", "سرعة"]
        },
        {
            "name": "customer satisfaction",
            "patterns": [
                r"\bcustomer satisfaction\b", r"\bsatisfaction guaranteed\b", r"رضا العملاء"
            ],
            "validation_terms": ["satisfaction", "satisfy", "رضا", "عملاء"]
        },
        {
            "name": "testimonials",
            "patterns": [
                r"\btestimonial[s]?\b", r"\bclient proof\b", r"\breview[s]?\b",
                r"\brating[s]?\b", r"\btrusted by\b", r"شهادات العملاء", r"آراء العملاء"
            ],
            "validation_terms": ["testimonial", "review", "rating", "trust", "proof", "شهادة", "آراء", "عملاء"]
        },
        {
            "name": "project counts",
            "patterns": [
                r"\b(?:more than|over) \d+ (?:project|client|website|customer)s?\b",
                r"\bproject count[s]?\b", r"مشاريع كثيرة", r"مئات المشاريع"
            ],
            "validation_terms": ["project", "client", "website", "customer", "count", "مشاريع", "عملاء", "أعمال"]
        },
        {
            "name": "certifications",
            "patterns": [
                r"\bcertified\b", r"\bcertification\b", r"\baccredited\b", r"معتمد"
            ],
            "validation_terms": ["certified", "certification", "accredited", "معتمد", "اعتماد"]
        },
        {
            "name": "partnerships",
            "patterns": [
                r"\bpartner\b", r"\bpartnership\b", r"شراكات", r"شريك"
            ],
            "validation_terms": ["partner", "partnership", "شريك", "شراكة", "شراكات"]
        },
        {
            "name": "pricing claims",
            "patterns": [
                r"\bprices? start(?:s)?\b", r"\bstarting at\b", r"\bstarting from\b",
                r"\bcheap prices?\b", r"\blowest prices?\b", r"أسعار مضمونة", r"أرخص الأسعار", r"أقل الأسعار"
            ],
            "validation_terms": ["price", "pricing", "start", "cheap", "lowest", "سعر", "أسعار", "تكلفة", "أرخص", "أقل"]
        },
        {
            "name": "success metrics",
            "patterns": [
                r"\bsuccess metric[s]?\b", r"\bsuccess rate\b", r"نتائج مؤكدة"
            ],
            "validation_terms": ["success", "metric", "rate", "نتائج", "نجاح", "مؤكد"]
        },
        {
            "name": "market leadership",
            "patterns": [
                r"\bleading choice\b", r"\bmarket leader\b", r"\bleading company\b",
                r"\btop agency\b", r"الأعلى", r"الأقوى", r"الأكثر ثقة", r"الأول"
            ],
            "validation_terms": ["leader", "leading", "top", "أعلى", "أقوى", "أول", "الأقوى", "الأول", "الأعلى"]
        },
        {
            "name": "strongest / best / top / trusted claims",
            "patterns": [
                r"\bstrongest\b", r"\bbest\b", r"\btop\b", r"\btrusted\b",
                r"أفضل", r"الأفضل", r"موثوق"
            ],
            "validation_terms": ["best", "top", "trust", "strong", "أفضل", "الأفضل", "موثوق"]
        },
        {
            "name": "geography / market presence",
            "patterns": [
                r"\bRiyadh\b", r"\bCairo\b", r"\bSaudi\b", r"\bEgypt\b",
                r"السعودية", r"الرياض", r"مصر"
            ],
            "validation_terms": ["riyadh", "cairo", "saudi", "egypt", "السعودية", "الرياض", "مصر"]
        }
    ]

    brand_name_lower = brand_name.lower()
    brand_aliases = brief.get("brand_aliases") or []
    if isinstance(brand_aliases, str):
        brand_aliases = [brand_aliases]
    brand_aliases_lower = [alias.lower() for alias in brand_aliases if alias]
    
    def mentions_brand(s: str) -> bool:
        s_lower = s.lower()
        if brand_name_lower in s_lower:
            return True
        for alias in brand_aliases_lower:
            if alias in s_lower:
                return True
        return False

    lines = text.split("\n")
    processed_lines = []
    
    active_brand_heading_level = None

    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            processed_lines.append("")
            continue
            
        # Detect markdown heading
        heading_match = re.match(r"^(#{1,6})\s+(.*)$", line_stripped)
        if heading_match:
            hashes = heading_match.group(1)
            level = len(hashes)
            heading_text = heading_match.group(2)
            
            # Close active brand heading block if level is same or higher
            if active_brand_heading_level is not None and level <= active_brand_heading_level:
                active_brand_heading_level = None
                
            # Start brand heading block if heading mentions the brand
            if mentions_brand(heading_text):
                active_brand_heading_level = level
                
            processed_lines.append(line)
            continue

        # Detect bullet list item
        bullet_match = re.match(r"^(\s*[-*+]\s+|\s*\d+\.\s+)(.*)$", line)
        if bullet_match:
            prefix = bullet_match.group(1)
            content = bullet_match.group(2)
        else:
            prefix = ""
            content = line

        sentences = re.split(r'(?<=[.!?])\s+', content)
        kept_sentences = []
        
        for s in sentences:
            s_stripped = s.strip()
            if not s_stripped:
                continue
                
            is_brand_sensitive = (active_brand_heading_level is not None) or mentions_brand(s_stripped)
            
            if is_brand_sensitive:
                violation = False
                for cat in forbidden_categories:
                    cat_matched = False
                    for pattern in cat["patterns"]:
                        if re.search(pattern, s_stripped, re.IGNORECASE):
                            cat_matched = True
                            break
                            
                    if cat_matched:
                        supported = False
                        for term in cat["validation_terms"]:
                            if term.lower() in combined_allowed_str:
                                supported = True
                                break
                                
                        if not supported:
                            violation = True
                            break
                            
                if violation:
                    continue
                    
            kept_sentences.append(s)

        if kept_sentences:
            reconstructed_content = " ".join(kept_sentences).strip()
            processed_lines.append(prefix + reconstructed_content)
        else:
            if not prefix:
                processed_lines.append("")

    final_lines = []
    for i, line in enumerate(processed_lines):
        if line == "":
            if i > 0 and processed_lines[i-1] != "":
                final_lines.append("")
        else:
            final_lines.append(line)
            
    return "\n".join(final_lines).strip()


def apply_brand_claim_gate(text: str, brief: dict) -> str:
    """
    Deterministic claim gate for brand copy.

    It removes only offending sentences, preserves paragraph/list/heading
    structure, and treats a markdown section as brand-sensitive when its heading
    mentions the brand.
    """
    if not text or not brief:
        return text

    import re

    brand_name = str(brief.get("brand_name") or "").strip()
    aliases = brief.get("brand_aliases") or []
    if isinstance(aliases, str):
        aliases = [aliases]
    brand_terms = [term.casefold() for term in [brand_name] + aliases if str(term).strip()]
    if not brand_terms:
        return text

    def iter_strings(value):
        if isinstance(value, dict):
            for item in value.values():
                yield from iter_strings(item)
        elif isinstance(value, (list, tuple, set)):
            for item in value:
                yield from iter_strings(item)
        elif isinstance(value, (str, int, float)):
            text_value = str(value).strip()
            if text_value:
                yield text_value

    allowed_values = []
    for field in [
        "allowed_claims",
        "allowed_services",
        "allowed_capabilities",
        "allowed_trust_signals",
        "allowed_conversion_actions",
    ]:
        allowed_values.extend(iter_strings(brief.get(field)))
    allowed_values.extend(iter_strings(brief.get("brand_offer_contract") or {}))
    allowed_values.extend(iter_strings(brief.get("section_source_text") or ""))

    allowed_norm = [value.casefold() for value in allowed_values if str(value).strip()]

    token_re = re.compile(r"[\w\u0600-\u06FF$%+/]+", re.UNICODE)
    stop_words = {
        "the", "a", "an", "and", "or", "for", "with", "from", "this", "that",
        "has", "have", "offers", "provides", "provide", "is", "are", "by",
        "في", "من", "على", "عن", "مع", "هذا", "هذه", "التي", "الذي", "أن",
    }

    def tokens(value: str) -> set[str]:
        return {
            tok.casefold()
            for tok in token_re.findall(value or "")
            if len(tok) > 1 and tok.casefold() not in stop_words
        }

    def mentions_brand(value: str) -> bool:
        folded = (value or "").casefold()
        return any(term in folded for term in brand_terms)

    forbidden_categories = [
        {
            "name": "guarantees",
            "patterns": [
                r"\bguarantee(?:s|d)?\b", r"\bwarrant(?:y|ies)\b", r"\b100%\s+satisfaction\b",
                r"\bguaranteed\b", r"\bguaranteed results\b",
                r"\u064a\u0636\u0645\u0646", r"\u062a\u0636\u0645\u0646", r"\u0636\u0645\u0627\u0646", r"\u0645\u0636\u0645\u0648\u0646(?:\u0629)?",
            ],
            "support_terms": ["guarantee", "warranty", "100%", "satisfaction", "ضمان", "مضمون"],
        },
        {
            "name": "delivery timelines",
            "patterns": [
                r"\bwithin\s+\d+\s+(?:day|hour|week|month)s?\b",
                r"\bin\s+\d+\s+(?:day|hour|week|month)s?\b",
                r"\bfast delivery\b", r"\bsame[- ]day delivery\b",
                r"\u062e\u0644\u0627\u0644\s+\d+", r"\u0641\u064a\s+\d+\s+(?:\u064a\u0648\u0645|\u0623\u064a\u0627\u0645|\u0633\u0627\u0639\u0629)",
                r"\u0633\u0631\u0639\u0629 \u0627\u0644\u062a\u0633\u0644\u064a\u0645", r"\u062a\u0633\u0644\u064a\u0645 \u0633\u0631\u064a\u0639",
            ],
            "support_terms": ["delivery", "within", "timeline", "day", "hour", "week", "month", "تسليم", "خلال"],
        },
        {
            "name": "response times",
            "patterns": [
                r"\b24/7\b", r"\bresponse time(?:s)?\b", r"\brespond within\b", r"\bfast response\b",
                r"\u0633\u0631\u0639\u0629 \u0627\u0644\u0627\u0633\u062a\u062c\u0627\u0628\u0629",
                r"\u0627\u0633\u062a\u062c\u0627\u0628\u0629 \u0633\u0631\u064a\u0639\u0629", r"\u0646\u0631\u062f \u062e\u0644\u0627\u0644",
            ],
            "support_terms": ["response", "24/7", "respond", "استجابة"],
        },
        {
            "name": "customer satisfaction",
            "patterns": [
                r"\bcustomer satisfaction\b", r"\bsatisfaction guaranteed\b",
                r"\u0631\u0636\u0627 \u0627\u0644\u0639\u0645\u0644\u0627\u0621", r"\u0631\u0636\u0627\u0621 \u0627\u0644\u0639\u0645\u0644\u0627\u0621",
            ],
            "support_terms": ["satisfaction", "رضا", "رضاء"],
        },
        {
            "name": "testimonials",
            "patterns": [
                r"\btestimonial(?:s)?\b", r"\bclient proof\b", r"\breview(?:s)?\b",
                r"\brating(?:s)?\b", r"\btrusted by\b",
                r"\u0634\u0647\u0627\u062f\u0627\u062a \u0627\u0644\u0639\u0645\u0644\u0627\u0621",
                r"\u0622\u0631\u0627\u0621 \u0627\u0644\u0639\u0645\u0644\u0627\u0621", r"\u062a\u0642\u064a\u064a\u0645\u0627\u062a",
            ],
            "support_terms": ["testimonial", "review", "rating", "trusted by", "شهادات", "آراء", "تقييم"],
        },
        {
            "name": "project counts",
            "patterns": [
                r"\b(?:more than|over)\s+\d+\s+(?:project|client|website|customer)s?\b",
                r"\b\d+\+?\s+(?:project|client|website|customer)s?\b",
                r"\bproject count(?:s)?\b",
                r"\d+\+?\s+(?:\u0645\u0634\u0631\u0648\u0639|\u0639\u0645\u064a\u0644|\u0645\u0648\u0642\u0639)",
                r"\u0645\u0634\u0627\u0631\u064a\u0639 \u0643\u062b\u064a\u0631\u0629", r"\u0645\u0626\u0627\u062a \u0627\u0644\u0645\u0634\u0627\u0631\u064a\u0639",
            ],
            "support_terms": ["project", "client", "website", "customer", "مشروع", "عميل", "موقع"],
        },
        {
            "name": "certifications",
            "patterns": [
                r"\bcertified\b", r"\bcertification\b", r"\baccredited\b", r"\blicensed\b",
                r"\u0645\u0639\u062a\u0645\u062f", r"\u0627\u0639\u062a\u0645\u0627\u062f", r"\u0645\u0631\u062e\u0635", r"\u0634\u0647\u0627\u062f\u0629",
            ],
            "support_terms": ["certified", "certification", "accredited", "licensed", "معتمد", "اعتماد", "مرخص", "شهادة"],
        },
        {
            "name": "partnerships",
            "patterns": [
                r"\bpartner(?:s)?\b", r"\bpartnership(?:s)?\b",
                r"\u0634\u0631\u0627\u0643\u0627\u062a", r"\u0634\u0631\u0627\u0643\u0629", r"\u0634\u0631\u064a\u0643",
            ],
            "support_terms": ["partner", "partnership", "شريك", "شراكة", "شراكات"],
        },
        {
            "name": "pricing claims",
            "patterns": [
                r"\bpricing\s+(?:starts?|starting)\b", r"\bprices?\s+start(?:s)?\b",
                r"\bstarting\s+(?:at|from)\b", r"\bfrom\s+[$€£]?\d+",
                r"\bcheap prices?\b", r"\blowest prices?\b",
                r"\u0623\u0633\u0639\u0627\u0631 \u062a\u0628\u062f\u0623", r"\u062a\u0628\u062f\u0623 \u0627\u0644\u0623\u0633\u0639\u0627\u0631",
                r"\u0623\u0633\u0639\u0627\u0631 \u0645\u0636\u0645\u0648\u0646\u0629", r"\u0623\u0631\u062e\u0635 \u0627\u0644\u0623\u0633\u0639\u0627\u0631", r"\u0623\u0642\u0644 \u0627\u0644\u0623\u0633\u0639\u0627\u0631",
            ],
            "support_terms": ["price", "pricing", "$", "€", "£", "sar", "ريال", "سعر", "أسعار", "تكلفة"],
        },
        {
            "name": "success metrics",
            "patterns": [
                r"\bsuccess metric(?:s)?\b", r"\bsuccess rate\b", r"\bproven results\b", r"\bguaranteed results\b",
                r"\u0646\u062a\u0627\u0626\u062c \u0645\u0624\u0643\u062f\u0629", r"\u0646\u0633\u0628\u0629 \u0646\u062c\u0627\u062d", r"\u0646\u062c\u0627\u062d \u0645\u0636\u0645\u0648\u0646",
            ],
            "support_terms": ["success", "metric", "rate", "results", "نتائج", "نجاح"],
        },
        {
            "name": "market leadership",
            "patterns": [
                r"\bleading choice\b", r"\bmarket leader\b", r"\bleading company\b", r"\btop agency\b", r"\b#1\b",
                r"\u0627\u0644\u0634\u0631\u0643\u0629 \u0627\u0644\u0631\u0627\u0626\u062f\u0629", r"\u0627\u0644\u0631\u0627\u0626\u062f",
                r"\u0627\u0644\u062e\u064a\u0627\u0631 \u0627\u0644\u0623\u0648\u0644", r"\u0627\u0644\u0623\u0639\u0644\u0649", r"\u0627\u0644\u0623\u0642\u0648\u0649",
            ],
            "support_terms": ["leader", "leading", "top", "#1", "رائد", "الرائدة", "الأول", "الأعلى", "الأقوى"],
        },
        {
            "name": "strongest / best / top / trusted claims",
            "patterns": [
                r"\bstrongest\b", r"\bbest\b", r"\btop\b", r"\btrusted\b", r"\bmost trusted\b",
                r"\u0623\u0641\u0636\u0644", r"\u0627\u0644\u0623\u0641\u0636\u0644", r"\u0645\u0648\u062b\u0648\u0642", r"\u0645\u0648\u062b\u0648\u0642\u0629",
                r"\u0627\u0644\u0623\u0643\u062b\u0631 \u062b\u0642\u0629",
            ],
            "support_terms": ["best", "top", "trusted", "strongest", "أفضل", "الأفضل", "موثوق", "ثقة"],
        },
        {
            "name": "geography / market presence",
            "patterns": [
                r"\bin\s+(?:riyadh|cairo|saudi arabia|saudi|egypt|jeddah)\b",
                r"\bacross\s+(?:saudi arabia|saudi|egypt)\b",
                r"\bserv(?:e|es|ing)\s+(?:riyadh|cairo|saudi arabia|saudi|egypt|jeddah)\b",
                r"\u0641\u064a \u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629", r"\u0641\u064a \u0627\u0644\u0631\u064a\u0627\u0636", r"\u0641\u064a \u0645\u0635\u0631",
                r"\u0641\u064a \u062c\u062f\u0629", r"\u0641\u064a \u0627\u0644\u0642\u0627\u0647\u0631\u0629", r"\u0628\u0627\u0644\u0645\u0645\u0644\u0643\u0629",
            ],
            "support_terms": ["riyadh", "cairo", "saudi", "egypt", "jeddah", "الرياض", "القاهرة", "السعودية", "مصر", "جدة"],
        },
    ]

    def category_is_supported(sentence: str, category: dict) -> bool:
        sentence_norm = sentence.casefold()
        sentence_tokens = tokens(sentence_norm)
        sentence_numbers = set(re.findall(r"\d+(?:\.\d+)?", sentence_norm))

        for source in allowed_norm:
            if not source:
                continue
            source_has_support_term = any(term.casefold() in source for term in category["support_terms"])
            if len(source) >= 4 and source_has_support_term and (source in sentence_norm or sentence_norm in source):
                return True

            source_tokens = tokens(source)
            overlap = sentence_tokens.intersection(source_tokens)
            if category["name"] == "geography / market presence":
                if any(term.casefold() in source and term.casefold() in sentence_norm for term in category["support_terms"]):
                    return True
            if len(overlap) >= 2 and source_has_support_term:
                return True

            if sentence_numbers and sentence_numbers.intersection(set(re.findall(r"\d+(?:\.\d+)?", source))):
                if source_has_support_term:
                    return True

        return False

    def sentence_violates(sentence: str) -> bool:
        for category in forbidden_categories:
            if any(re.search(pattern, sentence, re.IGNORECASE) for pattern in category["patterns"]):
                if not category_is_supported(sentence, category):
                    return True
        return False

    sentence_split_re = re.compile(r"(?<=[.!?\u061f])\s+")
    heading_re = re.compile(r"^(#{1,6})\s+(.*)$")
    bullet_re = re.compile(r"^(\s*[-*+]\s+|\s*\d+\.\s+)(.*)$")

    processed_lines = []
    active_brand_heading_level = None

    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped:
            processed_lines.append("")
            continue

        heading_match = heading_re.match(stripped)
        if heading_match:
            level = len(heading_match.group(1))
            heading_text = heading_match.group(2)
            if active_brand_heading_level is not None and level <= active_brand_heading_level:
                active_brand_heading_level = None
            if mentions_brand(heading_text):
                active_brand_heading_level = level
            processed_lines.append(line)
            continue

        bullet_match = bullet_re.match(line)
        prefix = bullet_match.group(1) if bullet_match else ""
        content = bullet_match.group(2) if bullet_match else line
        is_brand_sensitive_line = active_brand_heading_level is not None

        kept = []
        for sentence in sentence_split_re.split(content):
            clean_sentence = sentence.strip()
            if not clean_sentence:
                continue
            is_sensitive = is_brand_sensitive_line or mentions_brand(clean_sentence)
            if is_sensitive and sentence_violates(clean_sentence):
                continue
            kept.append(clean_sentence)

        if kept:
            processed_lines.append(prefix + " ".join(kept))

    final_lines = []
    previous_blank = False
    for line in processed_lines:
        is_blank = line == ""
        if is_blank and previous_blank:
            continue
        final_lines.append(line)
        previous_blank = is_blank

    return "\n".join(final_lines).strip()


def _legacy_build_brand_evidence_cards_unpatched(state: dict) -> list[dict]:
    """
    Pure deterministic function. No network, no logging, no state mutation.
    Parses state["internal_resources"] to construct structured evidence cards.
    """
    resources = state.get("internal_resources", []) or []
    cards = []
    
    for res in resources:
        url = res.get("link", "")
        title = res.get("title")
        headings = res.get("headings", []) or []
        cta_labels = res.get("cta_labels", []) or []
        page_text = res.get("page_text", "") or ""
        
        # 1. Detect page type
        page_type = "other"
        url_lower = url.lower()
        title_lower = (title or "").lower()
        headings_lower = [h.lower() for h in headings]
        
        # Check contact
        if any(k in url_lower for k in ["contact", "get-in-touch", "تواصل", "اتصل"]):
            page_type = "contact"
        elif any(k in title_lower or any(k in h for h in headings_lower) for k in ["contact us", "تواصل معنا", "اتصل بنا"]):
            page_type = "contact"
            
        # Check services
        elif any(k in url_lower for k in ["services", "service", "%d8%ae%d8%af%d9%85%d8%a7%d8%aa", "خدمات", "خدمة"]):
            page_type = "services"
        elif any(k in title_lower or any(k in h for h in headings_lower) for k in ["services", "خدماتنا", "خدمات"]):
            page_type = "services"
            
        # Check product
        elif any(k in url_lower for k in ["products", "product", "shop", "store", "منتجات", "منتج", "متجر"]):
            page_type = "product"
        elif any(k in title_lower or any(k in h for h in headings_lower) for k in ["products", "منتجاتنا", "منتجات", "متجر"]):
            page_type = "product"
            
        # Check portfolio
        elif any(k in url_lower for k in ["portfolio", "projects", "case-study", "case-studies", "work", "اعمالنا", "أعمالنا", "مشاريعنا"]):
            page_type = "portfolio"
        elif any(k in title_lower or any(k in h for h in headings_lower) for k in ["portfolio", "our work", "أعمالنا", "معرض الأعمال", "مشاريعنا"]):
            page_type = "portfolio"
            
        # Check about
        elif any(k in url_lower for k in ["about", "who-we-are", "من-نحن", "من_نحن", "نبذة"]):
            page_type = "about"
        elif any(k in title_lower or any(k in h for h in headings_lower) for k in ["about us", "who we are", "من نحن", "عن الشركة"]):
            page_type = "about"
            
        # Check blog
        elif any(k in url_lower for k in ["blog", "news", "articles", "مدونة", "أخبار", "اخبار", "مقالات"]):
            page_type = "blog"
        elif any(k in title_lower or any(k in h for h in headings_lower) for k in ["blog", "news", "articles", "مدونة", "المقالات"]):
            page_type = "blog"
            
        # Check pricing
        elif any(k in url_lower for k in ["pricing", "packages", "أسعار", "اسعار", "باقات"]):
            page_type = "pricing"
        elif any(k in title_lower or any(k in h for h in headings_lower) for k in ["pricing", "packages", "الأسعار", "الباقات"]):
            page_type = "pricing"
            
        # Check home
        elif url_lower.rstrip('/') == urlparse(url_lower).scheme + "://" + urlparse(url_lower).netloc or any(k in url_lower for k in ["/home", "index.html", "index.php"]):
            page_type = "home"
        elif any(k in title_lower for k in ["home", "homepage", "الرئيسية", "الصفحة الرئيسية"]):
            page_type = "home"
            
        # 2. Traceable extractions from visible text
        # Visible products or services
        visible_products_or_services = []
        if page_type in ["services", "home", "product"]:
            for h in headings:
                h_clean = h.strip()
                if len(h_clean) > 3 and not any(k in h_clean.lower() for k in ["about", "contact", "pricing", "blog", "portfolio", "why choose", "من نحن", "تواصل"]):
                    visible_products_or_services.append(h_clean)
                    
        services_regex = r"(?:we offer|we provide|specializes? in|خدماتنا|نقدم|متخصصون في)\s+([^.\n]+)"
        for match in re.finditer(services_regex, page_text, re.IGNORECASE):
            phrases = re.split(r",|و|-|and", match.group(1))
            for p in phrases:
                p_clean = p.strip()
                if 3 < len(p_clean) < 50:
                    visible_products_or_services.append(p_clean)
        visible_products_or_services = list(dict.fromkeys(visible_products_or_services))
        
        # Visible features or capabilities
        visible_features_or_capabilities = []
        features_patterns = [
            r"responsive\s+design|responsive\s+layout|mobile-friendly|fast\s+loading|seo\s+optimized|secure\s+payment",
            r"تصميم\s+متجاوب|سرعة\s+تصفح|تهيئة\s+محركات\s+البحث|أمان|حماية|دفع\s+آمن"
        ]
        for pat in features_patterns:
            for match in re.finditer(pat, page_text, re.IGNORECASE):
                visible_features_or_capabilities.append(match.group(0).strip())
        for line in page_text.split("."):
            line = line.strip()
            if any(k in line.lower() for k in ["features", "capabilities", "ميزات", "خصائص"]):
                if len(line) < 150:
                    visible_features_or_capabilities.append(line)
        visible_features_or_capabilities = list(dict.fromkeys(visible_features_or_capabilities))
        
        # Visible process steps
        visible_process_steps = []
        process_patterns = [
            r"(?:step|phase|الخطوة|المرحلة)\s*\d+[:\-\s]*([^\n.]+)",
            r"(?:طريقة العمل|خطوات العمل|طريقتنا|كيف نعمل)[:\-\s]*([^\n.]+)"
        ]
        for pat in process_patterns:
            for match in re.finditer(pat, page_text, re.IGNORECASE):
                visible_process_steps.append(match.group(0).strip())
        visible_process_steps = list(dict.fromkeys(visible_process_steps))
        
        # Visible conversion actions
        visible_conversion_actions = []
        conversion_keywords = [
            "book", "quote", "get a demo", "order", "purchase", "buy", "contact us", "subscribe",
            "احجز", "اطلب", "شراء", "تواصل معنا", "طلب سعر", "تسجيل"
        ]
        for cta in cta_labels:
            if any(k in cta.lower() for k in conversion_keywords):
                visible_conversion_actions.append(cta.strip())
        visible_conversion_actions = list(dict.fromkeys(visible_conversion_actions))
        
        # Visible trust signals
        visible_trust_signals = []
        trust_patterns = [
            r"\b\d+\+?\s*(?:[\w\-]+\s+)?(?:projects|clients|customers|years|employees|مشاريع|عملاء|سنة|سنوات)\b",
            r"\b(?:trusted by|reviews|rating|شريك|تقييم|خبرة)\b",
            r"(?:award-winning|certified|معتمد|حائز على جوائز)"
        ]
        for pat in trust_patterns:
            for match in re.finditer(pat, page_text, re.IGNORECASE):
                visible_trust_signals.append(match.group(0).strip())
        visible_trust_signals = list(dict.fromkeys(visible_trust_signals))
        
        # Visible geography
        visible_geography = []
        geo_keywords = [
            "الرياض", "جدة", "الدمام", "مكة", "المدينة", "الخبر", "السعودية", "المملكة العربية السعودية", "الخليج",
            "Riyadh", "Jeddah", "Dammam", "Khobar", "Saudi Arabia", "KSA", "Dubai", "دبي", "Cairo", "القاهرة", "Egypt", "مصر"
        ]
        for kw in geo_keywords:
            pattern = rf"\b{re.escape(kw)}\b"
            if re.search(pattern, page_text) or re.search(pattern, title_lower) or any(re.search(pattern, h.lower()) for h in headings):
                visible_geography.append(kw)
        visible_geography = list(dict.fromkeys(visible_geography))
        
        # Visible project or case study examples
        visible_project_or_case_study_examples = []
        project_patterns = [
            r"(?:case study|project|مشروع|أعمالنا|من أعمالنا)[:\-\s]*([A-Z\u0600-\u06FF\d\s]{3,30})",
        ]
        for pat in project_patterns:
            for match in re.finditer(pat, page_text, re.IGNORECASE):
                visible_project_or_case_study_examples.append(match.group(0).strip())
        visible_project_or_case_study_examples = list(dict.fromkeys(visible_project_or_case_study_examples))
        
        # Visible pricing or packages
        visible_pricing_or_packages = []
        pricing_patterns = [
            r"\b\d+\s*(?:sar|usd|aed|ريال|ر\.س|\$)\b",
            r"(?:starting at|start from|باقات|أسعار|تبدأ من)\s*\d+"
        ]
        for pat in pricing_patterns:
            for match in re.finditer(pat, page_text, re.IGNORECASE):
                visible_pricing_or_packages.append(match.group(0).strip())
        visible_pricing_or_packages = list(dict.fromkeys(visible_pricing_or_packages))
        
        # Visible support or contact methods
        visible_support_or_contact_methods = []
        email_pattern = r"[\w\.-]+@[\w\.-]+\.\w+"
        phone_pattern = r"\+?\d{3,4}[\s\-]?\d{3,4}[\s\-]?\d{3,4}"
        for match in re.finditer(email_pattern, page_text):
            visible_support_or_contact_methods.append(match.group(0).strip())
        for match in re.finditer(phone_pattern, page_text):
            visible_support_or_contact_methods.append(match.group(0).strip())
        if any(k in page_text.lower() for k in ["whatsapp", "واتساب"]):
            visible_support_or_contact_methods.append("WhatsApp")
        visible_support_or_contact_methods = list(dict.fromkeys(visible_support_or_contact_methods))
        
        # Usable snippets (1-4 short page-backed excerpts, max 240 chars each)
        usable_snippets = []
        sentences = re.split(r'(?<=[.!?])\s+|\n', page_text)
        for s in sentences:
            s_clean = s.strip()
            if 30 <= len(s_clean) <= 240:
                if any(k in s_clean.lower() for k in ["%", "+", "provide", "service", "year", "project", "client", "خبرة", "عميل", "مشروع", "خدمات", "ريال", "sar"]):
                    usable_snippets.append(s_clean)
                    if len(usable_snippets) >= 4:
                        break
        if len(usable_snippets) < 1:
            for s in sentences:
                s_clean = s.strip()
                if 20 <= len(s_clean) <= 240:
                    usable_snippets.append(s_clean)
                    if len(usable_snippets) >= 2:
                        break
        usable_snippets = list(dict.fromkeys(usable_snippets))[:4]
        
        # Excluded reason for generic blog/informational page without direct proof
        excluded_reason = None
        if page_type == "blog":
            has_direct_brand_proof = (
                len(visible_products_or_services) > 0 or
                len(visible_conversion_actions) > 0 or
                len(visible_pricing_or_packages) > 0 or
                len(visible_trust_signals) > 0 or
                "author" in page_text.lower() or "كاتب" in page_text
            )
            if not has_direct_brand_proof:
                excluded_reason = "Irrelevant informational blog page without direct brand proof"
                
        cards.append({
            "url": url,
            "title": title,
            "page_type": page_type,
            "headings": headings,
            "cta_labels": cta_labels,
            "visible_products_or_services": visible_products_or_services,
            "visible_features_or_capabilities": visible_features_or_capabilities,
            "visible_process_steps": visible_process_steps,
            "visible_conversion_actions": visible_conversion_actions,
            "visible_trust_signals": visible_trust_signals,
            "visible_geography": visible_geography,
            "visible_project_or_case_study_examples": visible_project_or_case_study_examples,
            "visible_pricing_or_packages": visible_pricing_or_packages,
            "visible_support_or_contact_methods": visible_support_or_contact_methods,
            "usable_snippets": usable_snippets,
            "excluded_reason": excluded_reason
        })
        
    return cards


def _legacy_build_brand_pages_index_unpatched(state: dict) -> dict:
    """
    Pure deterministic function. Constructs a compact pages index from evidence cards.
    Includes every card, displaying excluded_reason when present.
    """
    cards = build_brand_evidence_cards(state)
    index = {}
    
    for card in cards:
        url = card["url"]
        
        if card["excluded_reason"]:
            compact_text = (
                f"Page Title: {card['title'] or 'N/A'}\n"
                f"Page Type: {card['page_type']} (EXCLUDED)\n"
                f"Reason: {card['excluded_reason']}"
            )
        else:
            parts = []
            parts.append(f"Page Title: {card['title'] or 'N/A'}")
            parts.append(f"Page Type: {card['page_type']}")
            
            if card["visible_products_or_services"]:
                parts.append("Services/Products: " + ", ".join(card["visible_products_or_services"]))
            if card["visible_features_or_capabilities"]:
                parts.append("Capabilities: " + ", ".join(card["visible_features_or_capabilities"]))
            if card["visible_process_steps"]:
                parts.append("Process: " + ", ".join(card["visible_process_steps"]))
            if card["visible_trust_signals"]:
                parts.append("Trust Signals: " + ", ".join(card["visible_trust_signals"]))
            if card["visible_geography"]:
                parts.append("Geography: " + ", ".join(card["visible_geography"]))
            if card["visible_pricing_or_packages"]:
                parts.append("Pricing: " + ", ".join(card["visible_pricing_or_packages"]))
            if card["visible_support_or_contact_methods"]:
                parts.append("Contact: " + ", ".join(card["visible_support_or_contact_methods"]))
            if card["usable_snippets"]:
                snippets_list = "\n- ".join(card["usable_snippets"])
                parts.append(f"Usable Excerpts:\n- {snippets_list}")
                
            compact_text = "\n".join(parts)
            
        index[url] = compact_text
        
    return index


def build_brand_evidence_cards(state: dict) -> list[dict]:
    """
    Corrected Phase 1.7 card builder.

    Pure deterministic function: no network, no logging, no input mutation.
    Extracts only visible, page-backed facts from internal_resources.
    """
    resources = state.get("internal_resources", []) or []
    cards: List[dict] = []

    def clean(value: Any) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    def clean_list(value: Any) -> List[str]:
        if isinstance(value, list):
            raw_items = value
        elif isinstance(value, tuple):
            raw_items = list(value)
        elif value:
            raw_items = [value]
        else:
            raw_items = []
        return [clean(item) for item in raw_items if clean(item)]

    def dedupe(values: List[str]) -> List[str]:
        seen = set()
        out = []
        for value in values:
            item = clean(value)
            if not item:
                continue
            key = item.casefold()
            if key not in seen:
                seen.add(key)
                out.append(item)
        return out

    def has_arabic(text: str) -> bool:
        return bool(re.search(r"[\u0600-\u06FF]", text or ""))

    def contains_term(text: str, term: str) -> bool:
        if not text or not term:
            return False
        if has_arabic(term):
            return term in text
        return bool(re.search(rf"\b{re.escape(term)}\b", text, re.IGNORECASE))

    def contains_any(text: str, terms: List[str]) -> bool:
        return any(contains_term(text, term) for term in terms)

    def split_visible_phrase(text: str) -> List[str]:
        """Split only on safe delimiters; never split Arabic words on the letter waw."""
        if not text:
            return []
        normalized = re.sub(r"\s+", " ", text).strip(" .:-")
        parts = re.split(r"\s*(?:,|،|;|؛|\||/|•|·|\n|\r|\band\b)\s*", normalized, flags=re.IGNORECASE)
        safe_parts: List[str] = []
        for part in parts:
            for arabic_part in re.split(r"\s+و\s+", part):
                item = arabic_part.strip(" .:-")
                if item:
                    safe_parts.append(item)
        return safe_parts

    generic_headings = {
        "home", "homepage", "our services", "services", "service", "products", "product",
        "reviews", "portfolio", "our work", "projects", "case studies", "case study",
        "about", "about us", "who we are", "contact", "contact us", "pricing", "packages",
        "why choose us", "why us", "how it works", "how it works!?", "how it works?",
        "process", "our process", "steps", "read more", "share post",
        "خدمات", "خدماتنا", "منتجات", "منتجاتنا", "المتجر", "أعمالنا", "اعمالنا",
        "مشاريعنا", "معرض الأعمال", "من نحن", "عن الشركة", "تواصل معنا", "اتصل بنا",
        "الأسعار", "اسعار", "باقات", "المدونة", "مقالات", "أخبار", "اخبار",
        "خطوات العمل", "طريقة العمل", "كيف نعمل", "لماذا تختارنا", "الرئيسية",
    }
    generic_fragments = [
        "why choose", "contact us", "read more", "share post",
        "لماذا تختار", "تواصل معنا", "اقرأ المزيد",
    ]

    def is_generic_heading(text: str) -> bool:
        item = clean(text).casefold()
        return not item or item in {h.casefold() for h in generic_headings} or any(g in item for g in generic_fragments)

    def looks_like_offer(text: str) -> bool:
        item = clean(text)
        return bool(3 <= len(item) <= 90 and not is_generic_heading(item) and _sanitize_evidence_item(item, "service"))

    def looks_like_service_heading(text: str, page_type_value: str) -> bool:
        item = clean(text)
        if is_generic_heading(item):
            return False
        if page_type_value == "product":
            return bool(_sanitize_evidence_item(item, "service"))
        return bool(_SERVICE_HINT_RE.search(item) and _sanitize_evidence_item(item, "service"))

    def normalize_offer_candidate(text: str) -> str:
        item = clean(text)
        for marker in [" للشركات", " للعملاء", " للمؤسسات", " في ", " for "]:
            if marker in item:
                item = item.split(marker, 1)[0].strip()
        return item

    explicit_offer_patterns = [
        r"\b(?:we offer|we provide|specializes? in|services include|products include)\s+([^.\n]+)",
        r"(?:نقدم خدمات|نقدم خدمة|نقدم|نوفر|متخصصون في|تشمل خدماتنا)\s+([^.\n]+)",
    ]
    capability_patterns = [
        r"\b(?:responsive design|responsive layout|mobile-friendly|fast loading|seo optimized|secure payment|payment integration|companion apps?)\b",
        r"(?:تصميم المواقع|تطوير المواقع|التسويق الرقمي|تحسين محركات البحث|تصميم الجرافيك|تصميم الهوية|تطوير التطبيقات|برمجة التطبيقات|الدفع الآمن)",
    ]
    process_patterns = [
        r"(?:step|phase)\s*\d+[:\-\s]*([^\n.]+)",
        r"(?:consultation\s*&\s*planning|design\s*&\s*development|execution\s*&\s*delivery|launch\s*&\s*training)",
        r"(?:الخطوة|المرحلة)\s*\d+[:\-\s]*([^\n.]+)",
        r"(?:استشارة\s+وتخطيط|تصميم\s+وتطوير|تنفيذ\s+وتسليم|إطلاق\s+وتدريب)",
    ]
    conversion_keywords = [
        "book", "quote", "get a demo", "order", "purchase", "buy", "contact us", "subscribe",
        "request a quote", "add to cart", "اطلب", "احجز", "شراء", "تواصل معنا", "طلب سعر", "عرض سعر", "تسجيل",
    ]
    geo_keywords = [
        "Riyadh", "Jeddah", "Dammam", "Khobar", "Saudi Arabia", "KSA", "Dubai", "Cairo", "Egypt",
        "الرياض", "جدة", "الدمام", "مكة", "المدينة", "الخبر", "السعودية",
        "المملكة العربية السعودية", "الخليج", "دبي", "القاهرة", "مصر",
    ]

    for res in resources:
        url = clean(res.get("link") or res.get("url") or "")
        title = clean(res.get("title")) or None
        headings = clean_list(res.get("headings", []) or [])
        cta_labels = clean_list(res.get("cta_labels", []) or [])
        page_text = clean(res.get("page_text"))
        meta_description = clean(res.get("meta_description"))
        snippet = clean(res.get("snippet"))
        text = clean(res.get("text"))
        anchor = clean(res.get("anchor"))

        corpus_text = "\n".join(
            part for part in [
                title or "",
                "\n".join(headings),
                "\n".join(cta_labels),
                page_text,
                meta_description,
                snippet,
                text,
                anchor,
            ]
            if part
        )
        corpus_lower = corpus_text.casefold()
        url_lower = url.casefold()
        title_lower = (title or "").casefold()
        headings_lower = [h.casefold() for h in headings]

        parsed_url = urlparse(url_lower)
        root_url = f"{parsed_url.scheme}://{parsed_url.netloc}" if parsed_url.scheme and parsed_url.netloc else ""

        page_type = "other"
        if any(k in url_lower for k in ["contact", "get-in-touch", "تواصل", "اتصل"]):
            page_type = "contact"
        elif any(contains_term(title_lower, k) or any(contains_term(h, k) for h in headings_lower) for k in ["contact us", "تواصل معنا", "اتصل بنا"]):
            page_type = "contact"
        elif any(k in url_lower for k in ["services", "service", "%d8%ae%d8%af%d9%85%d8%a7%d8%aa", "خدمات", "خدمة"]):
            page_type = "services"
        elif any(contains_term(title_lower, k) or any(contains_term(h, k) for h in headings_lower) for k in ["services", "خدماتنا", "خدمات"]):
            page_type = "services"
        elif any(k in url_lower for k in ["products", "product", "shop", "store", "منتجات", "منتج", "متجر"]):
            page_type = "product"
        elif any(contains_term(title_lower, k) or any(contains_term(h, k) for h in headings_lower) for k in ["products", "منتجاتنا", "منتجات", "متجر"]):
            page_type = "product"
        elif any(k in url_lower for k in ["portfolio", "projects", "case-study", "case-studies", "work", "اعمالنا", "أعمالنا", "مشاريعنا"]):
            page_type = "portfolio"
        elif any(contains_term(title_lower, k) or any(contains_term(h, k) for h in headings_lower) for k in ["portfolio", "our work", "أعمالنا", "معرض الأعمال", "مشاريعنا"]):
            page_type = "portfolio"
        elif any(k in url_lower for k in ["about", "who-we-are", "من-نحن", "من_نحن", "نبذة"]):
            page_type = "about"
        elif any(contains_term(title_lower, k) or any(contains_term(h, k) for h in headings_lower) for k in ["about us", "who we are", "من نحن", "عن الشركة"]):
            page_type = "about"
        elif any(k in url_lower for k in ["blog", "news", "articles", "مدونة", "أخبار", "اخبار", "مقالات"]):
            page_type = "blog"
        elif any(contains_term(title_lower, k) or any(contains_term(h, k) for h in headings_lower) for k in ["blog", "news", "articles", "مدونة", "المقالات"]):
            page_type = "blog"
        elif any(k in url_lower for k in ["pricing", "packages", "أسعار", "اسعار", "باقات"]):
            page_type = "pricing"
        elif any(contains_term(title_lower, k) or any(contains_term(h, k) for h in headings_lower) for k in ["pricing", "packages", "الأسعار", "الباقات"]):
            page_type = "pricing"
        elif root_url and url_lower.rstrip("/") == root_url:
            page_type = "home"
        elif any(k in url_lower for k in ["/home", "index.html", "index.php"]):
            page_type = "home"
        elif any(contains_term(title_lower, k) for k in ["home", "homepage", "الرئيسية", "الصفحة الرئيسية"]):
            page_type = "home"

        visible_products_or_services = []
        if page_type in ["services", "home", "product"]:
            visible_products_or_services.extend([h for h in headings if looks_like_service_heading(h, page_type)])
        for pattern in explicit_offer_patterns:
            for match in re.finditer(pattern, corpus_text, re.IGNORECASE):
                for phrase in split_visible_phrase(match.group(1)):
                    phrase = normalize_offer_candidate(phrase)
                    cleaned_phrase = _sanitize_evidence_item(phrase, "service")
                    if cleaned_phrase:
                        visible_products_or_services.append(cleaned_phrase)

        visible_features_or_capabilities = []
        for pattern in capability_patterns:
            for match in re.finditer(pattern, corpus_text, re.IGNORECASE):
                visible_features_or_capabilities.append(match.group(0).strip())
        for line in re.split(r"(?<=[.!؟?])\s+|\n", corpus_text):
            item = clean(line)
            if 10 <= len(item) <= 160 and contains_any(item.casefold(), ["features", "capabilities", "مميزات", "خصائص"]):
                visible_features_or_capabilities.append(item)
        for item in list(visible_features_or_capabilities):
            if page_type in ["services", "home", "product"] and looks_like_service_heading(item, page_type):
                visible_products_or_services.append(item)

        visible_process_steps = []
        for pattern in process_patterns:
            for match in re.finditer(pattern, corpus_text, re.IGNORECASE):
                visible_process_steps.append(match.group(0).strip())

        visible_conversion_actions = []
        for cta in cta_labels:
            if contains_any(cta.casefold(), conversion_keywords):
                cleaned_cta = _sanitize_evidence_item(cta, "cta", allow_promotional=True)
                if cleaned_cta:
                    visible_conversion_actions.append(cleaned_cta)

        visible_trust_signals = []
        trust_patterns = [
            r"\b\d+\+?\s*(?:[\w\-]+\s+){0,3}(?:projects|clients|customers|years|employees)\b",
            r"\b(?:trusted by|reviews|rating|ratings|award-winning|certified|certification|licensed|partner(?:ship)?)\b",
            r"\d+\+?\s*(?:مشاريع|عملاء|عميل|سنوات|سنة|موظفين)",
            r"(?:شريك|شراكة|تقييم|تقييمات|معتمد|مرخص|حائز على جوائز|شهادات العملاء)",
        ]
        for pattern in trust_patterns:
            for match in re.finditer(pattern, corpus_text, re.IGNORECASE):
                cleaned_trust = _sanitize_evidence_item(match.group(0).strip(), "trust")
                if cleaned_trust:
                    visible_trust_signals.append(cleaned_trust)

        visible_geography = []
        geo_source = ""
        if page_type in {"contact", "about"}:
            geo_source = corpus_text
        else:
            geo_lines = []
            for line in re.split(r"(?<=[.!؟?])\s+|\n", corpus_text):
                line_lower = line.casefold()
                explicit_service_geo = any(
                    (geo.isascii() and re.search(
                        rf"\b(?:we\s+)?(?:provide|offer|serve|serving|available|services?)\b[^.\n]{{0,80}}\b(?:in|inside|within|across)\s+{re.escape(geo.casefold())}\b",
                        line_lower,
                    ))
                    or (not geo.isascii() and re.search(
                        rf"(?:نقدم|نوفر|نخدم|خدماتنا)[^.\n]{{0,80}}(?:في|داخل)\s*{re.escape(geo)}",
                        line,
                    ))
                    for geo in geo_keywords
                )
                explicit_real_estate_geo = any(
                    geo.isascii()
                    and re.search(
                        rf"\b(?:browse|search|view|find|rent|buy|sale|listings?|properties|apartments?|villas?)\b[^.\n]{{0,80}}\b(?:in|inside|within|across)\s+{re.escape(geo.casefold())}\b",
                        line_lower,
                    )
                    for geo in geo_keywords
                )
                if _GEOGRAPHY_CONTEXT_RE.search(line) or explicit_service_geo or explicit_real_estate_geo:
                    geo_lines.append(line)
            geo_source = "\n".join(geo_lines)
        for geo in geo_keywords:
            if contains_term(geo_source, geo):
                cleaned_geo = _sanitize_evidence_item(geo, "geography")
                if cleaned_geo:
                    visible_geography.append(cleaned_geo)

        visible_project_or_case_study_examples = []
        for pattern in [r"(?:case study|project|مشروع|دراسة حالة)[:\-\s]*([A-Z\u0600-\u06FF][A-Z\u0600-\u06FF\d\s&-]{2,60})"]:
            for match in re.finditer(pattern, corpus_text, re.IGNORECASE):
                project_value = clean(match.group(1))
                cleaned_project = _sanitize_evidence_item(project_value, "project")
                if cleaned_project:
                    visible_project_or_case_study_examples.append(cleaned_project)
        if page_type == "portfolio":
            visible_project_or_case_study_examples.extend([
                cleaned for cleaned in (_sanitize_evidence_item(h, "project_explicit") for h in headings)
                if cleaned
            ])

        visible_pricing_or_packages = []
        pricing_patterns = [
            r"\b\d+(?:[,.]\d+)?\s*(?:sar|usd|aed|egp|\$)\b",
            r"\b(?:sar|usd|aed|egp|\$)\s*\d+(?:[,.]\d+)?\b",
            r"\d+(?:[,.]\d+)?\s*(?:ريال|ر\.س|درهم|جنيه)",
            r"(?:starting at|start from|starts from|باقات|أسعار|تبدأ من)\s*\d+",
        ]
        for pattern in pricing_patterns:
            for match in re.finditer(pattern, corpus_text, re.IGNORECASE):
                cleaned_price = _sanitize_evidence_item(match.group(0).strip(), "pricing")
                if cleaned_price:
                    visible_pricing_or_packages.append(cleaned_price)

        visible_support_or_contact_methods = []
        for match in re.finditer(r"[\w\.-]+@[\w\.-]+\.\w+", corpus_text):
            visible_support_or_contact_methods.append(match.group(0).strip())
        for match in re.finditer(r"\+?\d{3,4}[\s\-]?\d{3,4}[\s\-]?\d{3,4}", corpus_text):
            visible_support_or_contact_methods.append(match.group(0).strip())
        if contains_any(corpus_lower, ["whatsapp", "واتساب", "واتس اب"]):
            visible_support_or_contact_methods.append("WhatsApp")

        sentence_source = page_text or snippet or meta_description or text or corpus_text
        sentences = re.split(r"(?<=[.!?؟])\s+|\n", sentence_source)
        usable_snippets = []
        for sentence in sentences:
            item = clean(sentence)
            if 30 <= len(item) <= 240 and contains_any(item.casefold(), ["%", "+", "provide", "service", "year", "project", "client", "خدمات", "عميل", "مشروع", "خبرة", "ريال", "sar", "نقدم"]):
                usable_snippets.append(item)
                if len(usable_snippets) >= 4:
                    break
        if not usable_snippets:
            for sentence in sentences:
                item = clean(sentence)
                if 20 <= len(item) <= 240:
                    usable_snippets.append(item)
                    if len(usable_snippets) >= 2:
                        break

        visible_products_or_services = _clean_evidence_items(visible_products_or_services, category="service", limit=16)
        visible_features_or_capabilities = _clean_evidence_items(visible_features_or_capabilities, category="capability", limit=16)
        visible_process_steps = _clean_evidence_items(visible_process_steps, category="process", limit=12)
        visible_conversion_actions = _clean_evidence_items(visible_conversion_actions, category="cta", limit=12, allow_promotional=True)
        visible_trust_signals = _clean_evidence_items(visible_trust_signals, category="trust", limit=12)
        visible_geography = _clean_evidence_items(visible_geography, category="geography", limit=8)
        visible_project_or_case_study_examples = _clean_evidence_items(visible_project_or_case_study_examples, category="project_explicit", limit=16)
        visible_pricing_or_packages = _clean_evidence_items(visible_pricing_or_packages, category="pricing", limit=12)
        visible_support_or_contact_methods = dedupe(visible_support_or_contact_methods)
        usable_snippets = _clean_evidence_items(dedupe(usable_snippets), category="snippet", limit=4, allow_promotional=True)

        excluded_reason = None
        if page_type == "blog":
            has_direct_brand_proof = (
                visible_products_or_services or
                visible_conversion_actions or
                visible_pricing_or_packages or
                visible_trust_signals or
                "author" in corpus_lower or
                "كاتب" in corpus_text
            )
            if not has_direct_brand_proof:
                excluded_reason = "Irrelevant informational blog page without direct brand proof"

        cards.append({
            "url": url,
            "title": title,
            "page_type": page_type,
            "headings": headings,
            "cta_labels": cta_labels,
            "visible_products_or_services": visible_products_or_services,
            "visible_features_or_capabilities": visible_features_or_capabilities,
            "visible_process_steps": visible_process_steps,
            "visible_conversion_actions": visible_conversion_actions,
            "visible_trust_signals": visible_trust_signals,
            "visible_geography": visible_geography,
            "visible_project_or_case_study_examples": visible_project_or_case_study_examples,
            "visible_pricing_or_packages": visible_pricing_or_packages,
            "visible_support_or_contact_methods": visible_support_or_contact_methods,
            "usable_snippets": usable_snippets,
            "excluded_reason": excluded_reason,
        })

    return cards


def build_brand_pages_index(state: dict) -> dict:
    """
    Pure deterministic function. Constructs a compact pages index from evidence cards.
    Includes every card and every populated evidence bucket.
    """
    cards = build_brand_evidence_cards(state)
    index: Dict[str, str] = {}

    for card in cards:
        url = card["url"]
        page_type = f"{card['page_type']} (EXCLUDED)" if card["excluded_reason"] else card["page_type"]
        parts = [
            f"Page Title: {card['title'] or 'N/A'}",
            f"Page Type: {page_type}",
        ]
        fields = [
            ("headings", "Headings"),
            ("cta_labels", "CTA Labels"),
            ("visible_products_or_services", "Services/Products"),
            ("visible_features_or_capabilities", "Capabilities"),
            ("visible_process_steps", "Process"),
            ("visible_conversion_actions", "Conversion Actions"),
            ("visible_trust_signals", "Trust Signals"),
            ("visible_geography", "Geography"),
            ("visible_project_or_case_study_examples", "Projects/Case Studies"),
            ("visible_pricing_or_packages", "Pricing/Packages"),
            ("visible_support_or_contact_methods", "Support/Contact"),
        ]
        for key, label in fields:
            values = card.get(key) or []
            if values:
                parts.append(f"{label}: " + ", ".join(values))
        if card.get("usable_snippets"):
            parts.append("Usable Excerpts:\n- " + "\n- ".join(card["usable_snippets"]))
        if card["excluded_reason"]:
            parts.append(f"Excluded Reason: {card['excluded_reason']}")
        index[url] = "\n".join(parts)

    return index


def _section_visibly_references_brand(section: dict, state: dict) -> bool:
    """True only when visible section text names the brand or an alias."""
    brand_name = (state.get("brand_name") or "").lower().strip()
    brand_aliases = state.get("brand_aliases")
    if not isinstance(brand_aliases, list):
        brand_aliases = []
    refs = [brand_name] + [str(alias).lower().strip() for alias in brand_aliases if str(alias).strip()]
    refs = [ref for ref in refs if ref]
    if not refs:
        return False

    visible_text = " ".join([
        str(section.get("heading_text") or ""),
        " ".join(str(item) for item in section.get("subheadings", []) or []),
    ]).lower()
    return any(ref in visible_text for ref in refs)


def _section_should_receive_brand_evidence(section: dict, state: dict) -> bool:
    """Return True when a commercial section should receive brand evidence."""
    section = section or {}
    state = state or {}
    if _section_visibly_references_brand(section, state):
        return True

    content_type = str(state.get("content_type") or section.get("content_type") or "").casefold()
    if content_type != "brand_commercial":
        return False

    section_type = str(section.get("section_type") or "").casefold()
    heading_level = str(section.get("heading_level") or "").upper()
    if section_type in {"introduction", "intro", "conclusion"} or heading_level == "INTRO":
        return True

    # Keep genuinely informational/support sections neutral unless they name
    # the brand. Commercial offer/process/proof sections need brand context even
    # when the approved heading is phrased generically.
    if section_type in {"faq", "comparison", "pricing", "packages", "location"}:
        return False

    contract = section.get("section_contract") if isinstance(section.get("section_contract"), dict) else {}
    brand_policy = str(contract.get("brand_policy") or section.get("brand_policy") or "").casefold()
    taxonomy_axis = str(section.get("taxonomy_axis") or contract.get("taxonomy_axis") or "").casefold()
    if brand_policy == "commercial" or taxonomy_axis.startswith("brand_"):
        return True

    inventory = state.get("brand_evidence_inventory") or {}
    if section_type in {"offer", "services", "core_or_benefits"} and inventory.get("services_available"):
        return True
    if section_type in {"features", "differentiation", "differentiators", "brand_support", "brand"} and any(
        inventory.get(key) for key in ("services_available", "projects_available", "process_available", "trust_available")
    ):
        return True
    if section_type in {"process", "process_or_how"} and (inventory.get("process_available") or inventory.get("services_available")):
        return True
    if section_type in {"proof", "case_study", "case-study"} and (inventory.get("projects_available") or inventory.get("trust_available")):
        return True

    intent = str(section.get("section_intent") or "").casefold()
    if intent in {"informational", "information", "info"}:
        return False

    brandable_types = {
        "offer", "services", "core_or_benefits", "features", "differentiation",
        "differentiators", "brand_support", "brand", "proof", "case_study",
        "case-study", "process", "process_or_how",
    }
    if section_type in brandable_types:
        return True

    return "commercial" in intent and section_type not in {"faq", "comparison"}


def select_section_brand_sources(section: dict, state: dict) -> tuple[str, int]:
    """
    Selects 1-2 brand evidence cards for a given section without mutating state.
    Returns (section_source_text, selected_sources_count).
    """
    brand_name = (state.get("brand_name") or "").lower().strip()
    brand_aliases = state.get("brand_aliases")
    if not isinstance(brand_aliases, list) or not all(isinstance(a, str) and a.strip() for a in brand_aliases):
        brand_aliases = []
    brand_aliases = [a.lower().strip() for a in brand_aliases if a.strip()]

    # Select sources only for visibly brand-owned headings. Generic commercial
    # H2s may still be useful in a brand article, but they should stay market
    # guidance unless the heading itself names the brand.
    heading = (section.get("heading_text") or "").lower()
    purpose = (section.get("content_goal") or "").lower()
    section_type = (section.get("section_type") or "").lower()
    
    brand_refs = [brand_name] + brand_aliases if brand_name else brand_aliases
    if not brand_refs:
        return "", 0
        
    if not _section_should_receive_brand_evidence(section, state):
        return "", 0

    # Constraint: fallback to build_brand_evidence_cards if missing (pure call)
    cards = state.get("brand_evidence_cards")
    if cards is None:
        cards = build_brand_evidence_cards(state)
        
    if not cards:
        return "", 0

    # Constraint: ignore noisy/generic tokens
    noisy_tokens = {
        "best", "company", "service", "services", "guide", "how", "what", "why", "for", "with",
        "افضل", "أفضل", "شركة", "شركه", "خدمة", "خدمات", "اختيار", "تختار", "كيف", "ما",
        "في", "من", "على", "الى", "إلى", "السعودية", "السعودي", "المتاحة", "المتوفره"
    }

    # Extract tokens from section heading and purpose
    import re
    raw_tokens = re.findall(r"\w+", heading + " " + purpose)
    section_tokens = {t for t in raw_tokens if t.lower() not in noisy_tokens and len(t) > 2}

    project_terms = {"مشاريع", "نماذج", "أعمال", "سابقة", "portfolio", "projects", "case", "examples"}
    service_terms = {"خدمات", "الخدمات", "حلول", "تصميم", "تطوير", "برمجة", "services", "solutions", "offer"}
    feature_terms = {"مميزات", "تقنية", "تقنيات", "خيارات", "features", "capabilities", "technology"}
    process_terms = {"خطوات", "مراحل", "تنفيذ", "طلب", "process", "steps", "workflow", "delivery"}
    heading_tokens = set(section_tokens)
    wants_projects = section_type in {"proof", "case_study"} or bool(heading_tokens & project_terms)
    wants_services = section_type in {"offer", "core_or_benefits"} or bool(heading_tokens & service_terms)
    wants_features = section_type in {"features", "differentiation"} or bool(heading_tokens & feature_terms)
    wants_process = section_type == "process" or bool(heading_tokens & process_terms)

    scored_cards = []
    for card in cards:
        if card.get("excluded_reason"):
            continue
            
        score = 0
        fields_to_check = [
            card.get("page_type", ""),
            card.get("title", ""),
            " ".join(card.get("headings", [])),
            " ".join(card.get("visible_products_or_services", [])),
            " ".join(card.get("visible_features_or_capabilities", [])),
            " ".join(card.get("visible_project_or_case_study_examples", []))
        ]
        
        card_text = " ".join(fields_to_check).lower()
        
        for token in section_tokens:
            if token.lower() in card_text:
                score += 1

        page_type = str(card.get("page_type") or "").lower()
        if wants_projects:
            if page_type in {"portfolio", "projects"}:
                score += 8
            if card.get("visible_project_or_case_study_examples"):
                score += 6
        if wants_services:
            if page_type in {"services", "home"}:
                score += 5
            if card.get("visible_products_or_services"):
                score += 5
        if wants_features:
            if card.get("visible_features_or_capabilities"):
                score += 5
            if card.get("visible_products_or_services"):
                score += 2
        if wants_process and card.get("visible_process_steps"):
            score += 7
                
        if score > 0:
            scored_cards.append((score, card))
            
    if not scored_cards:
        fallback_cards = []
        for card in cards:
            if not isinstance(card, dict) or card.get("excluded_reason"):
                continue
            page_type = str(card.get("page_type") or "").lower()
            if wants_projects and (page_type in {"portfolio", "projects"} or card.get("visible_project_or_case_study_examples")):
                fallback_cards.append(card)
            elif wants_services and (page_type in {"services", "home"} or card.get("visible_products_or_services")):
                fallback_cards.append(card)
            elif wants_features and (card.get("visible_features_or_capabilities") or card.get("visible_products_or_services")):
                fallback_cards.append(card)
            elif wants_process and card.get("visible_process_steps"):
                fallback_cards.append(card)
        if not fallback_cards:
            return "", 0
        top_cards = fallback_cards[:2]
    else:
        scored_cards.sort(key=lambda x: x[0], reverse=True)
        top_score = scored_cards[0][0]
        top_cards = [card for score, card in scored_cards if score >= top_score - 1][:2]
    
    # Format section_source_text
    output_lines = []
    for card in top_cards:
        output_lines.append("[SECTION-SPECIFIC BRAND EVIDENCE]")
        output_lines.append(f"Source URL: {card.get('url', 'N/A')}")
        output_lines.append(f"Page type: {card.get('page_type', 'N/A')}")
        
        facts = []
        fact_fields = [
            ("visible_products_or_services", "Products/Services"),
            ("visible_features_or_capabilities", "Capabilities/Features"),
            ("visible_process_steps", "Process Steps"),
            ("visible_conversion_actions", "Conversion Actions"),
            ("visible_trust_signals", "Trust Signals"),
            ("visible_geography", "Geography"),
            ("visible_project_or_case_study_examples", "Projects/Case Studies"),
            ("visible_pricing_or_packages", "Pricing/Packages"),
            ("visible_support_or_contact_methods", "Support/Contact Methods")
        ]
        for key, label in fact_fields:
            values = card.get(key)
            if values:
                facts.append(f"{label}: " + ", ".join(values))
                
        output_lines.append("Observed facts:")
        if facts:
            for fact in facts:
                output_lines.append(f"- {fact}")
        else:
            output_lines.append("- None")
            
        output_lines.append("Usable snippets:")
        snippets = card.get("usable_snippets", [])
        if snippets:
            for snippet in snippets:
                output_lines.append(f"- {snippet}")
        else:
            output_lines.append("- None")
            
        output_lines.append("Constraints:")
        output_lines.append("- Do not add facts not listed above.")
        output_lines.append("- If evidence is weak, keep the brand mention contextual.")
        output_lines.append("") # blank line between cards

    return "\n".join(output_lines).strip(), len(top_cards)


def build_compact_brand_evidence_summary(state: dict) -> str:
    """
    Builds a compact brand evidence summary for outline generation.
    - Max 8 bullet points.
    - Built only from brand_evidence_cards and brand_offer_contract.
    - No raw page dumps.
    - Clearly marked as evidence boundary, not brand description.
    """
    cards = state.get("brand_evidence_cards")
    if cards is None:
        try:
            cards = build_brand_evidence_cards(state)
        except Exception:
            cards = []
            
    contract = state.get("brand_offer_contract")
    if contract is None:
        try:
            contract = build_brand_offer_contract(state)
        except Exception:
            contract = {}
    
    bullets = []
    
    # Bullet 1: Discovered Brand Confidence and Category
    if contract:
        identity = contract.get("brand_identity", {})
        conf = identity.get("confidence") or identity.get("brand_confidence") or "low"
        cat = identity.get("category", "unknown")
        bullets.append(f"Confidence Level: {conf} | Business Category: {cat}")
        
    # Bullet 2: Strong Signals (if any)
    if contract:
        strong = contract.get("evidence_summary", {}).get("strong_evidence", [])
        if strong:
            bullets.append(f"Strong evidence signals: {', '.join(strong[:4])}")
            
    # Bullet 3: Weak or Inferred Evidence (if any)
    if contract:
        weak = contract.get("evidence_summary", {}).get("weak_or_inferred_evidence", [])
        if weak:
            bullets.append(f"Weak/inferred evidence: {', '.join(weak[:4])}")

    # Bullet 4: Discovered Page Types / Cards count
    if cards:
        non_excluded = [c for c in cards if not c.get("excluded_reason")]
        page_types = sorted(list({c.get("page_type") for c in non_excluded if c.get("page_type")}))
        bullets.append(f"Discovered {len(non_excluded)} page(s) matching types: {', '.join(page_types)}")
        
    # Bullet 5: Observed Products / Services
    products = []
    for card in cards:
        if not card.get("excluded_reason") and card.get("visible_products_or_services"):
            products.extend(card.get("visible_products_or_services"))
    if products:
        products = _clean_evidence_items(products, limit=8)
        bullets.append(f"Observed products/services: {', '.join(products[:6])}")
        
    # Bullet 6: Observed Capabilities / Features
    features = []
    for card in cards:
        if not card.get("excluded_reason") and card.get("visible_features_or_capabilities"):
            features.extend(card.get("visible_features_or_capabilities"))
    if features:
        features = _clean_evidence_items(features, limit=8)
        bullets.append(f"Observed capabilities/features: {', '.join(features[:6])}")

    projects = _collect_card_values(cards, ["visible_project_or_case_study_examples"], limit=8, category="project_explicit")
    if projects:
        bullets.append(f"Observed projects/case studies: {', '.join(projects[:6])}")

    process_steps = _collect_card_values(cards, ["visible_process_steps"], limit=6)
    if process_steps:
        bullets.append(f"Observed workflow/process: {', '.join(process_steps[:5])}")

    # Bullet 7: Allowed Conversion Actions (CTAs)
    if contract:
        ctas = contract.get("conversion_actions", [])
        if ctas:
            bullets.append(f"Verified conversion actions (CTAs): {', '.join(ctas)}")

    # Bullet 8: Brand Limitations/Missing Evidence
    if contract:
        limitations = contract.get("brand_limitations", [])
        if limitations:
            bullets.append(f"Brand limitations: {', '.join(limitations[:3])}")
            
    # Max 8 bullet points constraint
    selected_bullets = bullets[:8]
    
    summary_lines = [
        "",
        "[EVIDENCE BOUNDARY - COMPACT BRAND EVIDENCE SUMMARY - DO NOT TREAT AS BRAND DESCRIPTION]",
        "The following factual evidence was observed and verified from crawling the brand website:"
    ]
    for b in selected_bullets:
        summary_lines.append(f"- {b}")
    summary_lines.append("[END OF EVIDENCE BOUNDARY]")
    summary_lines.append("")
    
    return "\n".join(summary_lines)


def build_brand_heading_guardrails(state: dict) -> dict:
    """
    Build deterministic brand heading guardrails from observable brand evidence.

    Generic promotional headings remain forbidden. High-confidence evidence can
    allow differentiation framing, but only as evidence-grounded headings rather
    than generic "why choose" or "leading choice" claims.
    """
    contract = state.get("brand_offer_contract") or {}
    cards = state.get("brand_evidence_cards") or []
    identity = contract.get("brand_identity", {}) or {}
    brand_name = state.get("brand_name") or identity.get("brand_name") or "Brand"
    keyword = state.get("primary_keyword") or "Topic"
    confidence = identity.get("confidence", "low")

    geo_focus = identity.get("geographic_focus") or []
    has_geography = bool(geo_focus)
    has_card_geography = any(
        not card.get("excluded_reason") and bool(card.get("visible_geography"))
        for card in cards
    )
    has_geography = has_geography or has_card_geography

    trust_signals = contract.get("trust_signals") or []
    value_props = contract.get("value_propositions") or []
    has_card_trust = any(
        not card.get("excluded_reason") and bool(card.get("visible_trust_signals"))
        for card in cards
    )
    has_explicit_trust = bool(trust_signals) or has_card_trust
    has_differentiation_evidence = bool(value_props) or has_explicit_trust

    promotional_allowed = False
    proof_sections_allowed = False
    differentiation_allowed = False

    if confidence == "low":
        policy = "low_confidence_soft_context_only"
    elif confidence == "medium":
        policy = "medium_confidence_operational_only"
    elif confidence == "high" and has_differentiation_evidence:
        policy = "high_confidence_evidence_grounded_differentiation_allowed"
        proof_sections_allowed = True
        differentiation_allowed = True
    elif confidence == "high":
        policy = "high_confidence_operational_only"
    else:
        policy = "low_confidence_soft_context_only"

    forbidden = [
        f"Why choose {brand_name}?",
        f"Why customers trust {brand_name}?",
        f"Best {brand_name} for {keyword}",
        f"{brand_name}: the leading choice",
        f"\u0644\u0645\u0627\u0630\u0627 \u062a\u062e\u062a\u0627\u0631 {brand_name}\u061f",
        f"\u0644\u0645\u0627\u0630\u0627 \u064a\u062b\u0642 \u0627\u0644\u0639\u0645\u0644\u0627\u0621 \u0641\u064a {brand_name}\u061f",
        f"\u0623\u0641\u0636\u0644 {brand_name} \u0644\u0640 {keyword}",
        f"{brand_name} \u0627\u0644\u062e\u064a\u0627\u0631 \u0627\u0644\u0623\u0641\u0636\u0644",
    ]

    rules = [
        "Headings must match observed brand evidence.",
        "Generic promotional brand headings are blocked; use evidence-grounded operational headings instead.",
    ]
    if not has_geography:
        rules.append("Do not claim geography or specific location focus in brand headings.")
        forbidden.append(f"{brand_name} in Riyadh/Cairo/Location")
    if not has_explicit_trust:
        rules.append("Do not claim high trust, reviews, ratings, or customer choice in brand headings.")
        forbidden.append("Highly trusted " + brand_name)
    if not differentiation_allowed:
        rules.append("Do not create differentiation or brand-proof headings unless explicit trust signals or value propositions exist.")

    preferred = [
        f"Services offered by {brand_name} for {keyword}",
        f"What {brand_name} provides for {keyword}",
        f"How {brand_name} supports {keyword}",
        f"Features available from {brand_name}",
        f"Case studies or projects shown by {brand_name}",
        f"Contact options available from {brand_name}",
        f"\u062e\u062f\u0645\u0627\u062a {brand_name} \u0641\u064a {keyword}",
        f"\u0645\u0627 \u0627\u0644\u0630\u064a \u062a\u0648\u0641\u0631\u0647 {brand_name} \u0644\u0640 {keyword}",
        f"\u0643\u064a\u0641 \u062a\u062f\u0639\u0645 {brand_name} {keyword}",
        f"\u0623\u0645\u062b\u0644\u0629 \u0627\u0644\u0645\u0634\u0627\u0631\u064a\u0639 \u0627\u0644\u062a\u064a \u062a\u0639\u0631\u0636\u0647\u0627 {brand_name} \u0644\u0640 {keyword}",
    ]
    if differentiation_allowed:
        preferred.extend([
            f"Evidence-backed advantages available from {brand_name}",
            f"Observed trust signals for {brand_name}",
            f"\u0645\u064a\u0632\u0627\u062a {brand_name} \u0627\u0644\u0645\u062f\u0639\u0648\u0645\u0629 \u0628\u0623\u062f\u0644\u0629",
            f"\u0625\u0634\u0627\u0631\u0627\u062a \u0627\u0644\u062b\u0642\u0629 \u0627\u0644\u0645\u0631\u0635\u0648\u062f\u0629 \u0644\u062f\u0649 {brand_name}",
        ])

    return {
        "brand_heading_policy": policy,
        "dedicated_brand_proof_sections_allowed": proof_sections_allowed,
        "promotional_headings_allowed": promotional_allowed,
        "differentiation_headings_allowed": differentiation_allowed,
        "has_explicit_geography": has_geography,
        "has_explicit_trust": has_explicit_trust,
        "forbidden_generic_brand_headings": list(dict.fromkeys(forbidden)),
        "preferred_evidence_grounded_heading_patterns": list(dict.fromkeys(preferred)),
        "heading_rules": rules,
    }


def format_brand_heading_guardrails_context(guardrails: dict) -> str:
    """
    Formats the heading guardrails into a compact string context for outline generator.
    """
    if not guardrails:
        return ""
        
    policy = guardrails.get("brand_heading_policy", "low_confidence_soft_context_only")
    proof_allowed = "YES" if guardrails.get("dedicated_brand_proof_sections_allowed") else "NO"
    promo_allowed = "YES" if guardrails.get("promotional_headings_allowed") else "NO"
    differentiation_allowed = "YES" if guardrails.get("differentiation_headings_allowed") else "NO"
    
    forbidden_list = guardrails.get("forbidden_generic_brand_headings", [])
    forbidden_str = "; ".join(forbidden_list) if forbidden_list else "None"
    
    preferred_list = guardrails.get("preferred_evidence_grounded_heading_patterns", [])
    preferred_str = "; ".join(preferred_list) if preferred_list else "None"
    
    rules_list = guardrails.get("heading_rules", [])
    rules_str = " ".join(rules_list) if rules_list else "None"
    
    return f"""
[BRAND HEADING GUARDRAILS - CRITICAL OUTLINE BOUNDARY]
- Brand Heading Policy: {policy}
- Dedicated Brand Proof Sections Allowed: {proof_allowed}
- Promotional Headings Allowed: {promo_allowed}
- Evidence-Grounded Differentiation Headings Allowed: {differentiation_allowed}
- Heading Rules: {rules_str}
- Forbidden Generic Brand Headings: {forbidden_str}
- Preferred Evidence-Grounded Heading Patterns: {preferred_str}
[END OF BRAND HEADING GUARDRAILS]
"""

def classify_page_type(url: str, title: str = "", headings: List[str] = None) -> str:
    url_lower = url.lower()
    title_lower = (title or "").lower()
    headings_lower = [str(h or "").lower() for h in (headings or [])]
    parsed_path_first = urlparse(url_lower)
    root_path_first = (
        f"{parsed_path_first.scheme}://{parsed_path_first.netloc}"
        if parsed_path_first.scheme and parsed_path_first.netloc
        else ""
    )
    if root_path_first and url_lower.rstrip("/") == root_path_first:
        return "home"

    path = (parsed_path_first.path or "").strip("/").casefold()
    path_text = re.sub(r"[-_/]+", " ", path)

    def path_has(*needles: str) -> bool:
        return any(needle in path or needle in path_text for needle in needles)

    # URL/path is the strongest signal. Site navigation headings often contain
    # every section link, so they must not reclassify an about page as portfolio.
    if path_has("contact", "get in touch", "get-in-touch"):
        return "contact"
    if path_has("about", "who we are", "who-we-are"):
        return "about"
    if path_has("pricing", "packages", "plans"):
        return "pricing"
    if path_has("services", "service"):
        return "services"
    if path_has("products", "product", "shop", "store"):
        return "product"
    if path_has("portfolio", "projects", "project", "case study", "case-study", "case-studies", "portfoliotype"):
        return "portfolio"
    if path_has("blog", "news", "articles", "article"):
        return "blog"
    
    if any(k in url_lower for k in ["contact", "get-in-touch", "تواصل", "اتصل"]):
        return "contact"
    if any(k in title_lower or any(k in h for h in headings_lower) for k in ["contact us", "تواصل معنا", "اتصل بنا"]):
        return "contact"
    if any(k in url_lower for k in ["services", "service", "%d8%ae%d8%af%d9%85%d8%a7%d8%aa", "خدمات", "خدمة"]):
        return "services"
    if any(k in title_lower or any(k in h for h in headings_lower) for k in ["services", "خدماتنا", "خدمات"]):
        return "services"
    if any(k in url_lower for k in ["products", "product", "shop", "store", "منتجات", "منتج", "متجر"]):
        return "product"
    if any(k in title_lower or any(k in h for h in headings_lower) for k in ["products", "منتجاتنا", "منتجات", "متجر"]):
        return "product"
    if any(k in url_lower for k in ["portfolio", "projects", "case-study", "case-studies", "work", "اعمالنا", "أعمالنا", "مشاريعنا"]):
        return "portfolio"
    if any(k in title_lower or any(k in h for h in headings_lower) for k in ["portfolio", "our work", "أعمالنا", "معرض الأعمال", "مشاريعنا"]):
        return "portfolio"
    if any(k in url_lower for k in ["about", "who-we-are", "من-نحن", "من_نحن", "نبذة"]):
        return "about"
    if any(k in title_lower or any(k in h for h in headings_lower) for k in ["about us", "who we are", "من نحن", "عن الشركة"]):
        return "about"
    if any(k in url_lower for k in ["blog", "news", "articles", "مدونة", "أخبار", "اخبار", "مقالات"]):
        return "blog"
    if any(k in title_lower or any(k in h for h in headings_lower) for k in ["blog", "news", "articles", "مدونة", "المقالات"]):
        return "blog"
    if any(k in url_lower for k in ["pricing", "packages", "أسعار", "اسعار", "باقات"]):
        return "pricing"
    if any(k in title_lower or any(k in h for h in headings_lower) for k in ["pricing", "packages", "الأسعار", "الباقات"]):
        return "pricing"
    
    parsed = urlparse(url_lower)
    root = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""
    if root and url_lower.rstrip("/") == root:
        return "home"
    if any(k in url_lower for k in ["/home", "index.html", "index.php"]):
        return "home"
    if any(k in title_lower for k in ["home", "homepage", "الرئيسية", "الصفحة الرئيسية"]):
        return "home"
        
    return "other"


def extract_semantic_sections_from_soup(soup, url: str, title: str, page_type: str) -> List[Dict[str, Any]]:
    try:
        body = soup.body if soup.body else soup
        sections = []
        
        current_heading = "Introduction"
        current_level = 2
        current_text_parts = []
        
        # Traverse elements recursively
        for element in body.find_all(recursive=True):
            if element.name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                body_text = re.sub(r"\s+", " ", " ".join(current_text_parts)).strip()
                if body_text or current_heading != "Introduction":
                    sections.append({
                        "heading": current_heading,
                        "heading_level": current_level,
                        "body_text": body_text,
                        "url": url,
                        "page_title": title,
                        "page_type": page_type
                    })
                current_heading = element.get_text(strip=True)
                try:
                    current_level = int(element.name[1])
                except Exception:
                    current_level = 2
                current_text_parts = []
            elif element.name in ["p", "li", "td", "th", "div", "span"]:
                # Near-leaf nodes to avoid double counting parent block text
                if element.get_text(strip=True) and not element.find(["p", "div", "li", "td", "th"]):
                    text = element.get_text(strip=True)
                    if text:
                        current_text_parts.append(text)
                        
        # Save last section
        body_text = re.sub(r"\s+", " ", " ".join(current_text_parts)).strip()
        if body_text or current_heading != "Introduction":
            sections.append({
                "heading": current_heading,
                "heading_level": current_level,
                "body_text": body_text,
                "url": url,
                "page_title": title,
                "page_type": page_type
            })
            
        return [s for s in sections if s["body_text"]]
    except Exception as e:
        logger.error(f"[brand_site_evidence] Failed to extract semantic sections: {e}")
        return []


def chunk_text(text: str, max_tokens: int = 1000, overlap_tokens: int = 150) -> List[str]:
    # Use word count as proxy: 1 token ~ 0.75 words. Max words = max_tokens * 0.75
    max_words = int(max_tokens * 0.75)
    overlap_words = int(overlap_tokens * 0.75)
    
    words = text.split()
    if len(words) <= max_words:
        return [text]
        
    chunks = []
    start = 0
    while start < len(words):
        end = min(start + max_words, len(words))
        chunk_words = words[start:end]
        chunks.append(" ".join(chunk_words))
        if end == len(words):
            break
        start += max_words - overlap_words
        if start >= end:
            start = end
    return chunks


def build_brand_source_chunks(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    resources = state.get("internal_resources") or []
    all_chunks = []
    
    max_chunks_per_page = 25
    max_total_brand_chunks = 120
    
    for res in resources:
        url = res.get("link") or res.get("url") or ""
        title = res.get("title") or res.get("text") or "Untitled Page"
        
        # Build semantic sections
        sections = res.get("semantic_sections")
        headings = res.get("headings", [])
        page_type = res.get("page_type") or classify_page_type(url, title, headings)
        
        if not sections:
            # Fallback if no parsed sections are present (backward compatibility & plain text test fixtures)
            text_content = res.get("page_text_full") or res.get("page_text") or res.get("text") or ""
            if text_content:
                sections = [{
                    "heading": "Introduction",
                    "heading_level": 2,
                    "body_text": text_content,
                    "url": url,
                    "page_title": title,
                    "page_type": page_type
                }]
                
        if not sections:
            continue
            
        page_chunks = []
        for sec in sections:
            # Enforce heading-aware first, word-based second
            sec_chunks = chunk_text(sec["body_text"], max_tokens=1000, overlap_tokens=150)
            for chunk in sec_chunks:
                page_chunks.append({
                    "text": chunk,
                    "url": sec.get("url") or url,
                    "heading": sec.get("heading") or "Introduction",
                    "page_title": sec.get("page_title") or title,
                    "page_type": sec.get("page_type") or page_type
                })
                
        # Limit to max_chunks_per_page (25)
        all_chunks.extend(page_chunks[:max_chunks_per_page])
        
    # Limit to max_total_brand_chunks (120)
    return all_chunks[:max_total_brand_chunks]


def _compact_brand_page_text(text: str, max_chars: int = 900) -> str:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(text) <= max_chars:
        return text
    sentences = re.split(r"(?<=[.!?])\s+", text)
    selected: List[str] = []
    current = ""
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        candidate = f"{current} {sentence}".strip()
        if len(candidate) > max_chars:
            break
        selected.append(sentence)
        current = candidate
        if len(current) >= 450:
            break
    return (current or text[:max_chars]).strip()


def _chunk_body_text(chunk: Dict[str, Any]) -> str:
    return re.sub(
        r"\s+",
        " ",
        str(chunk.get("observed_text") or chunk.get("text") or chunk.get("body_text") or ""),
    ).strip()


def _chunk_heading_text(chunk: Dict[str, Any]) -> str:
    return re.sub(
        r"\s+",
        " ",
        str(chunk.get("heading") or chunk.get("page_title") or ""),
    ).strip()


def _source_chunk_url(chunk: Dict[str, Any]) -> str:
    return str(chunk.get("source_url") or chunk.get("url") or chunk.get("link") or "").strip()


def _source_chunk_page_type(chunk: Dict[str, Any]) -> str:
    return str(chunk.get("page_type") or "").strip().lower()


def _is_noisy_brand_source_text(text: str, heading: str = "") -> bool:
    combined = re.sub(r"\s+", " ", f"{heading} {text}".strip())
    if not combined:
        return True
    if _BRAND_EVIDENCE_DATE_RE.match(combined):
        return True
    if len(text) < 20 and _BRAND_EVIDENCE_JUNK_RE.match(combined):
        return True
    folded = combined.casefold()
    footer_hits = sum(
        1
        for item in [
            "subscribe", "newsletter", "facebook", "instagram", "linkedin",
            "main menu", "all rights reserved", "scroll to top", "privacy policy",
            "terms of use", "skip to content", "login", "sign up",
        ]
        if item in folded
    )
    if footer_hits >= 3:
        return True
    if _BRAND_EVIDENCE_JUNK_RE.match(heading or "") and not any(
        pattern.search(combined)
        for pattern in [_SERVICE_HINT_RE, _PROJECT_CONTEXT_RE, _PROCESS_HINT_RE, _PRICING_CONTEXT_RE, _TRUST_CONTEXT_RE]
    ):
        return True
    return False


def _extract_technologies_from_text(text: str) -> List[str]:
    tech_capitalization = {
        "react": "React", "node.js": "Node.js", "node": "Node.js", "python": "Python",
        "php": "PHP", "laravel": "Laravel", "wordpress": "WordPress", "mysql": "MySQL",
        "postgresql": "PostgreSQL", "flutter": "Flutter", "swift": "Swift",
        "kotlin": "Kotlin", "figma": "Figma", "adobe xd": "Adobe XD",
        "java": "Java", "html": "HTML", "css": "CSS", "javascript": "JavaScript",
        "aws": "AWS", "docker": "Docker", "bootstrap": "Bootstrap",
        "tailwind": "Tailwind", "erp": "ERP", "crm": "CRM", "pos": "POS",
        "wms": "WMS", "api": "API", "dashboard": "Dashboard",
    }
    found: List[str] = []
    folded = str(text or "").casefold()
    for raw, label in tech_capitalization.items():
        if re.search(r"\b" + re.escape(raw) + r"\b", folded):
            found.append(label)
    return list(dict.fromkeys(found))


def _extract_services_from_text(text: str) -> List[str]:
    folded = str(text or "").casefold()
    candidates: List[str] = []
    service_terms = [
        "web design", "web development", "website development", "web application development",
        "mobile app development", "mobile application development", "ui/ux design", "ux/ui design",
        "e-commerce", "ecommerce", "erp systems", "crm integrations", "crm", "pos software",
        "wms", "dashboard development", "software development", "digital marketing", "seo",
        "branding", "video production", "photography", "motion graphics", "content strategy",
    ]
    for term in service_terms:
        if term in folded:
            candidates.append(term.upper() if term in {"crm", "wms"} else term.title())

    provider_patterns = [
        r"\b(?:we\s+)?(?:provide|offer|deliver|build|develop|specialize in|services include|solutions include)\s+([^.\n]+)",
        r"\b(?:including|such as)\s+([^.\n]+)",
    ]
    for pattern in provider_patterns:
        for match in re.finditer(pattern, text or "", re.IGNORECASE):
            for part in re.split(r"\s*(?:,|;|\||/| and |&|\+)\s*", match.group(1), flags=re.IGNORECASE):
                item = re.sub(r"\s+", " ", part).strip(" .:-")
                if _SERVICE_HINT_RE.search(item):
                    candidates.append(item)

    return _clean_evidence_items(candidates, category="service", limit=18)


def _extract_process_steps_from_text(text: str) -> List[str]:
    candidates: List[str] = []
    folded = str(text or "").casefold()
    for step in [
        "consultation", "planning", "design", "development", "execution",
        "delivery", "testing", "launch", "discovery", "implementation",
        "training", "support",
    ]:
        if re.search(r"\b" + re.escape(step) + r"\b", folded):
            candidates.append(step.title())
    for match in re.finditer(r"(?:step|phase|stage)\s*\d*[:\-\s]+([A-Z][^.\n]{2,80})", text or "", re.IGNORECASE):
        candidates.append(match.group(1).strip())
    return _clean_evidence_items(candidates, category="process", limit=12)


def _extract_projects_from_text(text: str, headings: List[str], page_type: str, url: str, brand_names: List[str]) -> List[str]:
    source_path = urlparse(url or "").path.casefold()
    strong_project_source = (
        page_type in {"portfolio", "projects", "case_study", "case-study"}
        or any(segment in source_path for segment in ["/projects", "/project", "/portfolio", "/case"])
    )
    if not strong_project_source and not _PROJECT_CONTEXT_RE.search(text or ""):
        return []

    candidates: List[str] = []
    for pattern in [
        r"\b(?:client|project|case study)\s*[:\-]\s*([A-Z][A-Za-z0-9&.'\-\s]{2,90}?)(?=(?:[.;\n]\s*(?:client|project|case study|sector|audience|expertise|location|technology|technologies)\s*[:\-])|[.;\n]|$)",
        r"\b(?:app|website|platform)\s*[:\-]\s*([A-Z][A-Za-z0-9&.'\-\s]{2,90}?)(?=(?:[.;\n]\s*(?:client|project|case study|sector|audience|expertise|location|technology|technologies)\s*[:\-])|[.;\n]|$)",
        r"\b(?:mobile app|web app|website|platform)\s+([A-Z][A-Za-z0-9&'\-]*(?:\s+[A-Z][A-Za-z0-9&'\-]*){1,6}(?:\s+(?:Mob App|Mobile App|Web App|Web app|Website|Platform|App))?)\b",
    ]:
        for match in re.finditer(pattern, text or "", re.IGNORECASE):
            candidates.append(match.group(1).strip())

    for line in [line.strip() for line in str(text or "").splitlines() if line.strip()]:
        if re.match(r"^[-*•]\s+", line):
            candidates.append(re.sub(r"^[-*•\s]+", "", line).strip())

    if strong_project_source:
        for heading in headings:
            heading = re.sub(r"\s+", " ", str(heading or "")).strip()
            if heading and not _is_noise_label(heading):
                candidates.append(heading)

    if strong_project_source:
        for fragment in re.split(r"[.;\n]", text or ""):
            candidates.extend(
                re.findall(r"\b[A-Z][a-zA-Z0-9&'-]*(?:\s+[A-Z][a-zA-Z0-9&'-]*){1,5}(?:\s+(?:Mob App|Mobile App|Web App|Web app|Website|Platform|App))?\b", fragment)
            )

    seen = set()
    projects: List[str] = []
    expanded_candidates: List[str] = []
    for candidate in candidates:
        expanded_candidates.append(candidate)
        suffix_match = re.match(
            r"^(.+?)\s+(?:integration|implementation|development|migration|redesign)$",
            str(candidate or "").strip(),
            re.IGNORECASE,
        )
        if suffix_match and len(suffix_match.group(1).split()) >= 2:
            expanded_candidates.append(suffix_match.group(1).strip())

    for candidate in expanded_candidates:
        item = re.sub(
            r"\b(?:location|sector|audience|expertise|technologies used|view project|brief)\b.*$",
            "",
            str(candidate or ""),
            flags=re.IGNORECASE,
        ).strip(" :-\"'")
        item = re.sub(
            r"^(?:(?:all|mobile app|web app|website|websites|platform|design services|seo|portfolio)\s+)+",
            "",
            item,
            flags=re.IGNORECASE,
        ).strip(" :-\"'")
        item = re.sub(r"^(?:project|client|case study)\s+", "", item, flags=re.IGNORECASE).strip(" :-\"'")
        folded = item.casefold()
        if not item or _is_noise_label(item) or any(brand and brand.casefold() in folded for brand in brand_names):
            continue
        if folded in {"mobile app", "web app", "website", "websites", "platform", "portfolio"}:
            continue
        if re.fullmatch(r"(?:mobile|web|app|website|platform|\s)+", folded):
            continue
        cleaned = _sanitize_evidence_item(item, category="project_explicit")
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)
        projects.append(cleaned)
        if len(projects) >= 12:
            break
    return projects


def _extract_explicit_geography_from_text(text: str) -> List[str]:
    return _extract_explicit_brand_geography(text)


def build_brand_page_briefs(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build page-level grounded brand briefs from raw source chunks.

    These briefs are deterministic semantic compression: they preserve observed
    terminology and project/service names, but do not promote noisy card labels
    into claims. They are intended to be the writer-facing truth layer.
    """
    state = state or {}
    chunks = state.get("brand_source_chunks")
    if chunks is None:
        try:
            chunks = build_brand_source_chunks(state)
        except Exception:
            chunks = []
    if not isinstance(chunks, list):
        return []

    brand_names = [str(state.get("brand_name") or "").strip()]
    brand_names.extend(str(alias or "").strip() for alias in state.get("brand_aliases") or [])
    brand_names = [name for name in brand_names if name]

    grouped: Dict[str, Dict[str, Any]] = {}
    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        url = _source_chunk_url(chunk)
        if not url:
            continue
        heading = _chunk_heading_text(chunk)
        body = _chunk_body_text(chunk)
        if _is_noisy_brand_source_text(body, heading):
            continue
        classified_page_type = classify_page_type(url, chunk.get("page_title") or heading, [heading])
        source_page_type = _source_chunk_page_type(chunk)
        entry = grouped.setdefault(url, {
            "url": url,
            "page_type": classified_page_type if classified_page_type != "other" else source_page_type,
            "page_title": str(chunk.get("page_title") or heading or "Brand page").strip(),
            "headings": [],
            "texts": [],
            "chunk_records": [],
        })
        if heading and not _is_noise_label(heading) and heading not in entry["headings"]:
            entry["headings"].append(heading)
        if body:
            entry["texts"].append(body)
            entry["chunk_records"].append({"heading": heading, "text": body})

    briefs: List[Dict[str, Any]] = []
    for url, entry in grouped.items():
        page_type = entry["page_type"] or "other"
        headings = entry["headings"][:12]
        text = "\n".join(entry["texts"])
        if not text:
            continue

        services = _extract_services_from_text(text)
        technologies = _extract_technologies_from_text(text)
        projects = _extract_projects_from_text(text, headings, page_type, url, brand_names)
        if page_type in {"portfolio", "projects", "case_study", "case-study"}:
            record_candidates: List[str] = []
            for record in entry.get("chunk_records", []):
                record_heading = str(record.get("heading") or "").strip()
                record_text = str(record.get("text") or "")
                if record_heading and not _is_noise_label(record_heading) and (
                    _PROJECT_CONTEXT_RE.search(record_text)
                    or re.search(r"\b(?:client|project|case study|sector|expertise)\s*:", record_text, re.IGNORECASE)
                ):
                    record_candidates.append(record_heading)
                record_candidates.extend(_extract_projects_from_text(record_text, [], page_type, url, brand_names))
            projects = _clean_evidence_items(
                list(dict.fromkeys(projects + record_candidates)),
                category="project_explicit",
                limit=12,
            )
        process_steps = _extract_process_steps_from_text(text)
        geography = _extract_explicit_brand_geography(text, page_type)
        pricing = _clean_evidence_items(
            [
                sentence.strip()
                for sentence in re.split(r"(?<=[.!?])\s+|\n", text)
                if _has_explicit_pricing_evidence(sentence, page_type)
            ],
            category="pricing",
            limit=8,
        )
        trust = _clean_evidence_items(
            [
                sentence.strip()
                for sentence in re.split(r"(?<=[.!?])\s+|\n", text)
                if _TRUST_CONTEXT_RE.search(sentence)
            ],
            category="trust",
            limit=8,
        )
        ctas = list(dict.fromkeys(
            cta.title()
            for cta in ["contact", "quote", "call", "email", "phone", "whatsapp", "booking"]
            if re.search(r"\b" + re.escape(cta) + r"\b", text, re.IGNORECASE)
        ))

        source_snippets = []
        for sentence in re.split(r"(?<=[.!?])\s+|\n", text):
            sentence = re.sub(r"\s+", " ", sentence).strip()
            if 35 <= len(sentence) <= 260 and any(
                pattern.search(sentence)
                for pattern in [_SERVICE_HINT_RE, _PROJECT_CONTEXT_RE, _PROCESS_HINT_RE, _PRICING_CONTEXT_RE]
            ):
                snippet = _sanitize_evidence_item(sentence, category="snippet", allow_promotional=True)
                if snippet:
                    source_snippets.append(snippet)
            if len(source_snippets) >= 5:
                break

        summary_parts: List[str] = []
        if services:
            summary_parts.append("Observed services/capabilities include " + ", ".join(services[:8]) + ".")
        if technologies:
            summary_parts.append("Observed terminology/technology includes " + ", ".join(technologies[:8]) + ".")
        if projects:
            summary_parts.append("Observed project/client examples include " + ", ".join(projects[:8]) + ".")
        if process_steps:
            summary_parts.append("Observed process/workflow terms include " + ", ".join(process_steps[:8]) + ".")
        if pricing:
            summary_parts.append("Explicit pricing/package evidence is present on this page.")
        if geography:
            summary_parts.append("Explicit geography mentioned on this page: " + ", ".join(geography[:6]) + ".")
        if not summary_parts and source_snippets:
            summary_parts.append("Useful observed wording: " + source_snippets[0])

        claim_boundaries: List[str] = []
        if not pricing:
            claim_boundaries.append("No explicit pricing/packages observed on this page.")
        if not geography:
            claim_boundaries.append("No explicit geography/local presence observed on this page.")
        if not projects and page_type not in {"portfolio", "projects", "case_study", "case-study"}:
            claim_boundaries.append("No explicit project/case-study examples observed on this page.")

        brief = {
            "source_url": url,
            "url": url,
            "page_type": page_type,
            "page_title": entry["page_title"],
            "headings": headings[:8],
            "grounded_summary": _compact_brand_page_text(" ".join(summary_parts) or text, max_chars=900),
            "observed_services": services[:12],
            "observed_projects": projects[:12],
            "observed_process_steps": process_steps[:10],
            "observed_technologies": technologies[:12],
            "explicit_geography": geography[:8],
            "observed_pricing": pricing[:8],
            "observed_trust_signals": trust[:8],
            "observed_ctas": ctas[:8],
            "source_snippets": list(dict.fromkeys(source_snippets))[:5],
            "claim_boundaries": claim_boundaries,
        }
        if brief["grounded_summary"].strip():
            briefs.append(brief)

    def brief_rank(item: Dict[str, Any]) -> tuple:
        page_type = item.get("page_type") or ""
        evidence_count = sum(len(item.get(key) or []) for key in [
            "observed_services", "observed_projects", "observed_process_steps",
            "observed_technologies", "explicit_geography", "observed_pricing",
        ])
        page_priority = {
            "services": 6, "product": 6, "portfolio": 6, "projects": 6,
            "case_study": 6, "case-study": 6, "home": 4, "about": 3,
            "pricing": 5, "contact": 2,
        }.get(page_type, 1)
        return (-page_priority, -evidence_count, item.get("source_url") or "")

    briefs.sort(key=brief_rank)
    return briefs[:24]


def select_section_brand_page_briefs(section: dict, state: dict, max_briefs: int = 3) -> List[Dict[str, Any]]:
    """
    Select page-level brand briefs for the current section.

    Neutral informational sections receive no brand briefs. Brand-commercial
    offer/proof/process sections receive page summaries even when the approved
    heading is phrased without the brand name.
    """
    if max_briefs <= 0:
        return []
    section = section or {}
    state = state or {}
    section_type = str(section.get("section_type") or "").lower()
    heading_level = str(section.get("heading_level") or "").upper()
    is_intro_or_conclusion = section_type in {"introduction", "intro", "conclusion"} or heading_level == "INTRO"
    if not is_intro_or_conclusion and not _section_should_receive_brand_evidence(section, state):
        return []

    briefs = state.get("brand_page_briefs")
    if briefs is None:
        try:
            briefs = build_brand_page_briefs(state)
        except Exception:
            briefs = []
    if not isinstance(briefs, list) or not briefs:
        return []

    purpose = " ".join([
        str(section.get("heading_text") or ""),
        str(section.get("content_goal") or ""),
        str(section.get("section_intent") or ""),
        " ".join(str(item) for item in section.get("subheadings", []) or []),
    ]).casefold()
    wants_projects = _section_heading_mentions_projects(purpose) or section_type in {"proof", "case_study", "case-study"}
    wants_pricing = _section_heading_mentions_pricing(purpose) or section_type in {"pricing", "packages"}
    wants_process = _section_heading_mentions_process(purpose) or section_type in {"process", "process_or_how"}
    wants_services = (
        section_type in {"offer", "services", "core_or_benefits"}
        or any(term in purpose for term in ["service", "services", "offer", "solution", "provides"])
    )

    if wants_pricing and not any(brief.get("observed_pricing") for brief in briefs if isinstance(brief, dict)):
        return []

    query_tokens = {
        token for token in re.findall(r"[\w\u0600-\u06FF]+", purpose, flags=re.UNICODE)
        if len(token) > 2 and token not in {"the", "and", "for", "with", "how", "why", "what"}
    }
    scored: List[tuple[int, int, Dict[str, Any]]] = []
    for idx, brief in enumerate(briefs):
        if not isinstance(brief, dict):
            continue
        page_type = str(brief.get("page_type") or "").casefold()
        haystack = " ".join([
            str(brief.get("page_title") or ""),
            str(brief.get("grounded_summary") or ""),
            " ".join(brief.get("headings") or []),
            " ".join(brief.get("observed_services") or []),
            " ".join(brief.get("observed_projects") or []),
            " ".join(brief.get("observed_process_steps") or []),
            " ".join(brief.get("observed_technologies") or []),
        ]).casefold()
        score = sum(1 for token in query_tokens if token.casefold() in haystack)
        if wants_projects:
            if page_type in {"portfolio", "projects", "case_study", "case-study"}:
                score += 35
            if brief.get("observed_projects"):
                score += 30
        if wants_services or is_intro_or_conclusion:
            if page_type in {"services", "product", "home"}:
                score += 18
            if brief.get("observed_services"):
                score += 18
        if wants_process:
            if brief.get("observed_process_steps"):
                score += 25
        if wants_pricing:
            if brief.get("observed_pricing"):
                score += 35
            else:
                continue
        if not any([wants_projects, wants_services, wants_process, wants_pricing]) and brief.get("observed_services"):
            score += 8
        if score > 0:
            compact = dict(brief)
            compact["grounded_summary"] = _compact_brand_page_text(compact.get("grounded_summary", ""), 900)
            scored.append((score, idx, compact))

    scored.sort(key=lambda item: (-item[0], item[1]))
    selected: List[Dict[str, Any]] = []
    seen_urls = set()
    for _, _, brief in scored:
        url = brief.get("source_url") or brief.get("url")
        if url in seen_urls:
            continue
        seen_urls.add(url)
        selected.append(brief)
        if len(selected) >= max_briefs:
            break
    return selected


def retrieve_brand_source_chunks(section: dict, state: dict, top_k: int = 3) -> List[Dict[str, Any]]:
    # Get chunks or compile them dynamically
    chunks = state.get("brand_source_chunks")
    if chunks is None:
        chunks = build_brand_source_chunks(state)
        state["brand_source_chunks"] = chunks
        
    if not chunks:
        return []
        
    heading = (section.get("heading_text") or "").lower()
    purpose = (section.get("content_goal") or "").lower()
    intent = (section.get("section_intent") or "").lower()
    primary_keyword = (state.get("primary_keyword") or "").lower()
    brand_name = (state.get("brand_name") or "").lower()
    
    # Pre-flight relevance guard: raw brand chunks are only injected when the
    # visible section heading/subheadings name the brand. Generic commercial
    # sections should not turn into brand catalog copy by accident.
    if not _section_should_receive_brand_evidence(section, state):
        return []
    
    brand_aliases = state.get("brand_aliases")
    if not isinstance(brand_aliases, list) or not all(isinstance(a, str) and a.strip() for a in brand_aliases):
        brand_aliases = []
    brand_aliases = [a.lower().strip() for a in brand_aliases if a.strip()]
    brand_aliases_str = " ".join(brand_aliases)
    
    import re
    query_text = f"{heading} {purpose} {intent} {primary_keyword} {brand_name} {brand_aliases_str}"
    query_tokens = {t.lower() for t in re.findall(r"\w+", query_text) if len(t) >= 2}
    
    is_service_section = any(k in heading or k in purpose or k in intent for k in [
        "service", "services", "product", "products", "workflow", "process", "portfolio", "projects", "features", "capabilities",
        "خدمة", "خدمات", "منتج", "منتجات", "عمل", "طريقة", "سابقة أعمال", "مشاريع", "مميزات", "خصائص"
    ])
    
    scored_chunks = []
    for chunk in chunks:
        score = 0
        chunk_text_lower = chunk["text"].lower()
        chunk_heading_lower = chunk["heading"].lower()
        chunk_title_lower = chunk["page_title"].lower()
        chunk_page_type = chunk["page_type"].lower()
        
        # 1. Matching token overlap
        for token in query_tokens:
            if token in chunk_heading_lower:
                score += 5
            if token in chunk_title_lower:
                score += 3
            if token in chunk_text_lower:
                score += 1
                
        # 2. Page type boosts
        if any(k in heading or k in purpose for k in ["about", "who we are", "من نحن", "عن الشركة", "تأسيس", "قصة"]):
            if chunk_page_type == "about":
                score += 15
        if any(k in heading or k in purpose for k in ["service", "product", "features", "capabilities", "خدمة", "منتج", "مزايا"]):
            if chunk_page_type in ["services", "product"]:
                score += 15
        if any(k in heading or k in purpose for k in ["pricing", "cost", "package", "price", "سعر", "تكلفة", "باقات"]):
            if chunk_page_type == "pricing":
                score += 15
        if any(k in heading or k in purpose for k in ["portfolio", "project", "work", "أعمال", "مشاريع", "سابقة"]):
            if chunk_page_type == "portfolio":
                score += 15
                
        # 3. Suppress homepage domination for service/workflow/portfolio sections
        if is_service_section and chunk_page_type == "home":
            # Give a heavy penalty so non-homepage chunks rank higher
            score = max(1, score - 8)
            
        if score > 0:
            scored_chunks.append((score, chunk))
            
    scored_chunks.sort(key=lambda x: x[0], reverse=True)
    
    # Return top_k unique chunks
    unique_chunks = []
    seen_texts = set()
    for _, chunk in scored_chunks:
        txt_hash = chunk["text"].strip().lower()
        if txt_hash not in seen_texts:
            seen_texts.add(txt_hash)
            # Limit character length per chunk to 1200
            truncated_text = chunk["text"][:1200]
            new_chunk = dict(chunk)
            new_chunk["text"] = truncated_text
            unique_chunks.append(new_chunk)
            if len(unique_chunks) >= top_k:
                break
                
    return unique_chunks


def select_section_raw_brand_blocks(section: dict, state: dict, max_blocks: int = 4) -> List[Dict[str, Any]]:
    """
    Select compact raw, page-backed brand blocks for brand-owned sections.

    Cards/inventory can route availability, but returned blocks are built from
    raw source chunks and are intended to be the writer's factual grounding.
    This function is deterministic and does not mutate state.
    """
    if max_blocks <= 0:
        return []

    section = section or {}
    state = state or {}
    section_type = str(section.get("section_type") or "").lower()
    heading_level = str(section.get("heading_level") or "").upper()
    is_intro_or_conclusion = section_type in {"introduction", "intro", "conclusion"} or heading_level == "INTRO"
    if not is_intro_or_conclusion and not _section_should_receive_brand_evidence(section, state):
        return []

    chunks = state.get("brand_source_chunks")
    if chunks is None:
        try:
            chunks = build_brand_source_chunks(state)
        except Exception:
            chunks = []
    if not isinstance(chunks, list) or not chunks:
        return []

    inventory = state.get("brand_evidence_inventory")
    if not isinstance(inventory, dict):
        inventory_state = dict(state)
        inventory_state["brand_source_chunks"] = chunks
        try:
            inventory = build_brand_evidence_inventory(inventory_state)
        except Exception:
            inventory = {}

    heading = str(section.get("heading_text") or "")
    purpose = " ".join([
        heading,
        str(section.get("content_goal") or ""),
        str(section.get("section_intent") or ""),
        " ".join(str(item) for item in section.get("subheadings", []) or []),
    ]).lower()

    wants_projects = section_type in {"proof", "case_study", "case-study"} or any(
        term in purpose
        for term in ["project", "projects", "portfolio", "case study", "case studies", "clients", "examples", "Ù…Ø´Ø§Ø±ÙŠØ¹", "Ù†Ù…Ø§Ø°Ø¬", "Ø£Ø¹Ù…Ø§Ù„", "Ø¹Ù…Ù„Ø§Ø¡"]
    )
    wants_services = section_type in {"offer", "services", "core_or_benefits"} or any(
        term in purpose
        for term in ["service", "services", "product", "products", "offer", "provides", "solutions", "Ø®Ø¯Ù…Ø§Øª", "Ø®Ø¯Ù…Ø©", "Ø­Ù„ÙˆÙ„"]
    )
    wants_process = section_type in {"process", "process_or_how"} or any(
        term in purpose
        for term in ["process", "workflow", "steps", "stages", "how", "Ø®Ø·ÙˆØ§Øª", "Ù…Ø±Ø§Ø­Ù„", "ÙƒÙŠÙ"]
    )
    wants_pricing = section_type in {"pricing", "packages"} or any(
        term in purpose
        for term in ["pricing", "price", "prices", "package", "packages", "cost", "Ø³Ø¹Ø±", "Ø£Ø³Ø¹Ø§Ø±", "ØªÙƒÙ„ÙØ©", "Ø¨Ø§Ù‚Ø§Øª", "Ø¨Ø§Ù‚Ø©"]
    )
    wants_trust = section_type in {"differentiation", "features", "brand_support"} or any(
        term in purpose
        for term in ["trust", "certified", "certification", "reviews", "testimonials", "why choose", "Ù„Ù…Ø§Ø°Ø§", "Ø«Ù‚Ø©", "Ù…Ø¹ØªÙ…Ø¯"]
    )

    if wants_pricing and not inventory.get("pricing_available"):
        return []

    def chunk_url(chunk: Dict[str, Any]) -> str:
        return str(chunk.get("url") or chunk.get("link") or "").strip()

    def chunk_page_type(chunk: Dict[str, Any]) -> str:
        return str(chunk.get("page_type") or "").strip().lower()

    def chunk_heading(chunk: Dict[str, Any]) -> str:
        return str(chunk.get("heading") or chunk.get("page_title") or "").strip()

    def raw_text(chunk: Dict[str, Any]) -> str:
        return re.sub(r"\s+", " ", str(chunk.get("text") or chunk.get("body_text") or "")).strip()

    def is_explicit_pricing_text(text: str, page_type: str) -> bool:
        if page_type == "pricing":
            return True
        if _CURRENCY_RE.search(text):
            return True
        if re.search(r"\b(?:package|packages|plan|plans)\s*[:\-]\s*\w", text, re.IGNORECASE):
            return True
        if re.search(r"(?:Ø¨Ø§Ù‚Ø©|Ø¨Ø§Ù‚Ø§Øª|Ø®Ø·Ø©)\s*[:\-]\s*\S", text):
            return True
        return False

    def is_noisy_chunk(chunk: Dict[str, Any]) -> bool:
        text = raw_text(chunk)
        heading = chunk_heading(chunk)
        combined = f"{heading} {text}".strip()
        if not combined:
            return True
        if _BRAND_EVIDENCE_DATE_RE.match(combined):
            return True
        if len(text) < 20 and _BRAND_EVIDENCE_JUNK_RE.match(combined):
            return True
        folded = combined.casefold()
        hard_noise = [
            "subscribe newsletter", "subscribe to newsletter", "newsletter",
            "copyright", "all rights reserved", "privacy policy", "terms of use",
            "cookie policy", "skip to content", "scroll to top", "main menu",
            "footer menu", "navigation", "login", "sign up",
            "Ø§Ø´ØªØ±Ùƒ ÙÙŠ Ø§Ù„Ù†Ø´Ø±Ø©", "Ø§Ù„Ù†Ø´Ø±Ø© Ø§Ù„Ø¨Ø±ÙŠØ¯ÙŠØ©",
        ]
        footer_noise_hits = sum(
            1
            for item in [
                "subscribe", "facebook", "instagram", "linkedin", "main menu",
                "all rights reserved", "scroll to top", "let's talk", "lets talk",
                "email subscribe", "twitter", "x-twitter",
            ]
            if item in folded
        )
        generic_noise_headings = {
            "subscribe newsletters", "subscribe newsletter", "about us", "history",
            "our mission", "our vision", "testimonials", "faqs", "contact us",
        }
        if footer_noise_hits >= 3 or heading.casefold() in generic_noise_headings:
            return True
        if any(item in folded for item in hard_noise):
            evidence_present = any(
                pattern.search(combined)
                for pattern in [_SERVICE_HINT_RE, _PROJECT_CONTEXT_RE, _PROCESS_HINT_RE, _PRICING_CONTEXT_RE, _TRUST_CONTEXT_RE]
            )
            if not evidence_present:
                return True
        tokens = re.findall(r"\w+", combined)
        if len(tokens) <= 3 and not any(pattern.search(combined) for pattern in [_SERVICE_HINT_RE, _PROJECT_CONTEXT_RE, _PROCESS_HINT_RE, _CURRENCY_RE]):
            return True
        return False

    query_tokens = {
        token
        for token in re.findall(r"\w+", purpose)
        if len(token) > 2 and token not in {"the", "and", "for", "with", "how", "why", "what"}
    }

    def score_chunk(chunk: Dict[str, Any]) -> int:
        page_type = chunk_page_type(chunk)
        text = raw_text(chunk)
        heading_text = chunk_heading(chunk)
        url_path = urlparse(chunk_url(chunk)).path.casefold()
        haystack = f"{page_type} {heading_text} {text}".lower()
        score = sum(1 for token in query_tokens if token.lower() in haystack)

        if wants_projects:
            if page_type in {"portfolio", "projects", "case_study", "case-study"}:
                score += 20
            if any(segment in url_path for segment in ["/projects", "/project", "/portfolio", "/case"]):
                score += 35
            if any(segment in url_path for segment in ["/about", "/blog"]):
                score -= 18
            if re.search(r"\b(?:client|project|case study)\s*:", text, re.IGNORECASE):
                score += 18
            if heading_text and not _BRAND_EVIDENCE_JUNK_RE.match(heading_text) and len(heading_text.split()) <= 6:
                score += 8
            if _PROJECT_CONTEXT_RE.search(text):
                score += 8
        if wants_services or is_intro_or_conclusion:
            if page_type in {"services", "product"}:
                score += 18
            elif page_type == "home":
                score += 8
            if _SERVICE_HINT_RE.search(text):
                score += 6
        if wants_process:
            if _PROCESS_HINT_RE.search(text):
                score += 18
            if page_type in {"services", "about", "home", "process"}:
                score += 5
        if wants_pricing:
            if _has_explicit_pricing_evidence(text, page_type):
                score += 25
            else:
                return 0
        if wants_trust:
            if _TRUST_CONTEXT_RE.search(text):
                score += 12
        if page_type == "blog":
            score -= 8
        return max(score, 0)

    def compact_text(text: str, max_chars: int = 700) -> str:
        text = re.sub(r"\s+", " ", text or "").strip()
        if len(text) <= max_chars:
            return text
        sentences = re.split(r"(?<=[.!?ØŸ])\s+", text)
        selected: List[str] = []
        current = ""
        for sentence in sentences:
            candidate = (current + " " + sentence).strip()
            if len(candidate) > max_chars:
                if current:
                    break
                return sentence[:max_chars].strip()
            current = candidate
            selected.append(sentence)
            if len(current) >= 300:
                break
        return (current or text[:max_chars]).strip()

    def extract_observed_facts(text: str) -> List[str]:
        facts: List[str] = []
        for part in re.split(r"(?<=[.!?ØŸ])\s+|[;\n]", text):
            sentence = re.sub(r"\s+", " ", part).strip(" -")
            if len(sentence) < 4:
                continue
            category = None
            if _CURRENCY_RE.search(sentence) or _PRICING_CONTEXT_RE.search(sentence):
                category = "pricing"
            elif _PROJECT_CONTEXT_RE.search(sentence):
                category = "project"
            elif _PROCESS_HINT_RE.search(sentence):
                category = "process"
            elif _TRUST_CONTEXT_RE.search(sentence):
                category = "trust"
            elif _SERVICE_HINT_RE.search(sentence):
                category = "service"
            elif _GEOGRAPHY_CONTEXT_RE.search(sentence):
                category = "geography"
            if not category:
                continue
            cleaned = _sanitize_evidence_item(sentence, category="snippet", allow_promotional=True)
            if cleaned:
                facts.append(cleaned)
            if len(facts) >= 6:
                break
        return list(dict.fromkeys(facts))

    scored: List[tuple[int, int, Dict[str, Any]]] = []
    for idx, chunk in enumerate(chunks):
        if not isinstance(chunk, dict) or is_noisy_chunk(chunk):
            continue
        score = score_chunk(chunk)
        if score > 0:
            scored.append((score, idx, chunk))

    scored.sort(key=lambda item: (-item[0], item[1]))

    blocks: List[Dict[str, Any]] = []
    seen = set()
    for _, _, chunk in scored:
        text = raw_text(chunk)
        compact = compact_text(text)
        key = (chunk_url(chunk), chunk_heading(chunk).casefold(), compact.casefold())
        if key in seen:
            continue
        seen.add(key)
        blocks.append({
            "source_url": chunk_url(chunk),
            "page_type": chunk_page_type(chunk) or "other",
            "heading": chunk_heading(chunk) or "Introduction",
            "observed_text": compact,
            "observed_facts": extract_observed_facts(compact),
        })
        if len(blocks) >= max_blocks:
            break

    return blocks


def build_section_brand_understanding(section: dict, state: dict, retrieved_chunks: list) -> dict:
    import re
    retrieved_chunks = retrieved_chunks or []
    heading = (section.get("heading_text") or "").strip()
    intent = (section.get("section_intent") or "Informational").strip()
    
    brief = {
        "section_heading": heading,
        "section_intent": intent,
        "relevant_services": [],
        "relevant_projects": [],
        "relevant_process_steps": [],
        "relevant_technologies": [],
        "relevant_geography": [],
        "relevant_ctas": [],
        "useful_source_snippets": [],
        "not_supported_for_this_section": [],
        "recommended_angle": {
            "focus_types": [],
            "avoid_types": [],
            "best_evidence_categories": [],
            "preferred_section_style": "general_guidance"
        }
    }
    
    brand_name = (state.get("brand_name") or "").strip()
    brand_aliases = state.get("brand_aliases") or []
    brand_names_set = {brand_name.lower()} | {a.lower() for a in brand_aliases if a}
    
    cards = state.get("brand_evidence_cards") or []
    if not retrieved_chunks and not cards:
        brief["recommended_angle"]["preferred_section_style"] = "general_guidance"
        brief["recommended_angle"]["focus_types"] = ["general industry criteria", "editorial advice"]
        brief["recommended_angle"]["avoid_types"] = ["unsupported brand claims", "mock statistics"]
        return brief

    # Concatenate chunk texts/headings plus structured evidence-card fields. The
    # card backfill keeps project/service sections grounded even when retrieval
    # selected broad service chunks instead of the portfolio page.
    chunk_texts = [str(c.get("text") or "") for c in retrieved_chunks if isinstance(c, dict)]
    chunk_headings = [str(c.get("heading") or "") for c in retrieved_chunks if isinstance(c, dict)]
    card_texts: List[str] = []
    card_headings: List[str] = []
    if isinstance(cards, list):
        for card in cards:
            if not isinstance(card, dict) or card.get("excluded_reason"):
                continue
            card_headings.extend([str(card.get("title") or ""), str(card.get("page_type") or "")])
            card_headings.extend([str(item) for item in card.get("headings") or []])
            for key in [
                "visible_products_or_services",
                "visible_features_or_capabilities",
                "visible_process_steps",
                "visible_conversion_actions",
                "visible_trust_signals",
                "visible_geography",
                "visible_project_or_case_study_examples",
                "visible_support_or_contact_methods",
                "usable_snippets",
            ]:
                card_texts.extend([str(item) for item in card.get(key) or []])

    all_text = "\n".join(chunk_texts + card_texts).replace('\\n', '\n')
    all_headings = "\n".join(chunk_headings + card_headings)
    combined_corpus = all_text + "\n" + all_headings
    
    # 1. Geography
    geo_keywords = [
        "saudi arabia", "saudi", "riyadh", "jeddah", "dammam", "mecca", "medina",
        "egypt", "cairo", "alexandria", "giza", "uae", "dubai", "abu dhabi",
        "السعودية", "الرياض", "جدة", "الدمام", "مكة", "المدينة", "مصر", "القاهرة", "الاسكندرية", "الجيزة", "الإمارات", "دبي", "أبوظبي"
    ]
    card_geography = _collect_card_values(cards, ["visible_geography"], limit=8, category="geography")
    geo_context_chunks = "\n".join(
        str(c.get("text") or "")
        for c in retrieved_chunks
        if isinstance(c, dict) and _GEOGRAPHY_CONTEXT_RE.search(str(c.get("text") or ""))
    )
    for geo in geo_keywords:
        if geo in geo_context_chunks.lower():
            cleaned_geo = _sanitize_evidence_item(geo.title() if geo.isascii() else geo, "geography")
            if cleaned_geo:
                brief["relevant_geography"].append(cleaned_geo)
    brief["relevant_geography"] = list(dict.fromkeys(card_geography + brief["relevant_geography"]))
    
    # 2. Technologies
    tech_keywords = [
        "react", "node.js", "node", "python", "php", "laravel", "wordpress", "mysql", "postgresql", 
        "flutter", "swift", "kotlin", "figma", "adobe xd", "adobe", "java", "html", "css", "javascript", 
        "aws", "docker", "git", "bootstrap", "tailwind", "erp", "crm", "pos", "api", "dashboard",
        "ووردبريس", "لارافل", "فيجما", "أدوبي", "فلاتر", "سويفت", "كوتلن"
    ]
    tech_capitalization = {
        "react": "React", "node.js": "Node.js", "node": "Node.js", "python": "Python", "php": "PHP", 
        "laravel": "Laravel", "wordpress": "WordPress", "mysql": "MySQL", "postgresql": "PostgreSQL", 
        "flutter": "Flutter", "swift": "Swift", "kotlin": "Kotlin", "figma": "Figma", 
        "adobe xd": "Adobe XD", "adobe": "Adobe", "java": "Java", "html": "HTML", "css": "CSS", 
        "javascript": "JavaScript", "aws": "AWS", "docker": "Docker", "git": "Git", 
        "bootstrap": "Bootstrap", "tailwind": "Tailwind", "erp": "ERP", "crm": "CRM", "pos": "POS", 
        "api": "API", "dashboard": "Dashboard"
    }
    for tech in tech_keywords:
        if tech.isascii():
            pattern = r'\b' + re.escape(tech) + r'\b'
            if re.search(pattern, combined_corpus.lower()):
                brief["relevant_technologies"].append(tech_capitalization.get(tech.lower(), tech.upper() if len(tech) <= 4 else tech.title()))
        else:
            if tech in combined_corpus:
                brief["relevant_technologies"].append(tech_capitalization.get(tech.lower(), tech))
    brief["relevant_technologies"] = list(dict.fromkeys(brief["relevant_technologies"]))
    
    # 3. Services
    service_keywords = [
        "web design", "web development", "ui/ux design", "mobile app development", "e-commerce", 
        "erp systems", "crm integrations", "pos software", "hosting", "domain registration", "seo", "marketing",
        "تصميم المواقع", "تطوير المواقع", "برمجة التطبيقات", "متاجر إلكترونية", "أنظمة ERP", "برامج POS", 
        "خدمات الاستضافة", "التسويق الرقمي", "تحسين محركات البحث", "كتابة المحتوى", "هوية بصرية"
    ]
    for srv in service_keywords:
        if srv in combined_corpus.lower():
            brief["relevant_services"].append(srv.title() if srv.isascii() else srv)
    brief["relevant_services"].extend(_collect_card_values(cards, ["visible_products_or_services"], limit=10, category="service"))
    brief["relevant_services"].extend(_collect_card_values(cards, ["visible_features_or_capabilities"], limit=10, category="capability"))
    brief["relevant_services"] = _clean_evidence_items(list(dict.fromkeys(brief["relevant_services"])), category="service", limit=14)
    
    # 4. Process/Workflow steps
    process_keywords = [
        "consultation & planning", "design & development", "execution & delivery", "testing", "launch",
        "planning", "design", "development", "execution", "delivery",
        "الاستشارة والتخطيط", "التصميم والتطوير", "التنفيذ والتسليم", "مرحلة التخطيط", "مرحلة التصميم", 
        "مرحلة البرمجة", "مرحلة الاختبار", "مرحلة الإطلاق"
    ]
    for step in process_keywords:
        if step in combined_corpus.lower():
            brief["relevant_process_steps"].append(step.title() if step.isascii() else step)
    brief["relevant_process_steps"] = _clean_evidence_items(list(dict.fromkeys(brief["relevant_process_steps"])), category="process", limit=10)
    
    # 5. CTAs
    cta_keywords = [
        "quote", "contact", "call", "form", "whatsapp", "phone", "email", "booking",
        "اتصل", "تواصل", "طلب عرض", "واتساب", "هاتف", "نموذج", "البريد"
    ]
    for cta in cta_keywords:
        if cta in combined_corpus.lower():
            brief["relevant_ctas"].append(cta.title() if cta.isascii() else cta)
    brief["relevant_ctas"] = list(dict.fromkeys(brief["relevant_ctas"]))
    
    # 6. Projects (Multi-signal extraction)
    card_projects = _collect_card_values(cards, ["visible_project_or_case_study_examples"], limit=12, category="project_explicit")
    list_projects = []
    lines = [line.strip() for line in all_text.split('\n') if line.strip()]
    for line in lines:
        if line.startswith(('-', '•', '*', '1.', '2.', '3.')):
            cleaned_line = re.sub(r'^[-\s•*0-9.]+', '', line).strip()
            cleaned_project = _sanitize_evidence_item(cleaned_line, "project")
            if cleaned_project and len(cleaned_project) < 80:
                list_projects.append(cleaned_project)
                
    proper_nouns = []
    for source_line in all_text.splitlines():
        proper_nouns.extend(
            re.findall(r'\b[A-Z][a-zA-Z0-9]*(?:\s+[A-Z][a-zA-Z0-9]*){1,3}\b', source_line)
        )
    
    ignored_entities = {
        "saudi", "saudi arabia", "egypt", "cairo", "dubai", "uae",
        "consulting", "planning", "design", "development", "execution", "delivery", "testing", "launch",
        "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december",
        "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"
    }
    
    extracted_projects = []
    card_project_keys = {p.casefold() for p in card_projects}
    for pn in proper_nouns:
        pn_lower = pn.lower()
        if any(brand in pn_lower for brand in brand_names_set):
            continue
        if pn_lower in ignored_entities:
            continue
        if any(ign in pn_lower for ign in ["terms of", "privacy policy", "contact us", "about us", "home page"]):
            continue
        words = pn_lower.split()
        if all(w in ignored_entities for w in words):
            continue
        cleaned_project = _sanitize_evidence_item(pn, "project")
        if cleaned_project:
            cleaned_key = cleaned_project.casefold()
            if any(
                cleaned_key != card_key and (cleaned_key in card_key or card_key in cleaned_key)
                for card_key in card_project_keys
            ):
                continue
            extracted_projects.append(cleaned_project)
        
    ar_projects = re.findall(r'(?:مشروع|تطبيق|موقع|شركة|عميل|project|app|website|client|platform)\s+«([^»]+)»', all_text, re.IGNORECASE)
    
    for chunk in retrieved_chunks:
        if isinstance(chunk, dict) and chunk.get("page_type") in ["projects", "portfolio"]:
            ch_heading = chunk.get("heading", "").strip()
            if ch_heading and ch_heading.lower() not in ["projects", "portfolio", "أعمالنا", "مشاريعنا", "our work"]:
                cleaned_heading_project = _sanitize_evidence_item(ch_heading, "project")
                if cleaned_heading_project:
                    extracted_projects.append(cleaned_heading_project)
                
    all_extracted = card_projects + extracted_projects + list_projects + [
        p for p in (_sanitize_evidence_item(item, "project") for item in ar_projects)
        if p
    ]
    seen_projs = set()
    for p in all_extracted:
        p_clean = p.strip().strip('"').strip("'").strip("«").strip("»").strip()
        if not p_clean or len(p_clean) < 3 or len(p_clean) > 80:
            continue
        p_lower = p_clean.lower()
        if any(brand in p_lower for brand in brand_names_set):
            continue
        if p_lower in seen_projs:
            continue
        seen_projs.add(p_lower)
        brief["relevant_projects"].append(p_clean)
        
    brief["relevant_projects"] = brief["relevant_projects"][:8]
    
    # 7. Useful source snippets (raw sentences/fragments)
    sentences = []
    raw_sentences = re.split(r'[.!?؟\n•|]+', all_text)
    for s in raw_sentences:
        s_clean = s.strip()
        if len(s_clean) > 20 and len(s_clean) < 250:
            s_lower = s_clean.lower()
            has_brand = any(brand in s_lower for brand in brand_names_set)
            has_evidence = any(kw in s_lower for kw in service_keywords + tech_keywords + process_keywords)
            if has_brand or has_evidence:
                snippet = _sanitize_evidence_item(s_clean, "snippet", allow_promotional=True)
                if snippet:
                    sentences.append(snippet)
    brief["useful_source_snippets"] = list(dict.fromkeys(sentences))[:4]
    
    # 8. Not supported warnings
    heading_lower = heading.lower()
    
    is_project_heading = any(k in heading_lower for k in ["مشاريع", "سابقة أعمال", "عملاء", "أعمالنا", "projects", "portfolio", "clients", "case studies", "examples"])
    if is_project_heading and not brief["relevant_projects"]:
        brief["not_supported_for_this_section"].append("مشاريع وسوابق أعمال محددة غير متوفرة في الأدلة المرصودة لهذه الشركة.")

    is_pricing_heading = any(k in heading_lower for k in ["باقات", "باقة", "أسعار", "اسعار", "تكلفة", "pricing", "packages", "package", "cost"])
    has_pricing_evidence = bool(_collect_card_values(cards, ["visible_pricing_or_packages"], limit=4, category="pricing"))
    if is_pricing_heading and not has_pricing_evidence:
        brief["not_supported_for_this_section"].append("تسعير أو باقات خاصة بالبراند غير مدعومة صراحة في أدلة هذا القسم.")
        
    is_tech_heading = any(k in heading_lower for k in ["تقنيات", "أدوات", "لغات", "برامج", "technology", "technologies", "tools", "stack", "platforms"])
    if is_tech_heading and not brief["relevant_technologies"]:
        brief["not_supported_for_this_section"].append("تقنيات أو أدوات برمجية محددة غير متوفرة في الأدلة المرصودة لهذه الشركة.")
        
    is_process_heading = any(k in heading_lower for k in ["خطوات", "مراحل", "طريقة", "كيف نعمل", "workflow", "process", "steps", "stages"])
    if is_process_heading and not brief["relevant_process_steps"]:
        brief["not_supported_for_this_section"].append("خطوات أو مراحل عمل تشغيلية محددة غير متوفرة في الأدلة المرصودة لهذه الشركة.")
        
    is_saudi_heading = any(k in heading_lower for k in ["السعودية", "الرياض", "جدة", "saudi", "riyadh", "jeddah"])
    has_saudi_evidence = any("saudi" in g.lower() or "السعودية" in g for g in brief["relevant_geography"])
    if is_saudi_heading and not has_saudi_evidence:
        brief["not_supported_for_this_section"].append("حضور جغرافي أو ترخيص عمل داخل السعودية غير مدعوم صراحة في أدلة هذا القسم.")
        
    is_why_heading = any(k in heading_lower for k in ["لماذا", "مميزات", "أفضل", "why choose", "why trust", "advantages", "differentiators"])
    has_trust_evidence = bool(brief["relevant_services"] or brief["relevant_process_steps"] or brief["relevant_technologies"] or brief["relevant_projects"])
    if is_why_heading and not has_trust_evidence:
        brief["not_supported_for_this_section"].append("لا توجد ميزات تنافسية ملموسة (تقنيات، سوابق أعمال، خدمات) لدعم مزايا التعامل مع الشركة.")

    # 9. Structured recommended_angle
    has_rich_evidence = len(brief["relevant_services"]) >= 2 or len(brief["relevant_projects"]) >= 1 or len(brief["relevant_process_steps"]) >= 2 or len(brief["relevant_technologies"]) >= 2
    
    if has_rich_evidence:
        brief["recommended_angle"]["preferred_section_style"] = "evidence_grounded"
        
        if is_project_heading:
            brief["recommended_angle"]["focus_types"] = ["specific observed projects", "raw source snippets"]
        elif is_tech_heading:
            brief["recommended_angle"]["focus_types"] = ["observed tools and platforms", "specific technical workflows"]
        elif is_process_heading:
            brief["recommended_angle"]["focus_types"] = ["actual workflow stages", "delivery milestones"]
        elif is_why_heading:
            brief["recommended_angle"]["focus_types"] = ["observed service differentiators", "concrete process milestones"]
        else:
            brief["recommended_angle"]["focus_types"] = ["observed services", "verified technical capabilities"]
            
        brief["recommended_angle"]["avoid_types"] = ["generic praise", "unsupported location claims", "abstract marketing prose"]
        
        cats = []
        if brief["relevant_projects"]: cats.append("portfolio")
        if brief["relevant_services"]: cats.append("services")
        if brief["relevant_technologies"]: cats.append("technologies")
        if brief["relevant_process_steps"]: cats.append("process_stages")
        brief["recommended_angle"]["best_evidence_categories"] = cats
    else:
        brief["recommended_angle"]["preferred_section_style"] = "general_guidance"
        brief["recommended_angle"]["focus_types"] = ["general industry criteria", "editorial advice"]
        brief["recommended_angle"]["avoid_types"] = ["mock statistics", "unsupported brand assertions"]
        brief["recommended_angle"]["best_evidence_categories"] = ["market_standards"]

    return brief


def _section_understanding_heading_text(section: dict) -> str:
    return " ".join([
        str((section or {}).get("heading_text") or ""),
        str((section or {}).get("content_goal") or ""),
        str((section or {}).get("section_intent") or ""),
        " ".join(str(item) for item in ((section or {}).get("subheadings") or [])),
    ]).casefold()


def _section_heading_mentions_projects(heading_blob: str) -> bool:
    return any(term in heading_blob for term in [
        "project", "projects", "portfolio", "case study", "case studies", "client", "clients", "examples",
        "Ù…Ø´Ø§Ø±ÙŠØ¹", "Ù†Ù…Ø§Ø°Ø¬", "Ø£Ø¹Ù…Ø§Ù„", "Ø¹Ù…Ù„Ø§Ø¡", "Ø³Ø§Ø¨Ù‚Ø©",
    ])


def _section_heading_mentions_pricing(heading_blob: str) -> bool:
    return any(term in heading_blob for term in [
        "pricing", "price", "prices", "package", "packages", "cost", "fee", "budget",
        "Ø³Ø¹Ø±", "Ø£Ø³Ø¹Ø§Ø±", "Ø§Ø³Ø¹Ø§Ø±", "ØªÙƒÙ„ÙØ©", "Ø¨Ø§Ù‚Ø§Øª", "Ø¨Ø§Ù‚Ø©",
    ])


def _section_heading_mentions_process(heading_blob: str) -> bool:
    return any(term in heading_blob for term in [
        "process", "workflow", "steps", "stages", "how it works",
        "Ø®Ø·ÙˆØ§Øª", "Ù…Ø±Ø§Ø­Ù„", "Ø·Ø±ÙŠÙ‚Ø©", "ÙƒÙŠÙ",
    ])


def _section_heading_mentions_geography(heading_blob: str) -> bool:
    if any(term in heading_blob for term in [
        " market", "local", "location", "city", "country", "service area",
        "saudi", "riyadh", "jeddah", "cairo", "egypt", "dubai", "uae",
        "Ø§Ù„Ø³Ø¹ÙˆØ¯ÙŠØ©", "Ø§Ù„Ø±ÙŠØ§Ø¶", "Ø¬Ø¯Ø©", "Ù…ØµØ±", "Ø§Ù„Ù‚Ø§Ù‡Ø±Ø©", "Ø¯Ø¨ÙŠ", "ÙÙŠ",
        "\u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629", "\u0627\u0644\u0633\u0639\u0648\u062f\u064a", "\u0627\u0644\u0631\u064a\u0627\u0636", "\u062c\u062f\u0629", "\u0645\u0635\u0631", "\u0627\u0644\u0642\u0627\u0647\u0631\u0629", "\u062f\u0628\u064a",
    ]):
        return True
    return bool(re.search(r"\b(?:inside|within|across)\s+[A-Z][A-Za-z .'-]{2,40}\b", heading_blob, re.IGNORECASE))


def _section_understanding_blocks(raw_items: list) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for item in raw_items or []:
        if not isinstance(item, dict):
            continue
        observed_text = item.get("observed_text")
        if observed_text is None:
            observed_text = item.get("text") or item.get("body_text") or ""
        observed_text = str(observed_text or "").replace("\\n", "\n")
        observed_text = re.sub(r"[ \t]+", " ", observed_text)
        observed_text = re.sub(r"\n\s*", "\n", observed_text).strip()
        if not observed_text:
            continue
        blocks.append({
            "source_url": str(item.get("source_url") or item.get("url") or item.get("link") or "").strip(),
            "page_type": str(item.get("page_type") or "").strip().lower(),
            "heading": str(item.get("heading") or item.get("page_title") or "").strip(),
            "observed_text": observed_text,
            "observed_facts": [
                re.sub(r"\s+", " ", str(fact or "")).strip()
                for fact in (item.get("observed_facts") or [])
                if str(fact or "").strip()
            ],
        })
    return blocks


def _section_understanding_support_flags(raw_blocks: List[Dict[str, Any]]) -> Dict[str, bool]:
    text = "\n".join(
        "\n".join([block.get("heading", ""), block.get("observed_text", ""), "\n".join(block.get("observed_facts", []))])
        for block in raw_blocks
    )
    page_types = {block.get("page_type") for block in raw_blocks}
    return {
        "projects": any(pt in {"portfolio", "projects", "case_study", "case-study"} for pt in page_types) and bool(_PROJECT_CONTEXT_RE.search(text)),
        "pricing": any(pt == "pricing" for pt in page_types) or bool(_CURRENCY_RE.search(text)) or bool(re.search(r"\b(?:package|packages|plan|plans)\s*[:\-]\s*\w", text, re.IGNORECASE)),
        "process": bool(_PROCESS_HINT_RE.search(text)),
        "geography": bool(_GEOGRAPHY_CONTEXT_RE.search(text)),
    }


def _add_section_understanding_support_warnings(
    brief: Dict[str, Any],
    section: dict,
    state: dict,
    raw_blocks: List[Dict[str, Any]],
    *,
    support_flags: Optional[Dict[str, bool]] = None,
) -> None:
    heading_blob = _section_understanding_heading_text(section)
    support_flags = support_flags or _section_understanding_support_flags(raw_blocks)

    warnings = brief.setdefault("not_supported_for_this_section", [])
    if _section_heading_mentions_projects(heading_blob) and not brief.get("relevant_projects"):
        warnings.append("Specific brand projects/case studies are not supported by the selected raw brand evidence.")
    if _section_heading_mentions_pricing(heading_blob) and not support_flags.get("pricing"):
        warnings.append("Brand pricing/packages are not supported by explicit raw brand pricing evidence.")
    if _section_heading_mentions_geography(heading_blob) and not brief.get("relevant_geography"):
        heading_text = str((section or {}).get("heading_text") or "").strip()
        suffix = f" Heading: {heading_text}" if heading_text else ""
        warnings.append("Brand geography/market presence is not supported by explicit raw brand geography evidence." + suffix)
    if _section_heading_mentions_process(heading_blob) and not brief.get("relevant_process_steps"):
        warnings.append("Brand process/workflow steps are not supported by selected raw brand evidence.")


def build_section_brand_understanding(section: dict, state: dict, retrieved_chunks: list) -> dict:
    """
    Build a section-specific organizer from raw page-backed blocks/chunks.

    Cards can route selection upstream, but this function treats raw blocks as
    the only content truth for services, projects, process, geography, and CTAs.
    """
    section = section or {}
    state = state or {}
    heading = (section.get("heading_text") or "").strip()
    intent = (section.get("section_intent") or "Informational").strip()

    brief = {
        "section_heading": heading,
        "section_intent": intent,
        "relevant_services": [],
        "relevant_projects": [],
        "relevant_project_families": [],
        "relevant_process_steps": [],
        "relevant_technologies": [],
        "relevant_geography": [],
        "relevant_ctas": [],
        "useful_source_snippets": [],
        "not_supported_for_this_section": [],
        "recommended_angle": {
            "focus_types": [],
            "avoid_types": [],
            "best_evidence_categories": [],
            "preferred_section_style": "general_guidance",
        },
    }

    raw_blocks = _section_understanding_blocks(retrieved_chunks or [])
    if not raw_blocks:
        raw_blocks = select_section_raw_brand_blocks(section, state)

    page_briefs = section.get("section_brand_page_briefs")
    if not isinstance(page_briefs, list) or not page_briefs:
        page_briefs = select_section_brand_page_briefs(section, state)

    if not raw_blocks and not page_briefs:
        brief["recommended_angle"]["focus_types"] = ["general industry criteria", "editorial advice"]
        brief["recommended_angle"]["avoid_types"] = ["unsupported brand claims", "mock statistics"]
        brief["recommended_angle"]["best_evidence_categories"] = ["market_standards"]
        _add_section_understanding_support_warnings(brief, section, state, raw_blocks)
        return brief

    brand_name = (state.get("brand_name") or "").strip()
    brand_aliases = state.get("brand_aliases") or []
    brand_names_set = {brand_name.casefold()} | {str(alias).casefold() for alias in brand_aliases if alias}

    page_summary_texts = [
        str(page.get("grounded_summary") or "")
        for page in page_briefs or []
        if isinstance(page, dict)
    ]
    page_snippets = [
        str(snippet or "")
        for page in page_briefs or []
        if isinstance(page, dict)
        for snippet in (page.get("source_snippets") or [])
    ]
    raw_texts = [block["observed_text"] for block in raw_blocks] + page_summary_texts + page_snippets
    raw_facts = [fact for block in raw_blocks for fact in block.get("observed_facts", [])]
    raw_headings = [block.get("heading", "") for block in raw_blocks] + [
        str(page.get("page_title") or "")
        for page in page_briefs or []
        if isinstance(page, dict)
    ]
    combined_corpus = "\n".join(raw_texts + raw_facts + raw_headings)
    combined_lower = combined_corpus.casefold()

    noisy_exact = {
        "brief", "technologies used", "s 0", "on time", "management", "delivering",
        "why you should choose us", "intosoftware", "top rated agency", "fast turnaround",
        "name", "project name", "client name", "publish date", "published date",
        "publication date", "objective", "objectives", "creation", "created",
        "sector", "audience", "location", "expertise", "view project",
        "details", "project details", "scope of work", "deliverables",
        "technology stack", "technologies used", "quality assurance",
    }

    def is_noisy_label(value: Any) -> bool:
        item = re.sub(r"\s+", " ", str(value or "")).strip(" -:|")
        folded = item.casefold()
        if not item or folded in noisy_exact:
            return True
        if _BRAND_EVIDENCE_DATE_RE.match(item) or _BRAND_EVIDENCE_JUNK_RE.match(item):
            return True
        if _BRAND_EVIDENCE_FRAGMENT_RE.search(item):
            return True
        if folded.startswith(("on time", "delivering ", "management ")):
            return True
        return False

    def normalize_project_candidate(value: Any) -> str:
        item = re.sub(r"\s+", " ", str(value or "")).strip().strip('"').strip("'")
        item = item.strip("«»<>").strip(" :-|")
        if not item:
            return ""
        if item.casefold() in noisy_exact:
            return ""
        item = re.sub(
            r"^(?:name|project\s+name|client\s+name|client|case\s+study|creation|created|objective|objectives)\b\s*[:\-]?\s*",
            "",
            item,
            flags=re.IGNORECASE,
        ).strip(" :-|")
        item = re.sub(r"^(?:project)\s*[:\-]\s*", "", item, flags=re.IGNORECASE).strip(" :-|")
        item = re.sub(
            r"\b(?:location|sector|audience|expertise|technologies used|technology stack|publish date|published date|view project|scope of work|deliverables|quality assurance)\b.*$",
            "",
            item,
            flags=re.IGNORECASE,
        ).strip(" :-|")
        if not item:
            return ""
        folded = item.casefold()
        if folded in noisy_exact:
            return ""
        if re.match(r"^(?:to|for|with|using|by)\s+\w+", item, re.IGNORECASE):
            return ""
        if re.match(r"^(?:in|at|from|inside|within)\s+[A-Za-z\u0600-\u06FF .'-]{2,80}$", item, re.IGNORECASE):
            return ""
        if re.match(r"^(?:\u0641\u064a|\u062f\u0627\u062e\u0644|\u0639\u0628\u0631)\s+[\u0600-\u06FF A-Za-z.'-]{2,80}$", item):
            return ""

        # Merge delivery-channel variants of the same named project. This keeps
        # "Example Web App" and "Example Mob App" from being treated as two
        # separate projects while preserving the observed project name itself.
        base = re.sub(
            r"\s+(?:web\s+app|mob\s+app|mobile\s+app|app|website|web\s+site|platform)$",
            "",
            item,
            flags=re.IGNORECASE,
        ).strip(" :-|")
        if base and len(base.split()) >= 2:
            item = base
        return item

    def clean_project_variant(value: Any) -> str:
        item = re.sub(r"\s+", " ", str(value or "")).strip().strip('"').strip("'").strip("«»<>").strip(" :-|")
        item = re.sub(
            r"^(?:name|project\s+name|client\s+name|client|case\s+study|creation|created|objective|objectives)\b\s*[:\-]?\s*",
            "",
            item,
            flags=re.IGNORECASE,
        ).strip(" :-|")
        item = re.sub(r"^(?:project)\s*[:\-]\s*", "", item, flags=re.IGNORECASE).strip(" :-|")
        item = re.sub(
            r"\b(?:location|sector|audience|expertise|technologies used|technology stack|publish date|published date|view project|scope of work|deliverables|quality assurance)\b.*$",
            "",
            item,
            flags=re.IGNORECASE,
        ).strip(" :-|")
        cleaned = _sanitize_evidence_item(item, "project_explicit")
        return cleaned or ""

    def is_noisy_project_block(block: Dict[str, Any]) -> bool:
        heading_value = re.sub(r"\s+", " ", str(block.get("heading") or "")).strip()
        text_value = re.sub(r"\s+", " ", str(block.get("observed_text") or "")).strip()
        folded = f"{heading_value} {text_value}".casefold()
        explicit_project_signal = bool(re.search(
            r"\b(?:client|project|case study)\s*:",
            text_value,
            re.IGNORECASE,
        ))
        footer_noise_hits = sum(
            1
            for item in [
                "subscribe", "facebook", "instagram", "linkedin", "main menu",
                "all rights reserved", "scroll to top", "let's talk", "lets talk",
                "email subscribe", "twitter", "x-twitter",
            ]
            if item in folded
        )
        if footer_noise_hits >= 3:
            return True
        if heading_value.casefold() in {
            "subscribe newsletters", "subscribe newsletter", "about us", "history",
            "our mission", "our vision", "testimonials", "faqs", "contact us",
            "portfolio", "our portfolio", "our work",
        } and not explicit_project_signal:
            return True
        return is_noisy_label(heading_value) and not explicit_project_signal

    def add_clean(target: List[str], values: List[Any], category: str, limit: int) -> None:
        seen = {item.casefold() for item in target}
        for value in values:
            if is_noisy_label(value):
                continue
            cleaned = _sanitize_evidence_item(value, category=category)
            if not cleaned:
                continue
            key = cleaned.casefold()
            if key in seen:
                continue
            target.append(cleaned)
            seen.add(key)
            if len(target) >= limit:
                break

    tech_keywords = [
        "react", "node.js", "node", "python", "php", "laravel", "wordpress", "mysql", "postgresql",
        "flutter", "swift", "kotlin", "figma", "adobe xd", "adobe", "java", "html", "css", "javascript",
        "aws", "docker", "git", "bootstrap", "tailwind", "erp", "crm", "pos", "api", "dashboard",
    ]
    tech_capitalization = {
        "react": "React", "node.js": "Node.js", "node": "Node.js", "python": "Python", "php": "PHP",
        "laravel": "Laravel", "wordpress": "WordPress", "mysql": "MySQL", "postgresql": "PostgreSQL",
        "flutter": "Flutter", "swift": "Swift", "kotlin": "Kotlin", "figma": "Figma", "adobe xd": "Adobe XD",
        "adobe": "Adobe", "java": "Java", "html": "HTML", "css": "CSS", "javascript": "JavaScript",
        "aws": "AWS", "docker": "Docker", "git": "Git", "bootstrap": "Bootstrap", "tailwind": "Tailwind",
        "erp": "ERP", "crm": "CRM", "pos": "POS", "api": "API", "dashboard": "Dashboard",
    }
    for tech in tech_keywords:
        if re.search(r"\b" + re.escape(tech) + r"\b", combined_lower):
            brief["relevant_technologies"].append(tech_capitalization.get(tech, tech.title()))
    brief["relevant_technologies"] = list(dict.fromkeys(brief["relevant_technologies"]))

    service_candidates: List[str] = []
    for page in page_briefs or []:
        if not isinstance(page, dict):
            continue
        service_candidates.extend(page.get("observed_services") or [])
    service_keywords = [
        "web design", "web development", "web application development", "web applications", "ui/ux design",
        "mobile app development", "e-commerce", "erp systems", "crm integrations", "pos software",
        "hosting", "domain registration", "seo", "marketing", "software development",
    ]
    for service in service_keywords:
        if service in combined_lower:
            service_candidates.append(service.title() if service.isascii() else service)
    for pattern in [
        r"\b(?:we\s+)?(?:provide|offer|deliver|build|develop|specialize in|services include)\s+([^.\n]+)",
    ]:
        for match in re.finditer(pattern, combined_corpus, re.IGNORECASE):
            for part in re.split(r"\s*(?:,|;|\||/| and |&|\+)\s*", match.group(1), flags=re.IGNORECASE):
                item = re.sub(r"\s+", " ", part).strip(" .:-")
                if _SERVICE_HINT_RE.search(item):
                    service_candidates.append(item)
    add_clean(brief["relevant_services"], service_candidates, "service", 14)

    process_candidates: List[str] = []
    for page in page_briefs or []:
        if isinstance(page, dict):
            process_candidates.extend(page.get("observed_process_steps") or [])
    for step in [
        "consultation & planning", "design & development", "execution & delivery",
        "testing", "launch", "planning", "design", "development", "execution", "delivery",
    ]:
        if step in combined_lower:
            process_candidates.append(step.title())
    for match in re.finditer(r"(?:step|phase)\s*\d+[:\-\s]*([A-Z][^.\n]{2,80})", combined_corpus, re.IGNORECASE):
        process_candidates.append(match.group(1).strip())
    add_clean(brief["relevant_process_steps"], process_candidates, "process", 10)

    for page in page_briefs or []:
        if isinstance(page, dict):
            brief["relevant_ctas"].extend(str(item) for item in (page.get("observed_ctas") or []) if str(item).strip())
    for cta in ["quote", "contact", "call", "form", "whatsapp", "phone", "email", "booking"]:
        if cta in combined_lower:
            brief["relevant_ctas"].append(cta.title())
    for match in re.finditer(r"[\w\.-]+@[\w\.-]+\.\w+|\+?\d{3,4}[\s\-]?\d{3,4}[\s\-]?\d{3,4}", combined_corpus):
        brief["relevant_ctas"].append(match.group(0).strip())
    brief["relevant_ctas"] = list(dict.fromkeys(brief["relevant_ctas"]))

    geo_candidates: List[str] = []
    for page in page_briefs or []:
        if isinstance(page, dict):
            geo_candidates.extend(page.get("explicit_geography") or [])
    geo_patterns = [
        r"\b(?:address|based in|located in|office in|branch in|headquarters in|service area|serving|serves)\s*:?\s*([A-Z][A-Za-z\s.'-]{2,70})",
    ]
    for line in re.split(r"(?<=[.!?])\s+|\n", combined_corpus):
        if not _GEOGRAPHY_CONTEXT_RE.search(line):
            continue
        for pattern in geo_patterns:
            for match in re.finditer(pattern, line, re.IGNORECASE):
                value = re.split(r"[.,;|\n]|\s+\band\b\s+", match.group(1).strip(), maxsplit=1, flags=re.IGNORECASE)[0]
                geo_candidates.append(value)
    add_clean(brief["relevant_geography"], geo_candidates, "geography", 8)

    page_project_candidates: List[str] = []
    for page in page_briefs or []:
        if isinstance(page, dict):
            page_project_candidates.extend(page.get("observed_projects") or [])

    project_blocks = []
    for block in raw_blocks:
        if is_noisy_project_block(block):
            continue
        source_path = urlparse(str(block.get("source_url") or "")).path.casefold()
        strong_project_source = (
            block.get("page_type") in {"portfolio", "projects", "case_study", "case-study"}
            or any(segment in source_path for segment in ["/projects", "/project", "/portfolio", "/case"])
        )
        text_blob = "\n".join([block.get("observed_text", ""), "\n".join(block.get("observed_facts", []))])
        if strong_project_source and (
            _PROJECT_CONTEXT_RE.search(text_blob)
            or re.search(r"\b(?:client|project|case study)\s*:", text_blob, re.IGNORECASE)
            or (block.get("heading") and not is_noisy_label(block.get("heading")))
        ):
            project_blocks.append(block)
    project_text = "\n".join(
        "\n".join([block.get("heading", ""), block.get("observed_text", ""), "\n".join(block.get("observed_facts", []))])
        for block in project_blocks
    )
    project_candidates: List[str] = list(page_project_candidates)
    for pattern in [
        r"\b(?:name|client|project|case study)\s*[:\-]\s*([A-Z][A-Za-z0-9&.'\-\s]{2,80}?)(?=(?:[.;\n]\s*(?:name|client|project|case study|sector|audience|expertise|location)\s*[:\-])|[.;\n]|$)",
        r"\b(?:name|project|case study|client|app|website|platform)\s*[:\-]\s*([A-Z][A-Za-z0-9&.'\-\s]{2,80}?)(?=(?:[.;\n]\s*(?:name|project|case study|client|app|website|platform)\s*[:\-])|[.;\n]|$)",
        r"\b(?:name|client|creation)\s+([A-Z][A-Za-z0-9&.'\-\s]{2,80}?)(?=(?:[.;\n]\s*(?:name|client|project|case study|sector|audience|expertise|location|creation|objective))|[.;\n]|$)",
        r"\b(?:mobile app|web app|website|platform)\s+([A-Z][A-Za-z0-9&'\-]*(?:\s+[A-Z][A-Za-z0-9&'\-]*){1,6}(?:\s+(?:Mob App|Mobile App|Web App|Web app|Website|Platform|App))?)\b",
        r"(?:project|app|website|client|platform)\s+(?:\u00c2\u00ab|\u00ab|\u0164)(.*?)(?:\u00c2\u00bb|\u00bb|\u0165)",
    ]:
        for match in re.finditer(pattern, project_text, re.IGNORECASE):
            value = re.split(
                r"[.;\n]|\b(?:we|also|location|sector|audience|expertise|technologies used|view project)\b",
                match.group(1).strip(),
                maxsplit=1,
                flags=re.IGNORECASE,
            )[0]
            project_candidates.append(value)
    for line in [line.strip() for line in project_text.splitlines() if line.strip()]:
        if line.startswith(("-", "*", "1.", "2.", "3.")):
            project_candidates.append(re.sub(r"^[-\s*0-9.]+", "", line).strip())
    if not project_candidates:
        for source_line in project_text.splitlines():
            project_candidates.extend(
                re.findall(r"\b[A-Z][a-zA-Z0-9]*(?:\s+[A-Z][a-zA-Z0-9]*){1,3}\b", source_line)
            )
    for block in project_blocks:
        if block.get("page_type") in {"projects", "portfolio", "case_study", "case-study"}:
            heading_value = str(block.get("heading") or "").strip()
            if heading_value and heading_value.casefold() not in {"projects", "portfolio", "our work"}:
                project_candidates.append(heading_value)

    seen_projects = set()
    expanded_project_candidates: List[str] = []
    project_family_variants: Dict[str, set] = {}
    for candidate in project_candidates:
        expanded_project_candidates.append(candidate)
        suffix_match = re.match(
            r"^(.+?)\s+(?:integration|implementation|development|migration|redesign)$",
            str(candidate or "").strip(),
            re.IGNORECASE,
        )
        if suffix_match and len(suffix_match.group(1).split()) >= 2:
            expanded_project_candidates.append(suffix_match.group(1).strip())

    for candidate in expanded_project_candidates:
        raw_candidate = str(candidate or "").strip()
        variant = clean_project_variant(raw_candidate)
        candidate = normalize_project_candidate(candidate)
        item = str(candidate or "").strip().strip('"').strip("'").strip("Â«").strip("Â»")
        item = re.sub(
            r"\b(?:location|sector|audience|expertise|technologies used|view project)\b.*$",
            "",
            item,
            flags=re.IGNORECASE,
        ).strip(" :-")
        item = re.sub(
            r"^(?:(?:all|mobile app|web app|website|websites|platform|design services|seo|portfolio)\s+)+",
            "",
            item,
            flags=re.IGNORECASE,
        ).strip(" :-")
        item = normalize_project_candidate(item)
        if not item or is_noisy_label(item):
            continue
        folded = item.casefold()
        if folded in {"mobile app", "web app", "website", "websites", "platform", "portfolio"}:
            continue
        if any(brand and brand in folded for brand in brand_names_set):
            continue
        if any(noise in folded for noise in ["terms of", "privacy policy", "contact us", "about us", "home page"]):
            continue
        cleaned = _sanitize_evidence_item(item, "project_explicit")
        if not cleaned or cleaned.casefold() in seen_projects:
            if cleaned and variant:
                project_family_variants.setdefault(cleaned.casefold(), set()).add(variant)
            continue
        cleaned_key = cleaned.casefold()
        partial_suffixes = (" app", " web app", " mob app", " mobile app", " website", " platform")
        if any(
            existing.startswith(cleaned_key) and existing[len(cleaned_key):] in partial_suffixes
            for existing in seen_projects
        ):
            if variant:
                for existing in seen_projects:
                    if existing.startswith(cleaned_key) and existing[len(cleaned_key):] in partial_suffixes:
                        project_family_variants.setdefault(existing, set()).add(variant)
            continue
        seen_projects.add(cleaned_key)
        project_family_variants.setdefault(cleaned_key, set()).add(cleaned)
        if variant:
            project_family_variants.setdefault(cleaned_key, set()).add(variant)
        brief["relevant_projects"].append(cleaned)
        if len(brief["relevant_projects"]) >= 24:
            break

    def project_context_extension(project: str, other: str) -> bool:
        project_key = project.casefold()
        other_key = other.casefold()
        if project_key == other_key or not project_key.startswith(other_key + " "):
            return False
        extra = project_key[len(other_key):].strip()
        if not extra:
            return False
        channel_suffixes = {
            "app", "web app", "mob app", "mobile app", "website", "platform",
            "integration", "implementation", "development", "migration", "redesign",
        }
        if extra in channel_suffixes:
            return False
        return len(extra.split()) <= 5

    if len(brief["relevant_projects"]) > 1:
        filtered_projects: List[str] = []
        for project in brief["relevant_projects"]:
            if any(
                project_context_extension(project, other)
                for other in brief["relevant_projects"]
                if other != project and len(other.split()) >= 2
            ):
                continue
            filtered_projects.append(project)
        brief["relevant_projects"] = filtered_projects

    area_terms = [
        str(state.get("area") or "").strip(),
        str(state.get("target_area") or "").strip(),
        str(state.get("primary_keyword") or "").strip(),
    ]
    area_terms.extend(str(item).strip() for item in brief.get("relevant_geography", []) if str(item).strip())
    area_terms = [term for term in area_terms if term]

    def project_country_relevance_score(project: str) -> int:
        folded_project = _fulfillment_text(project)
        score = 0
        for term in area_terms:
            folded_term = _fulfillment_text(term)
            if not folded_term or len(folded_term) < 3:
                continue
            if folded_term in folded_project:
                score += 80
        for block in project_blocks:
            block_blob = "\n".join([
                str(block.get("heading") or ""),
                str(block.get("observed_text") or ""),
                "\n".join(str(fact) for fact in block.get("observed_facts", []) or []),
            ])
            folded_blob = _fulfillment_text(block_blob)
            if folded_project and folded_project not in folded_blob:
                continue
            for term in area_terms:
                folded_term = _fulfillment_text(term)
                if folded_term and len(folded_term) >= 3 and folded_term in folded_blob:
                    score += 40
        return score

    original_project_order = {project.casefold(): idx for idx, project in enumerate(brief["relevant_projects"])}
    brief["relevant_projects"] = sorted(
        brief["relevant_projects"],
        key=lambda project: (-project_country_relevance_score(project), original_project_order.get(project.casefold(), 999)),
    )[:8]
    brief["relevant_project_families"] = [
        {
            "name": project,
            "variants": [
                variant for variant in sorted(project_family_variants.get(project.casefold(), set()))
                if variant and variant.casefold() != project.casefold()
            ][:6],
            "target_area_relevance": "explicit" if project_country_relevance_score(project) > 0 else "general",
        }
        for project in brief["relevant_projects"]
    ]

    snippets: List[str] = []
    for page in page_briefs or []:
        if not isinstance(page, dict):
            continue
        summary = str(page.get("grounded_summary") or "").strip()
        if summary:
            snippets.append(summary)
        for snippet in page.get("source_snippets") or []:
            if str(snippet or "").strip():
                snippets.append(str(snippet).strip())
    for raw in raw_texts:
        for sentence in re.split(r"[.!?\n]+", raw):
            item = sentence.strip()
            if 20 <= len(item) <= 250:
                has_brand = any(brand and brand in item.casefold() for brand in brand_names_set)
                has_evidence = any(pattern.search(item) for pattern in [_SERVICE_HINT_RE, _PROJECT_CONTEXT_RE, _PROCESS_HINT_RE])
                if has_brand or has_evidence:
                    snippet = _sanitize_evidence_item(item, "snippet", allow_promotional=True)
                    if snippet:
                        snippets.append(snippet)
    brief["useful_source_snippets"] = list(dict.fromkeys(snippets))[:4]

    support_flags = _section_understanding_support_flags(raw_blocks)
    if page_briefs:
        support_flags = dict(support_flags)
        support_flags["projects"] = support_flags.get("projects") or any(
            bool(page.get("observed_projects")) for page in page_briefs if isinstance(page, dict)
        )
        support_flags["pricing"] = support_flags.get("pricing") or any(
            bool(page.get("observed_pricing")) for page in page_briefs if isinstance(page, dict)
        )
        support_flags["process"] = support_flags.get("process") or any(
            bool(page.get("observed_process_steps")) for page in page_briefs if isinstance(page, dict)
        )
        support_flags["geography"] = support_flags.get("geography") or any(
            bool(page.get("explicit_geography")) for page in page_briefs if isinstance(page, dict)
        )
    _add_section_understanding_support_warnings(brief, section, state, raw_blocks, support_flags=support_flags)

    heading_blob = _section_understanding_heading_text(section)
    is_project_heading = _section_heading_mentions_projects(heading_blob)
    is_process_heading = _section_heading_mentions_process(heading_blob)
    is_tech_heading = any(term in heading_blob for term in ["technology", "technologies", "tools", "stack", "platforms"])
    is_why_heading = any(term in heading_blob for term in ["why choose", "why trust", "advantages", "differentiators"])

    has_rich_evidence = (
        len(brief["relevant_services"]) >= 2
        or len(brief["relevant_projects"]) >= 1
        or len(brief["relevant_process_steps"]) >= 2
        or len(brief["relevant_technologies"]) >= 2
    )
    if has_rich_evidence:
        brief["recommended_angle"]["preferred_section_style"] = "evidence_grounded"
        if is_project_heading:
            brief["recommended_angle"]["focus_types"] = ["specific observed projects", "raw source snippets"]
        elif is_tech_heading:
            brief["recommended_angle"]["focus_types"] = ["observed tools and platforms", "specific technical workflows"]
        elif is_process_heading:
            brief["recommended_angle"]["focus_types"] = ["actual workflow stages", "delivery milestones"]
        elif is_why_heading:
            brief["recommended_angle"]["focus_types"] = ["observed service differentiators", "concrete process milestones"]
        else:
            brief["recommended_angle"]["focus_types"] = ["observed services", "verified technical capabilities"]
        brief["recommended_angle"]["avoid_types"] = ["generic praise", "unsupported location claims", "abstract marketing prose"]
        cats = []
        if brief["relevant_projects"]:
            cats.append("portfolio")
        if brief["relevant_services"]:
            cats.append("services")
        if brief["relevant_technologies"]:
            cats.append("technologies")
        if brief["relevant_process_steps"]:
            cats.append("process_stages")
        brief["recommended_angle"]["best_evidence_categories"] = cats
    else:
        brief["recommended_angle"]["focus_types"] = ["general industry criteria", "editorial advice"]
        brief["recommended_angle"]["avoid_types"] = ["mock statistics", "unsupported brand assertions"]
        brief["recommended_angle"]["best_evidence_categories"] = ["market_standards"]

    brief["not_supported_for_this_section"] = list(dict.fromkeys(brief["not_supported_for_this_section"]))
    return brief


def _fulfillment_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def _fulfillment_value_mentioned(content_text: str, value: Any) -> bool:
    candidate = _fulfillment_text(value)
    if not candidate:
        return False
    if candidate in content_text:
        return True

    tokens = [
        token
        for token in re.findall(r"[\w\u0600-\u06FF]+", candidate, flags=re.UNICODE)
        if len(token) > 2
    ]
    if len(tokens) >= 3:
        matches = sum(1 for token in tokens if token in content_text)
        return matches >= max(2, len(tokens) - 1)
    return False


def _fulfillment_any_mentioned(content_text: str, values: List[Any]) -> List[str]:
    matched: List[str] = []
    for value in values or []:
        if _fulfillment_value_mentioned(content_text, value):
            cleaned = re.sub(r"\s+", " ", str(value or "")).strip()
            if cleaned:
                matched.append(cleaned)
    return list(dict.fromkeys(matched))


def _fulfillment_paragraphs(content: str) -> List[str]:
    """Extract prose/list paragraphs while ignoring headings and markdown tables."""
    chunks = re.split(r"\n\s*\n", str(content or "").strip())
    paragraphs: List[str] = []
    for chunk in chunks:
        lines = []
        for line in chunk.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("|") and stripped.endswith("|"):
                continue
            lines.append(stripped)
        text = re.sub(r"\s+", " ", " ".join(lines)).strip()
        if len(text) >= 35:
            paragraphs.append(text)
    return paragraphs


def _evidence_density_report(content: str, anchors: List[Any]) -> Dict[str, Any]:
    """Measure whether brand-owned prose is led by observed evidence anchors."""
    cleaned_anchors: List[str] = []
    seen = set()
    for anchor in anchors or []:
        text = re.sub(r"\s+", " ", str(anchor or "")).strip()
        if not text or len(text) < 3:
            continue
        folded = text.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        cleaned_anchors.append(text)

    paragraphs = _fulfillment_paragraphs(content)
    anchored_indices: List[int] = []
    missing_indices: List[int] = []
    matched_by_paragraph: List[List[str]] = []
    for idx, paragraph in enumerate(paragraphs, start=1):
        folded_paragraph = _fulfillment_text(paragraph)
        matched = _fulfillment_any_mentioned(folded_paragraph, cleaned_anchors)
        matched_by_paragraph.append(matched)
        if matched:
            anchored_indices.append(idx)
        else:
            missing_indices.append(idx)

    total = len(paragraphs)
    anchored = len(anchored_indices)
    ratio = round(anchored / total, 3) if total else 1.0
    return {
        "total_paragraphs": total,
        "anchored_paragraphs": anchored,
        "anchor_ratio": ratio,
        "missing_paragraph_indices": missing_indices,
        "anchors_available": cleaned_anchors[:18],
        "matched_by_paragraph": matched_by_paragraph,
    }


_GENERIC_ADVICE_DRIFT_RE = re.compile(
    r"\b(?:choose|compare|ask|check|ensure|make sure|criteria|evaluate|if your priority|best option|suitable for)\b|"
    r"(?:\u0643\u064a\u0641\s+\u062a\u062e\u062a\u0627\u0631|\u0627\u062e\u062a\u064a\u0627\u0631|\u0645\u0639\u0627\u064a\u064a\u0631|\u0642\u0627\u0631\u0646|\u062a\u0623\u0643\u062f|\u0627\u0633\u0623\u0644|\u0625\u0630\u0627\s+\u0643\u0627\u0646|\u064a\u0646\u0627\u0633\u0628|\u0627\u0644\u062e\u064a\u0627\u0631\s+\u0627\u0644\u0623\u0646\u0633\u0628)",
    re.IGNORECASE,
)


def _heading_drift_report(content: str, axis: str, matched_evidence: List[str]) -> Dict[str, Any]:
    """Detect when a brand-owned section drifts into generic buyer advice."""
    paragraphs = _fulfillment_paragraphs(content)
    advice_hits = sum(len(_GENERIC_ADVICE_DRIFT_RE.findall(paragraph)) for paragraph in paragraphs)
    matched_count = len(matched_evidence or [])
    drift = False
    if axis in {"brand_offer", "brand_features", "brand_support"}:
        drift = advice_hits >= 3 and matched_count <= 1
    elif axis == "brand_projects":
        drift = advice_hits >= 2 and matched_count <= 1
    return {
        "generic_advice_hits": advice_hits,
        "matched_evidence_count": matched_count,
        "drift_detected": drift,
    }


def _legacy_section_understanding_from_cards(state: Dict[str, Any]) -> Dict[str, Any]:
    """Compatibility fallback for callers not yet passing raw section blocks."""
    brief = {
        "relevant_services": [],
        "relevant_projects": [],
        "relevant_project_families": [],
        "relevant_process_steps": [],
        "relevant_technologies": [],
        "relevant_geography": [],
        "relevant_ctas": [],
        "not_supported_for_this_section": [],
        "recommended_angle": {"preferred_section_style": "general_guidance"},
    }
    key_map = {
        "relevant_services": ["visible_products_or_services", "visible_features_or_capabilities"],
        "relevant_projects": ["visible_project_or_case_study_examples"],
        "relevant_process_steps": ["visible_process_steps"],
        "relevant_geography": ["visible_geography"],
    }
    for card in (state or {}).get("brand_evidence_cards", []) or []:
        if not isinstance(card, dict) or card.get("excluded_reason"):
            continue
        for target, keys in key_map.items():
            for key in keys:
                brief[target].extend(str(item).strip() for item in card.get(key, []) if str(item).strip())
    for key in key_map:
        brief[key] = list(dict.fromkeys(brief[key]))
    return brief


def _legacy_raw_blocks_from_cards(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for card in (state or {}).get("brand_evidence_cards", []) or []:
        if not isinstance(card, dict) or card.get("excluded_reason"):
            continue
        values: List[str] = []
        for key in [
            "visible_products_or_services",
            "visible_features_or_capabilities",
            "visible_project_or_case_study_examples",
            "visible_process_steps",
            "visible_pricing_or_packages",
            "visible_geography",
            "visible_trust_signals",
        ]:
            values.extend(str(item).strip() for item in card.get(key, []) if str(item).strip())
        if not values:
            continue
        blocks.append({
            "source_url": str(card.get("url") or ""),
            "page_type": str(card.get("page_type") or ""),
            "heading": str(card.get("heading") or ""),
            "observed_text": ". ".join(values),
            "observed_facts": values,
        })
    return blocks


def evaluate_brand_section_fulfillment(
    section: Dict[str, Any],
    content: str,
    section_brand_understanding: Optional[Dict[str, Any]] = None,
    section_raw_brand_blocks: Optional[List[Dict[str, Any]]] = None,
    state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Deterministically verify that a brand-owned section fulfills the heading
    using raw page-backed evidence.

    The function is intentionally pure: it never mutates section, state, raw
    blocks, or the understanding brief.
    """
    section = section or {}
    state = state or {}
    contract = section.get("section_contract") or {}
    brand_policy = str(contract.get("brand_policy") or section.get("brand_policy") or "").casefold()
    axis = str(
        section.get("taxonomy_axis")
        or contract.get("taxonomy_axis")
        or ""
    ).casefold()
    content_type = str(state.get("content_type") or "").casefold()
    heading_blob = _section_understanding_heading_text(section)

    strict_brand_axis = axis in {"brand_offer", "brand_projects", "brand_process", "brand_pricing"}
    promised_pricing = _section_heading_mentions_pricing(heading_blob) or axis in {"pricing", "brand_pricing"}
    promised_projects = _section_heading_mentions_projects(heading_blob) or axis == "brand_projects"
    promised_process = _section_heading_mentions_process(heading_blob) or axis == "brand_process"
    promised_geo = _section_heading_mentions_geography(heading_blob)
    is_brand_owned = (
        brand_policy == "commercial"
        or strict_brand_axis
        or bool(section.get("_visible_brand_reference"))
        or (content_type == "brand_commercial" and strict_brand_axis)
    )
    if not is_brand_owned:
        return {
            "fulfillment_status": "satisfied",
            "fulfillment_reason": "not brand-owned",
            "matched_evidence": [],
        }

    brief = dict(section_brand_understanding or section.get("section_brand_understanding") or {})
    raw_blocks = _section_understanding_blocks(
        section_raw_brand_blocks
        or section.get("section_raw_brand_blocks")
        or []
    )
    if not brief and not raw_blocks:
        brief = {}
        raw_blocks = []

    raw_text = "\n".join(
        "\n".join([
            block.get("heading", ""),
            block.get("observed_text", ""),
            "\n".join(block.get("observed_facts", [])),
        ])
        for block in raw_blocks
    )
    support_flags = _section_understanding_support_flags(raw_blocks)
    content_text = _fulfillment_text(content)
    heading_text_raw = str(section.get("heading_text") or "")
    heading_text = _fulfillment_text(heading_text_raw)
    combined_written_text = f"{heading_text}\n{content_text}"
    brand_name = str(state.get("brand_name") or section.get("brand_name") or "").strip()
    aliases = state.get("brand_aliases") or []
    if isinstance(aliases, str):
        aliases = [aliases]
    brand_terms = [term.casefold() for term in [brand_name] + aliases if str(term).strip()]
    heading_mentions_brand = bool(
        brand_terms and any(term in heading_text_raw.casefold() for term in brand_terms)
    )

    services = list(dict.fromkeys(
        [str(item).strip() for item in (brief.get("relevant_services") or []) if str(item).strip()]
        + [str(item).strip() for item in (brief.get("relevant_technologies") or []) if str(item).strip()]
    ))
    projects = [str(item).strip() for item in (brief.get("relevant_projects") or []) if str(item).strip()]
    process_steps = [str(item).strip() for item in (brief.get("relevant_process_steps") or []) if str(item).strip()]
    geography = [str(item).strip() for item in (brief.get("relevant_geography") or []) if str(item).strip()]
    ctas = [str(item).strip() for item in (brief.get("relevant_ctas") or []) if str(item).strip()]

    evidence_anchors = list(dict.fromkeys(services + projects + process_steps + geography + ctas))
    density_report = _evidence_density_report(content or "", evidence_anchors)
    section_type = str(section.get("section_type") or "").casefold()

    def attach_reports(report: Dict[str, Any], matched: Optional[List[str]] = None) -> Dict[str, Any]:
        matched_values = matched if matched is not None else report.get("matched_evidence", [])
        report["evidence_density"] = density_report
        report["heading_fidelity"] = _heading_drift_report(content or "", axis, matched_values or [])
        return report

    def evidence_density_issue(matched: List[str]) -> Optional[Dict[str, Any]]:
        if section_type in {"introduction", "intro", "conclusion", "faq"}:
            return None
        if not evidence_anchors or density_report.get("total_paragraphs", 0) < 2:
            return None
        ratio = float(density_report.get("anchor_ratio", 1.0))
        if ratio >= 0.75:
            return None
        status = "unsupported" if density_report.get("anchored_paragraphs", 0) == 0 else "weak"
        return {
            "fulfillment_status": status,
            "fulfillment_reason": "brand evidence density below threshold; section contains generic prose not anchored to observed brand evidence",
            "matched_evidence": matched,
        }

    def heading_drift_issue(matched: List[str]) -> Optional[Dict[str, Any]]:
        drift = _heading_drift_report(content or "", axis, matched)
        if not drift.get("drift_detected"):
            return None
        return {
            "fulfillment_status": "weak",
            "fulfillment_reason": "heading drift detected; brand-owned section reads like generic buyer advice instead of answering the heading with observed brand evidence",
            "matched_evidence": matched,
        }

    raw_pricing_supported = bool(support_flags.get("pricing"))
    raw_geo_supported = bool(geography) or bool(support_flags.get("geography"))
    raw_trust_supported = bool(_TRUST_CONTEXT_RE.search(raw_text))
    raw_timeline_supported = bool(re.search(
        r"\b(?:within|in\s+\d+\s+(?:hours?|days?|weeks?)|response\s+time|same[-\s]?day|24/7)\b|"
        r"(?:\u062e\u0644\u0627\u0644\s+\d+\s+(?:\u0633\u0627\u0639\u0629|\u0623\u064a\u0627\u0645|\u0627\u064a\u0627\u0645|\u0623\u0633\u0627\u0628\u064a\u0639)|"
        r"\u0648\u0642\u062a\s+\u0627\u0644\u0627\u0633\u062a\u062c\u0627\u0628\u0629|\u062e\u062f\u0645\u0629\s+24/7)",
        raw_text,
        re.IGNORECASE,
    ))

    pricing_claim_re = re.compile(
        r"\b(?:pricing|price|prices|package|packages|plan|plans|cost|fee|fees|starts? from|starting at)\b|"
        r"(?:\u0633\u0639\u0631|\u0623\u0633\u0639\u0627\u0631|\u0627\u0633\u0639\u0627\u0631|\u062a\u0643\u0644\u0641\u0629|\u0628\u0627\u0642\u0627\u062a|\u0628\u0627\u0642\u0629|\u062e\u0637\u0629|\u0631\u0633\u0648\u0645|\u062a\u0628\u062f\u0623\s+\u0645\u0646)",
        re.IGNORECASE,
    )
    geography_claim_re = re.compile(
        r"\b(?:based in|located in|office in|branch in|serves|serving|available in|across|within)\s+[A-Z][A-Za-z .'-]{2,80}\b|"
        r"(?:\u062f\u0627\u062e\u0644|\u0639\u0628\u0631)\s+[\u0600-\u06FFA-Za-z .'-]{2,80}",
        re.IGNORECASE,
    )
    trust_claim_re = re.compile(
        r"\b(?:trusted|top|best|strongest|leading|certified|certification|licensed|award|awards|partner|partnership|testimonial|review|rating|guarantee|guaranteed)\b|"
        r"(?:\u0645\u0648\u062b\u0648\u0642|\u0627\u0644\u0623\u0641\u0636\u0644|\u0627\u0641\u0636\u0644|\u0627\u0644\u0623\u0642\u0648\u0649|\u0627\u0644\u0623\u0639\u0644\u0649|\u0631\u0627\u0626\u062f|\u0645\u0639\u062a\u0645\u062f|\u0645\u0631\u062e\u0635|\u062c\u0627\u0626\u0632\u0629|\u0634\u0631\u064a\u0643|\u0634\u0631\u0627\u0643\u0629|\u0636\u0645\u0627\u0646|\u064a\u0636\u0645\u0646)",
        re.IGNORECASE,
    )
    timeline_claim_re = re.compile(
        r"\b(?:within|in\s+\d+\s+(?:hours?|days?|weeks?)|response\s+time|same[-\s]?day|24/7|fast turnaround)\b|"
        r"(?:\u062e\u0644\u0627\u0644\s+\d+\s+(?:\u0633\u0627\u0639\u0629|\u0623\u064a\u0627\u0645|\u0627\u064a\u0627\u0645|\u0623\u0633\u0627\u0628\u064a\u0639)|\u0633\u0631\u0639\u0629\s+\u0627\u0644\u0627\u0633\u062a\u062c\u0627\u0628\u0629|\u0648\u0642\u062a\s+\u0627\u0644\u0627\u0633\u062a\u062c\u0627\u0628\u0629)",
        re.IGNORECASE,
    )

    area = str(state.get("area") or "").strip()
    area_claimed = bool(area and _fulfillment_text(area) in content_text)
    promised_geo_claim = promised_geo and (
        heading_mentions_brand
        or axis in {"brand_projects", "brand_pricing"}
        or bool(section.get("_visible_brand_reference"))
    )
    unsupported_claims: List[str] = []
    if (promised_pricing or pricing_claim_re.search(combined_written_text)) and not raw_pricing_supported:
        unsupported_claims.append("brand pricing/packages promised without explicit raw brand pricing evidence")
    if (promised_geo_claim or area_claimed or geography_claim_re.search(content_text)) and not raw_geo_supported:
        unsupported_claims.append("brand geography/market presence promised without explicit raw brand geography evidence")
    if trust_claim_re.search(content_text) and not raw_trust_supported:
        unsupported_claims.append("brand trust/certification/leadership claim lacks explicit raw evidence")
    if timeline_claim_re.search(content_text) and not raw_timeline_supported:
        unsupported_claims.append("brand timeline/response-time claim lacks explicit raw evidence")
    if unsupported_claims:
        return attach_reports({
            "fulfillment_status": "unsupported",
            "fulfillment_reason": "; ".join(dict.fromkeys(unsupported_claims)),
            "matched_evidence": [],
        })

    if promised_projects:
        matched_projects = _fulfillment_any_mentioned(content_text, projects)
        if projects and matched_projects:
            density_issue = evidence_density_issue(matched_projects)
            if density_issue:
                return attach_reports(density_issue, matched_projects)
            drift_issue = heading_drift_issue(matched_projects)
            if drift_issue:
                return attach_reports(drift_issue, matched_projects)
            return attach_reports({
                "fulfillment_status": "satisfied",
                "fulfillment_reason": "observed project evidence used",
                "matched_evidence": matched_projects,
            }, matched_projects)
        return attach_reports({
            "fulfillment_status": "unsupported" if projects else "weak",
            "fulfillment_reason": "project section did not surface observed project names",
            "matched_evidence": [],
        })

    if axis == "brand_offer":
        matched_services = _fulfillment_any_mentioned(content_text, services)
        if services and matched_services:
            density_issue = evidence_density_issue(matched_services)
            if density_issue:
                return attach_reports(density_issue, matched_services)
            drift_issue = heading_drift_issue(matched_services)
            if drift_issue:
                return attach_reports(drift_issue, matched_services)
            return attach_reports({
                "fulfillment_status": "satisfied",
                "fulfillment_reason": "observed service/capability evidence used",
                "matched_evidence": matched_services,
            }, matched_services)
        return attach_reports({
            "fulfillment_status": "unsupported" if services else "weak",
            "fulfillment_reason": "service section lacks observed service/capability evidence",
            "matched_evidence": [],
        })

    if promised_process:
        matched_steps = _fulfillment_any_mentioned(content_text, process_steps)
        if process_steps and matched_steps:
            density_issue = evidence_density_issue(matched_steps)
            if density_issue:
                return attach_reports(density_issue, matched_steps)
            return attach_reports({
                "fulfillment_status": "satisfied",
                "fulfillment_reason": "observed process evidence used",
                "matched_evidence": matched_steps,
            }, matched_steps)
        return attach_reports({
            "fulfillment_status": "unsupported" if process_steps else "weak",
            "fulfillment_reason": "process section lacks observed process evidence",
            "matched_evidence": [],
        })

    density_issue = evidence_density_issue([])
    if density_issue and axis.startswith("brand_"):
        return attach_reports(density_issue, [])

    return attach_reports({
        "fulfillment_status": "satisfied",
        "fulfillment_reason": "no strict brand-owned promise detected",
        "matched_evidence": [],
    })
