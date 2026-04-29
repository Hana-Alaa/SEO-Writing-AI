import asyncio
import json
import os
import sys

from jinja2 import Environment, FileSystemLoader, StrictUndefined

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.services.strategy_service import StrategyService
from src.services.research_service import ResearchService
from src.services.workflow_controller import AsyncWorkflowController
from src.services.validation_service import ValidationService
from src.services.workflow_controller import StructureError


EXPECTED_KEYS = {
    "primary_angle",
    "market_angle",
    "target_reader_state",
    "pain_point_focus",
    "emotional_trigger",
    "depth_level",
    "authority_strategy",
    "eeat_signals_to_include",
    "differentiation_focus",
    "conversion_strategy",
    "cta_philosophy",
    "local_strategy",
    "cultural_peer_areas",
    "tone_direction",
    "section_role_map",
}

LOCKED_TARGET_READER_STATE = (
    "A buyer with little or no prior market knowledge who needs simple, practical "
    "guidance to understand the available options, compare them confidently, and "
    "take a clear next step without feeling overwhelmed."
)

LOCKED_TONE_DIRECTION = (
    "Clear, confident, beginner-friendly, practical, and persuasive without pressure."
)

LOCKED_CTA_PHILOSOPHY = (
    "Earn action through clarity and trust. A very soft CTA may appear at the end "
    "of the introduction only if the section has already delivered clear value. "
    "Reserve the main CTA for the conclusion."
)

LOCKED_SECTION_ROLE_MAP = {
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

FORBIDDEN_STRATEGY_PHRASES = [
    "performance-first execution",
    "comparing providers",
    "fear of losing leads",
    "business outcomes",
    "implementation path",
    "delivery model",
    "roi",
    "investment return",
    "legal verification",
    "provider selection",
    "broad market opportunity",
]

EXPECTED_BRAND_CONVERSION_MARKERS = [
    "offer",
    "features",
    "proof",
    "compare",
    "buying",
    "objection",
    "cta",
]


class DummyAIClient:
    def __init__(self, payloads):
        self._payloads = list(payloads)

    async def send(self, prompt, step=""):
        if self._payloads:
            payload = self._payloads.pop(0)
        else:
            payload = {}
        content = json.dumps(payload, ensure_ascii=False)
        return {
            "content": content,
            "metadata": {
                "prompt": prompt,
                "response": content,
                "tokens": {},
                "duration": 0,
                "model": "test-double",
            },
        }


class DummyTitleGenerator:
    async def generate(self, **kwargs):
        return {
            "intent": "commercial",
            "optimized_title": kwargs.get("raw_title", "Untitled"),
            "metadata": {},
        }


class DummyOutlineGenerator:
    def __init__(self, outline_payload):
        self.outline_payload = outline_payload

    async def generate(self, **kwargs):
        return {
            "outline": self.outline_payload,
            "metadata": {
                "prompt": "test-prompt",
                "response": json.dumps({"outline": self.outline_payload}, ensure_ascii=False),
                "tokens": {},
                "model": "test-double",
            },
        }

    def _normalize_section(self, sec, idx, content_type, content_strategy, area):
        return sec


def _make_service(ai_payloads=None):
    env = Environment(
        loader=FileSystemLoader("assets/prompts/templates"),
        undefined=StrictUndefined,
    )
    return StrategyService(
        ai_client=DummyAIClient(ai_payloads or []),
        title_generator=DummyTitleGenerator(),
        jinja_env=env,
    )


def _thin_observed_seo():
    return {
        "market_analysis": {
            "intent_analysis": {
                "confirmed_intent": "Commercial / Transactional",
                "intent_confidence_score": 0.9,
            },
            "structural_intelligence": {
                "dominant_page_type": "listing",
                "pricing_presence_ratio": 0.0,
                "faq_presence_ratio": 0.0,
            },
            "market_insights": {
                "keyword_clusters": [
                    {
                        "cluster_name": "Location Specifics",
                        "keywords": ["شقق للبيع فى القاهرة الجديدة"],
                    }
                ],
                "content_gaps": [],
                "brand_advantages": [],
                "mandatory_serp_topics": [],
                "topic_observations": {
                    "core_recurring_topics": [
                        {
                            "topic": "شقق للبيع فى القاهرة الجديدة",
                            "frequency": 3,
                            "confidence": "high",
                        }
                    ],
                    "secondary_mentions": [],
                    "weak_signals": [],
                },
            },
            "semantic_assets": {
                "lsi_keywords": [],
                "related_searches": [],
            },
        }
    }


def _brand_state(ai_payload):
    return {
        "primary_keyword": "شقق للبيع فى القاهرة الجديدة",
        "intent": "commercial",
        "content_type": "brand_commercial",
        "area": "القاهرة الجديدة",
        "brand_name": "عقار يا مصر",
        "prohibited_competitors": [],
        "seo_intelligence": _thin_observed_seo(),
        "workflow_logger": None,
        "content_strategy": {},
        "last_step_prompt": "",
        "last_step_response": "",
        "last_step_tokens": {},
        "last_step_model": "",
        "_ai_payload": ai_payload,
    }


def _flatten_text(value):
    if isinstance(value, dict):
        return " ".join(_flatten_text(v) for v in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(v) for v in value)
    return str(value)


def _assert_no_forbidden_phrases(text):
    lowered = text.lower()
    for phrase in FORBIDDEN_STRATEGY_PHRASES:
        assert phrase not in lowered, f"Forbidden phrase survived strategy output: {phrase}"


def _assert_exact_contract_fields(strategy):
    assert set(strategy.keys()) == EXPECTED_KEYS, "content_strategy JSON shape changed"
    assert (
        strategy["target_reader_state"] == LOCKED_TARGET_READER_STATE
    ), "target_reader_state does not match the locked beginner-friendly contract"
    assert (
        strategy["tone_direction"] == LOCKED_TONE_DIRECTION
    ), "tone_direction does not match the locked contract"
    assert (
        strategy["cta_philosophy"] == LOCKED_CTA_PHILOSOPHY
    ), "cta_philosophy does not match the locked contract"
    assert (
        strategy["section_role_map"] == LOCKED_SECTION_ROLE_MAP
    ), "section_role_map does not match the locked commercial contract"
    assert strategy["depth_level"] == "comprehensive", "depth_level should remain comprehensive in v1"
    assert strategy["cultural_peer_areas"] == [], "cultural_peer_areas should remain an empty list in v1"


def _assert_market_angle_contract(strategy):
    market_angle = strategy["market_angle"].lower()
    assert "القاهرة الجديدة" in strategy["market_angle"], "market_angle must preserve the area"
    assert "market" not in market_angle, "market_angle drifted into broad market wording"
    assert "real estate" not in market_angle, "market_angle drifted into real-estate broadening"
    assert "project" not in market_angle, "market_angle drifted into projects broadening"
    assert "unit type" not in market_angle, "market_angle leaked real-estate unit wording"
    assert "location fit" not in market_angle, "market_angle leaked real-estate location-fit wording"
    assert "buying path" not in market_angle, "market_angle should not use property-specific buying-path wording"


def _assert_primary_angle_contract(strategy):
    angle = strategy["primary_angle"].lower()
    assert "شقق" in strategy["primary_angle"], "primary_angle must preserve the main entity"
    assert "القاهرة الجديدة" in strategy["primary_angle"], "primary_angle must preserve the area"
    assert any(
        token in angle for token in ["decide", "compare", "choose", "buy"]
    ), "primary_angle must stay buyer-decision-focused"
    _assert_no_forbidden_phrases(strategy["primary_angle"])


def _assert_conversion_strategy_contract(strategy):
    value = strategy["conversion_strategy"].lower()
    for marker in EXPECTED_BRAND_CONVERSION_MARKERS:
        assert marker in value, f"Missing bounded conversion marker: {marker}"


def _assert_proof_and_process_contract(strategy):
    proof = strategy["section_role_map"]["proof"].lower()
    process = strategy["section_role_map"]["process_or_how"].lower()

    assert (
        "unit level" in proof or "listing level" in proof
    ), "proof must stay tied to the product at unit/listing level"
    assert "abstract market conditions" in proof, "proof contract must explicitly forbid market abstraction"
    assert "investment framing" in proof, "proof contract must explicitly forbid investment framing"
    assert "buying journey" in process, "process_or_how must stay buying-journey-oriented"
    assert "legal" in process, "process_or_how contract must explicitly handle legal drift"


def test_strategy_contract_brand_commercial_shape_is_stable():
    service = _make_service()
    strategy = service._normalize_content_strategy(
        {},
        "شقق للبيع فى القاهرة الجديدة",
        "brand_commercial",
        "القاهرة الجديدة",
    )

    assert set(strategy.keys()) == EXPECTED_KEYS
    assert isinstance(strategy["section_role_map"], dict)


def test_strategy_contract_brand_commercial_locked_defaults_are_enforced():
    service = _make_service()
    strategy = service._normalize_content_strategy(
        {},
        "شقق للبيع فى القاهرة الجديدة",
        "brand_commercial",
        "القاهرة الجديدة",
    )

    _assert_exact_contract_fields(strategy)
    _assert_market_angle_contract(strategy)
    _assert_primary_angle_contract(strategy)
    _assert_conversion_strategy_contract(strategy)
    _assert_proof_and_process_contract(strategy)


def test_strategy_contract_run_content_strategy_stays_conservative_with_thin_serp():
    ai_payload = {
        "primary_angle": "Help the reader compare apartment options in New Cairo.",
        "market_angle": "New Cairo residential options.",
        "target_reader_state": "Unsure buyer comparing listings.",
        "pain_point_focus": [],
        "emotional_trigger": "",
        "depth_level": "intermediate",
        "authority_strategy": [],
        "eeat_signals_to_include": [],
        "differentiation_focus": [],
        "conversion_strategy": "Offer -> proof -> CTA",
        "cta_philosophy": "Soft CTA",
        "local_strategy": "",
        "tone_direction": "Helpful",
        "section_role_map": {
            "introduction": "Intro",
            "core_or_benefits": "Core",
            "proof": "Proof",
            "process_or_how": "Process",
            "faq": "FAQ",
            "conclusion": "Conclusion",
        },
    }
    service = _make_service([ai_payload])
    state = _brand_state(ai_payload)

    asyncio.run(service.run_content_strategy(state))
    strategy = state["content_strategy"]

    _assert_exact_contract_fields(strategy)
    _assert_market_angle_contract(strategy)
    _assert_primary_angle_contract(strategy)
    _assert_conversion_strategy_contract(strategy)
    _assert_no_forbidden_phrases(_flatten_text(strategy))


def test_strategy_contract_run_content_strategy_sanitizes_dirty_ai_output():
    ai_payload = {
        "primary_angle": "Drive ROI from real estate investment in New Cairo.",
        "market_angle": "Broad real estate market opportunity in New Cairo.",
        "target_reader_state": "Comparing providers and ready to shortlist.",
        "pain_point_focus": [
            "Need a high-return investment product",
            "Need legal verification before contract execution",
        ],
        "emotional_trigger": "Fear of losing leads due to weak digital presence.",
        "depth_level": "advanced",
        "authority_strategy": [
            "Use ROI projections and delivery model superiority",
            "Lead with provider selection and implementation path clarity",
        ],
        "eeat_signals_to_include": [
            "Legal verification checklist",
            "Investment return proof",
        ],
        "differentiation_focus": [
            "Best provider execution model",
            "Real estate market leadership",
        ],
        "conversion_strategy": "Lead with urgency, market opportunity, and a hard CTA.",
        "cta_philosophy": "Aggressive CTA early and often.",
        "local_strategy": "Reflect broad market opportunity and payment context.",
        "tone_direction": "Authoritative, direct, and sales-first.",
        "section_role_map": {
            "introduction": "Hook with urgency and hard CTA.",
            "core_or_benefits": "Show business outcomes and service value.",
            "proof": "Use ROI metrics and abstract market conditions.",
            "process_or_how": "Explain legal verification and delivery model.",
            "faq": "Overcome objections with hard selling.",
            "conclusion": "Push a strong CTA now.",
        },
    }
    service = _make_service([ai_payload])
    state = _brand_state(ai_payload)

    asyncio.run(service.run_content_strategy(state))
    strategy = state["content_strategy"]

    _assert_exact_contract_fields(strategy)
    _assert_market_angle_contract(strategy)
    _assert_primary_angle_contract(strategy)
    _assert_conversion_strategy_contract(strategy)
    _assert_proof_and_process_contract(strategy)
    _assert_no_forbidden_phrases(_flatten_text(strategy))


def test_strategy_contract_brand_presence_does_not_make_structure_brand_first():
    ai_payload = {
        "primary_angle": "Help the buyer choose the right apartment in New Cairo.",
        "market_angle": "Compare apartment options in New Cairo.",
        "target_reader_state": "Buyer",
        "pain_point_focus": ["Hard to compare options"],
        "emotional_trigger": "Wants clarity",
        "depth_level": "intermediate",
        "authority_strategy": ["Area expertise"],
        "eeat_signals_to_include": ["Local knowledge"],
        "differentiation_focus": ["Curated platform experience"],
        "conversion_strategy": "Explain brand first, then everything else.",
        "cta_philosophy": "Mention the brand often.",
        "local_strategy": "Focus on Cairo districts",
        "tone_direction": "Helpful",
        "section_role_map": {
            "introduction": "Lead with the brand and its strengths.",
            "core_or_benefits": "Show the platform value first.",
            "proof": "Support why the brand is the best.",
            "process_or_how": "Explain how the platform works.",
            "faq": "Answer objections about the brand.",
            "conclusion": "Close with brand CTA.",
        },
    }
    service = _make_service([ai_payload])
    state = _brand_state(ai_payload)

    asyncio.run(service.run_content_strategy(state))
    strategy = state["content_strategy"]

    intro = strategy["section_role_map"]["introduction"].lower()
    core = strategy["section_role_map"]["core_or_benefits"].lower()

    assert "soft brand mention" in intro, "introduction should allow only a soft brand mention"
    assert "offering" in core, "core_or_benefits should stay offer-focused"
    assert "platform" not in core, "brand presence must not dominate core_or_benefits structure"


def test_strategy_contract_non_brand_types_keep_valid_shape():
    service = _make_service()

    informational = service._normalize_content_strategy(
        {},
        "what is shared hosting",
        "informational",
        "Global",
    )
    comparison = service._normalize_content_strategy(
        {},
        "hostinger vs siteground",
        "comparison",
        "Global",
    )

    assert set(informational.keys()) == EXPECTED_KEYS
    assert set(comparison.keys()) == EXPECTED_KEYS
    assert isinstance(informational["section_role_map"], dict)
    assert isinstance(comparison["section_role_map"], dict)


def test_heading_only_detox_preserves_brand_contract():
    service = _make_service()
    controller = object.__new__(AsyncWorkflowController)
    controller.strategy_service = service

    dirty_strategy = {
        "primary_angle": "Drive ROI from real estate investment in New Cairo.",
        "market_angle": "Broad real estate market opportunity in New Cairo.",
        "target_reader_state": "Comparing providers and ready to shortlist.",
        "pain_point_focus": [
            "Need a high-return investment product",
            "Need legal verification before contract execution",
        ],
        "emotional_trigger": "Fear of losing leads due to weak digital presence.",
        "depth_level": "advanced",
        "authority_strategy": [
            "Use ROI projections and delivery model superiority",
            "Lead with provider selection and implementation path clarity",
        ],
        "eeat_signals_to_include": [
            "Legal verification checklist",
            "Investment return proof",
        ],
        "differentiation_focus": [
            "Best provider execution model",
            "Real estate market leadership",
        ],
        "conversion_strategy": "Lead with urgency, market opportunity, and a hard CTA.",
        "cta_philosophy": "Aggressive CTA early and often.",
        "local_strategy": "Reflect broad market opportunity and payment context.",
        "tone_direction": "Authoritative, direct, and sales-first.",
        "section_role_map": {
            "introduction": "Define the keyword and add a hard CTA.",
            "core_or_benefits": "Show business outcomes and service value.",
            "proof": "Show general evidence of quality or standard benefits.",
            "process_or_how": "Explain the standard practical steps simply.",
            "faq": "Overcome objections with hard selling.",
            "conclusion": "Push a strong CTA now.",
        },
    }

    sanitized_strategy, sanitized_brand_context, _, sanitized_writing_blueprint = (
        AsyncWorkflowController._apply_heading_only_detox(
            controller,
            content_strategy=dirty_strategy,
            brand_context="Brand-first positioning with aggressive platform language.",
            brand_advantages=["Exclusive ROI guidance", "Fast provider execution"],
            writing_blueprint="Lead with urgency and market opportunity.",
            primary_keyword="Ø´Ù‚Ù‚ Ù„Ù„Ø¨ÙŠØ¹ ÙÙ‰ Ø§Ù„Ù‚Ø§Ù‡Ø±Ø© Ø§Ù„Ø¬Ø¯ÙŠØ¯Ø©",
            content_type="brand_commercial",
            area="Ø§Ù„Ù‚Ø§Ù‡Ø±Ø© Ø§Ù„Ø¬Ø¯ÙŠØ¯Ø©",
            seo_intelligence=_thin_observed_seo(),
        )
    )

    _assert_exact_contract_fields(sanitized_strategy)
    _assert_conversion_strategy_contract(sanitized_strategy)
    _assert_proof_and_process_contract(sanitized_strategy)
    _assert_no_forbidden_phrases(_flatten_text(sanitized_strategy))

    assert sanitized_strategy["market_angle"] == service._build_brand_market_angle(
        "Ø´Ù‚Ù‚ Ù„Ù„Ø¨ÙŠØ¹ ÙÙ‰ Ø§Ù„Ù‚Ø§Ù‡Ø±Ø© Ø§Ù„Ø¬Ø¯ÙŠØ¯Ø©",
        "Ø§Ù„Ù‚Ø§Ù‡Ø±Ø© Ø§Ù„Ø¬Ø¯ÙŠØ¯Ø©",
    ), "detox should preserve the locked market_angle builder output"
    assert sanitized_strategy["primary_angle"] == service._build_brand_primary_angle(
        "Ø´Ù‚Ù‚ Ù„Ù„Ø¨ÙŠØ¹ ÙÙ‰ Ø§Ù„Ù‚Ø§Ù‡Ø±Ø© Ø§Ù„Ø¬Ø¯ÙŠØ¯Ø©",
        "Ø§Ù„Ù‚Ø§Ù‡Ø±Ø© Ø§Ù„Ø¬Ø¯ÙŠØ¯Ø©",
    ), "detox should preserve the locked primary_angle builder output"

    intro = sanitized_strategy["section_role_map"]["introduction"].lower()
    proof = sanitized_strategy["section_role_map"]["proof"].lower()
    process = sanitized_strategy["section_role_map"]["process_or_how"].lower()

    assert "start with a light, relevant hook" in intro, "detox should preserve the locked commercial intro contract"
    assert "define " not in intro, "detox should not flatten the intro back into a generic definition"
    assert "unit level" in proof or "listing level" in proof, "detox should preserve product-tied proof guidance"
    assert "buying journey" in process, "detox should preserve the buying-journey process role"
    assert "buyer-first" in sanitized_brand_context.lower(), "brand context should stay supportive, not dominant"
    assert "buyer-focused" in sanitized_writing_blueprint.lower(), "writing blueprint should reinforce buyer-facing headings"


def legacy_outline_step_rejects_invalid_heading_only_outline_after_all_retries():
    service = _make_service()
    controller = object.__new__(AsyncWorkflowController)
    controller.strategy_service = service
    controller.validator = ValidationService()
    controller.outline_gen = DummyOutlineGenerator([
        {
            "section_id": "sec_01",
            "heading_text": "مقدمة عن سوق شقق القاهرة الجديدة",
            "heading_level": "INTRO",
            "section_type": "introduction",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_02",
            "heading_text": "كيف تختار أفضل شقق للبيع فى القاهرة الجديدة حسب ميزانيتك؟",
            "heading_level": "H2",
            "section_type": "offer",
            "section_intent": "commercial",
            "subheadings": [],
        },
        {
            "section_id": "sec_03",
            "heading_text": "ما هي أنواع ومساحات الشقق المتاحة في القاهرة الجديدة؟",
            "heading_level": "H2",
            "section_type": "features",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_04",
            "heading_text": "متوسط أسعار شقق القاهرة الجديدة حسب المنطقة وأهم العوامل المؤثرة",
            "heading_level": "H2",
            "section_type": "proof",
            "section_intent": "commercial",
            "subheadings": [],
        },
        {
            "section_id": "sec_05",
            "heading_text": "لماذا تختار عقار يا مصر للبحث عن شقق للبيع فى القاهرة الجديدة؟",
            "heading_level": "H2",
            "section_type": "differentiation",
            "section_intent": "commercial",
            "subheadings": [],
        },
        {
            "section_id": "sec_06",
            "heading_text": "هل تختار شقق جاهزة أم تحت الإنشاء في القاهرة الجديدة؟",
            "heading_level": "H2",
            "section_type": "comparison",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_07",
            "heading_text": "خطوات شراء شقق القاهرة الجديدة من المعاينة حتى القرار",
            "heading_level": "H2",
            "section_type": "process",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_08",
            "heading_text": "أهم الأسئلة الشائعة حول شراء شقق في القاهرة الجديدة",
            "heading_level": "H2",
            "section_type": "faq",
            "section_intent": "informational",
            "subheadings": [
                "هل تتوفر شقق للبيع بتسهيلات في السداد خارج الكمبوندات؟",
            ],
        },
        {
            "section_id": "sec_09",
            "heading_text": "ابدأ الآن في اختيار شقتك المثالية في القاهرة الجديدة",
            "heading_level": "H2",
            "section_type": "conclusion",
            "section_intent": "commercial",
            "subheadings": [],
        },
    ])

    state = {
        "primary_keyword": "شقق للبيع فى القاهرة الجديدة",
        "input_data": {
            "title": "شقق للبيع فى القاهرة الجديدة 2026 | أفضل الأسعار مع عقار يا مصر",
            "keywords": ["شقق للبيع فى القاهرة الجديدة"],
            "urls": [],
            "article_language": "ar",
        },
        "internal_resources": [],
        "seo_intelligence": _thin_observed_seo(),
        "content_strategy": service._normalize_content_strategy(
            {},
            "شقق للبيع فى القاهرة الجديدة",
            "brand_commercial",
            "القاهرة الجديدة",
            seo_intelligence=_thin_observed_seo(),
        ),
        "area": "القاهرة الجديدة",
        "content_type": "brand_commercial",
        "intent": "commercial",
        "article_language": "ar",
        "heading_only_mode": True,
        "brand_name": "عقار يا مصر",
        "brand_context": "Buyer-first structural guidance.",
        "brand_url": "https://aqaryamasr.com/",
        "prohibited_competitors": [],
    }

    try:
        asyncio.run(controller._step_1_outline(state))
        raise AssertionError("Expected StructureError when invalid outline survives all retries")
    except StructureError as exc:
        message = str(exc)
        assert "PRICE_KEYWORD_INTENT_MISSING" in message


def test_research_service_canonicalizes_marketing_brand_line():
    service = ResearchService(ai_client=None, work_dir=os.getcwd())
    canonical = service._canonicalize_brand_name(
        {
            "visible": ["احجز شقق وشاليهات وفلل خاصة في السعودية | Golden Host"],
            "metadata": [],
            "mentions": [],
            "domain": [],
        },
        "https://goldenhost.co/",
        primary_keyword="شقق للايجار في الرياض",
    )
    assert canonical["display_brand_name"] == "Golden Host"
    assert canonical["domain_brand_name"] == "Golden Host"


def test_research_service_prefers_display_brand_name_when_present():
    service = ResearchService(ai_client=None, work_dir=os.getcwd())
    canonical = service._canonicalize_brand_name(
        {
            "explicit_input": ["Creative Minds"],
            "visible": ["Creative Minds (CEMS) | Cems It", "Web Development Company"],
            "metadata": ["Web Development Company"],
            "mentions": [],
            "domain": ["Cems It"],
        },
        "https://cems-it.com/",
        primary_keyword="افضل شركة تصميم مواقع في السعودية",
    )
    assert canonical["display_brand_name"] == "Creative Minds"
    assert canonical["official_brand_name"] == "Creative Minds"
    assert "CEMS" in canonical["brand_aliases"]
    assert "Cems It" in canonical["brand_aliases"]
    assert "Web Development Company" not in canonical["brand_aliases"]


def test_research_service_extracts_explicit_brand_inputs_from_input_payload():
    service = ResearchService(ai_client=None, work_dir=os.getcwd())
    extracted = service._extract_explicit_brand_inputs({
        "input_data": {
            "urls": [
                {"link": "https://cems-it.com/", "text": "Creative Minds"},
                {"link": "https://example.com", "label": "Creative Minds"},
            ]
        }
    })
    assert extracted == ["Creative Minds"]


def test_research_service_serp_intent_firewall_keeps_guide_page_type_for_provider_choice():
    service = ResearchService(ai_client=None, work_dir=os.getcwd())
    insights = {
        "intent_analysis": {
            "confirmed_intent": "informational",
            "commercial_signal_strength": 0.0,
            "informational_signal_strength": 1.0,
            "dominant_page_type": "guide",
        },
        "structural_intelligence": {"dominant_page_type": "guide"},
        "observed_notes": [],
    }

    repaired = service._apply_serp_intent_firewall(
        insights,
        "افضل شركة تصميم مواقع في السعودية",
    )

    assert repaired["intent_analysis"]["confirmed_intent"] == "commercial"
    assert repaired["intent_analysis"]["commercial_signal_strength"] >= 0.7
    assert repaired["intent_analysis"]["informational_signal_strength"] >= 1.0
    assert repaired["intent_analysis"]["dominant_page_type"] == "guide"
    assert repaired["structural_intelligence"]["dominant_page_type"] == "guide"
    assert any("provider-selection keyword signals" in note for note in repaired["observed_notes"])


def test_research_service_serp_intent_firewall_handles_service_provider_without_best():
    service = ResearchService(ai_client=None, work_dir=os.getcwd())
    insights = {
        "intent_analysis": {
            "confirmed_intent": "informational",
            "commercial_signal_strength": 0.1,
            "informational_signal_strength": 0.6,
        },
        "structural_intelligence": {"dominant_page_type": "mixed"},
    }

    repaired = service._apply_serp_intent_firewall(insights, "شركة تنظيف منازل في جدة")

    assert repaired["intent_analysis"]["confirmed_intent"] == "commercial"
    assert repaired["intent_analysis"]["commercial_signal_strength"] >= 0.7


def test_research_service_serp_intent_firewall_does_not_override_learning_query():
    service = ResearchService(ai_client=None, work_dir=os.getcwd())
    insights = {
        "intent_analysis": {
            "confirmed_intent": "informational",
            "commercial_signal_strength": 0.0,
            "informational_signal_strength": 1.0,
        }
    }

    repaired = service._apply_serp_intent_firewall(insights, "ما هو تصميم المواقع")

    assert repaired["intent_analysis"]["confirmed_intent"] == "informational"
    assert repaired["intent_analysis"]["commercial_signal_strength"] == 0.0


def test_research_service_serp_intent_firewall_does_not_overtrigger_best_types_query():
    service = ResearchService(ai_client=None, work_dir=os.getcwd())
    insights = {
        "intent_analysis": {
            "confirmed_intent": "informational",
            "commercial_signal_strength": 0.0,
            "informational_signal_strength": 0.8,
        }
    }

    repaired = service._apply_serp_intent_firewall(insights, "أفضل أنواع الشقق في الرياض")

    assert repaired["intent_analysis"]["confirmed_intent"] == "informational"
    assert repaired["intent_analysis"]["commercial_signal_strength"] == 0.0


def test_strategy_builders_preserve_compound_service_entity():
    service = _make_service()
    terms = service._derive_entity_terms(
        "افضل شركة تصميم مواقع في السعودية",
        "السعودية",
    )
    assert terms["head"] == "شركة"
    assert terms["phrase"] == "شركة تصميم مواقع"
    assert "شركة تصميم مواقع in السعودية" in service._build_brand_primary_angle(
        "افضل شركة تصميم مواقع في السعودية",
        "السعودية",
    )
    assert "شركة تصميم مواقع in السعودية" in service._build_brand_market_angle(
        "افضل شركة تصميم مواقع في السعودية",
        "السعودية",
    )


def test_strategy_market_angle_stays_domain_neutral_for_service_keywords():
    service = _make_service()
    market_angle = service._build_brand_market_angle(
        "افضل شركة تصميم مواقع في السعودية",
        "السعودية",
    ).lower()

    assert "شركة تصميم مواقع in السعودية" in market_angle
    assert "unit type" not in market_angle
    assert "location fit" not in market_angle
    assert "buying path" not in market_angle


def test_heading_only_outline_step_applies_repairs_before_validation():
    service = _make_service()
    controller = object.__new__(AsyncWorkflowController)
    controller.strategy_service = service
    controller.validator = ValidationService()
    controller.outline_gen = DummyOutlineGenerator([
        {
            "section_id": "sec_01",
            "heading_text": "مقدمة عن البحث عن شقق للبيع في القاهرة الجديدة",
            "heading_level": "INTRO",
            "section_type": "introduction",
            "section_intent": "informational",
            "subheadings": [],
        },
        {
            "section_id": "sec_02",
            "heading_text": "كيف تختار أفضل شقق للبيع في القاهرة الجديدة حسب ميزانيتك؟",
            "heading_level": "H2",
            "section_type": "offer",
            "section_intent": "commercial",
            "subheadings": [],
        },
        {
            "section_id": "sec_03",
            "heading_text": "أنواع الشقق المناسبة في القاهرة الجديدة",
            "heading_level": "H2",
            "section_type": "features",
            "section_intent": "informational",
            "subheadings": [
                "تقسيمات الغرف",
                "شقق استوديو",
            ],
        },
        {
            "section_id": "sec_04",
            "heading_text": "أسعار العقارات",
            "heading_level": "H2",
            "section_type": "proof",
            "section_intent": "commercial",
            "subheadings": [],
        },
        {
            "section_id": "sec_05",
            "heading_text": "لماذا تختارنا؟",
            "heading_level": "H2",
            "section_type": "differentiation",
            "section_intent": "commercial",
            "subheadings": [],
        },
        {
            "section_id": "sec_06",
            "heading_text": "هل تختار شقق جاهزة أم تحت الإنشاء في القاهرة الجديدة؟",
            "heading_level": "H2",
            "section_type": "comparison",
            "section_intent": "commercial",
            "subheadings": [],
        },
        {
            "section_id": "sec_07",
            "heading_text": "خطوات شراء شقق في القاهرة الجديدة من المعاينة حتى الاستلام",
            "heading_level": "H2",
            "section_type": "process",
            "section_intent": "commercial",
            "subheadings": [],
        },
        {
            "section_id": "sec_08",
            "heading_text": "أسئلة شائعة حول شراء شقق في القاهرة الجديدة",
            "heading_level": "H2",
            "section_type": "faq",
            "section_intent": "informational",
            "subheadings": [
                "هل تتوفر شقق للبيع بتسهيلات في السداد خارج الكمبوندات؟",
                "ما هي أفضل الأحياء للسكن العائلي في القاهرة الجديدة؟",
            ],
        },
        {
            "section_id": "sec_09",
            "heading_text": "ابدأ الآن في اختيار شقتك المناسبة",
            "heading_level": "H2",
            "section_type": "conclusion",
            "section_intent": "commercial",
            "subheadings": [],
        },
    ])

    state = {
        "primary_keyword": "شقق للبيع في القاهرة الجديدة",
        "input_data": {
            "title": "شقق للبيع في القاهرة الجديدة 2026 | أفضل الأسعار مع عقار يا مصر",
            "keywords": ["شقق للبيع في القاهرة الجديدة"],
            "urls": [],
            "article_language": "ar",
        },
        "internal_resources": [],
        "seo_intelligence": _thin_observed_seo(),
        "content_strategy": service._normalize_content_strategy(
            {},
            "شقق للبيع في القاهرة الجديدة",
            "brand_commercial",
            "القاهرة الجديدة",
            seo_intelligence=_thin_observed_seo(),
        ),
        "area": "القاهرة الجديدة",
        "content_type": "brand_commercial",
        "intent": "commercial",
        "article_language": "ar",
        "heading_only_mode": True,
        "brand_name": "عقار يا مصر",
        "brand_context": "Buyer-first structural guidance.",
        "brand_url": "https://aqaryamasr.com/",
        "prohibited_competitors": [],
    }

    new_state = asyncio.run(controller._step_1_outline(state))
    outline = controller.validator.repair_outline_deterministic(
        new_state["outline"],
        primary_keyword=state["primary_keyword"],
        content_strategy=state["content_strategy"],
        seo_intelligence=state["seo_intelligence"],
        brand_name=state["brand_name"],
        area=state["area"],
    )

    normalized_proof = controller.validator._normalize_heading_label(outline[3]["heading_text"])
    assert "شقق" in normalized_proof
    assert "للبيع" in normalized_proof
    assert "القاهره" in normalized_proof and "الجديده" in normalized_proof
    assert "عقار يا مصر" in outline[4]["heading_text"]
    assert len(outline[2]["subheadings"]) == 1
    assert "استوديو" in controller.validator._normalize_heading_label(outline[2]["subheadings"][0])
    normalized_faq_subs = [
        controller.validator._normalize_heading_label(subheading)
        for subheading in outline[7]["subheadings"]
    ]
    assert all("تسهيلات" not in sub and "السداد" not in sub for sub in normalized_faq_subs)


if __name__ == "__main__":
    failures = []
    tests = [
        obj for name, obj in sorted(globals().items())
        if name.startswith("test_") and callable(obj)
    ]

    for test in tests:
        try:
            test()
            print(f"PASS: {test.__name__}")
        except Exception as exc:
            failures.append((test.__name__, exc))
            safe_error = str(exc).encode("unicode_escape").decode("ascii")
            print(f"FAIL: {test.__name__} -> {safe_error}")

    if failures:
        raise SystemExit(1)
