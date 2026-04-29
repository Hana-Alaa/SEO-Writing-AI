import re
import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple, ClassVar
from collections import Counter
from src.utils.link_manager import LinkManager

logger = logging.getLogger(__name__)

class ValidationService:
    """Service dedicated to content validation, quality checks, and structure enforcement."""

    GENERIC_VISIBLE_HEADINGS: ClassVar[set[str]] = {
        "introduction",
        "overview",
        "summary",
        "faq",
        "faqs",
        "questions",
        "pricing",
        "pricing information",
        "features",
        "benefits",
        "process",
        "steps",
        "conclusion",
        "final thoughts",
        "why us",
        "contact us",
        "guide",
        "complete guide",
        "\u0645\u0642\u062f\u0645\u0629",
        "\u0646\u0638\u0631\u0629 \u0639\u0627\u0645\u0629",
        "\u0627\u0644\u0623\u0633\u0626\u0644\u0629 \u0627\u0644\u0634\u0627\u0626\u0639\u0629",
        "\u0627\u0633\u0626\u0644\u0629 \u0634\u0627\u0626\u0639\u0629",
        "\u0627\u0644\u0623\u0633\u0639\u0627\u0631",
        "\u0627\u0644\u0627\u0633\u0639\u0627\u0631",
        "\u0627\u0644\u0645\u0645\u064a\u0632\u0627\u062a",
        "\u0627\u0644\u0645\u0632\u0627\u064a\u0627",
        "\u0627\u0644\u062e\u0637\u0648\u0627\u062a",
        "\u0627\u0644\u062e\u0627\u062a\u0645\u0629",
        "\u062e\u0627\u062a\u0645\u0629",
        "\u062f\u0644\u064a\u0644",
        "\u062f\u0644\u064a\u0644 \u0634\u0627\u0645\u0644",
    }

    GENERIC_VISIBLE_HEADING_PATTERNS: ClassVar[tuple[str, ...]] = (
        r"^(complete|full)\s+guide$",
        r"^everything\s+you\s+need\s+to\s+know$",
        r"^questions?(?:\s+and\s+answers)?$",
        r"^(pricing|features|benefits|process|steps)(?:\s+\w+)?$",
        r"^(final|last)\s+(thoughts|notes|words)$",
        r"^(why|how)\s+it\s+works$",
        r"^\u062f\u0644\u064a\u0644(?:\s+\u0634\u0627\u0645\u0644)?$",
        r"^\u0643\u0644\s+\u0645\u0627\s+\u062a\u062d\u062a\u0627\u062c\s+\u0645\u0639\u0631\u0641\u062a\u0647$",
    )

    LIGHT_SEO_STOPWORDS: ClassVar[set[str]] = {
        "a", "an", "the", "and", "or", "of", "to", "for", "in", "on", "at", "by", "with",
        "your", "you", "how", "why", "what", "when", "vs", "versus",
        "في", "فى", "من", "على", "عن", "الى", "إلى", "مع", "او", "أو", "ثم", "هذا", "هذه",
    }

    ENTITY_SKIP_TOKENS: ClassVar[set[str]] = {
        "best", "top", "cheap", "cheapest", "affordable", "guide", "compare", "comparison",
        "how", "why", "what", "latest", "new", "sale", "rent", "buy",
        "افضل", "أفضل", "ارخص", "أرخص", "دليل", "مقارنة", "كيف", "ما", "متى", "اين", "أين",
        "للبيع", "للايجار", "للاستثمار", "بيع", "شراء", "استثمار", "استثماري", "سعر", "اسعار", "أسعار",
    }

    OPTIONAL_SECTION_SIGNALS: ClassVar[Dict[str, tuple[str, ...]]] = {
        "legal": (
            "legal", "law", "laws", "contract", "contracts", "registration", "documents", "documentation",
            "paperwork", "license", "licenses", "verify", "verification", "validate", "validation", "due diligence", "قانون", "قانوني", "عقد", "عقود", "تسجيل", "مستندات", "اوراق", "أوراق", "تحقق", "مراجعة", "التحقق", "قانونية",
        ),
        "financing_payment": (
            "finance", "financing", "payment", "payments", "installment", "installments", "mortgage", "plan", "plans",
            "تمويل", "تمويلي", "سداد", "دفعة", "دفعات", "مقدم", "تقسيط", "اقساط", "أقساط", "خطة سداد",
        ),
        "infrastructure": (
            "infrastructure", "metro", "transport", "roads", "road", "access", "utilities", "connectivity",
            "بنية", "تحتية", "مرافق", "طرق", "طريق", "محور", "مترو", "مواصلات",
        ),
        "investment": (
            "investment", "investing", "roi", "yield", "returns", "return", "profit", "profits",
            "استثمار", "استثماري", "عائد", "عوائد", "ربح", "ارباح", "أرباح",
        ),
    }

    BRAND_ALLOWED_HEADING_SECTION_TYPES: ClassVar[set[str]] = {
        "introduction", "conclusion", "differentiation", "brand_differentiation", "why_choose_us", "differentiators", "usp"
    }

    COMMERCIAL_FLOW_SECTION_ALIASES: ClassVar[Dict[str, set[str]]] = {
        "introduction": {"introduction"},
        "offer": {"offer", "core", "service_definition", "what_is", "definition", "offer_overview"},
        "features": {"features", "key_features", "included", "features_benefits", "key_benefits"},
        "differentiation": {"differentiation", "brand_differentiation", "why_choose_us", "differentiators", "usp"},
        "proof": {"proof", "authority", "case_study", "proof_authority", "validation", "pricing"},
        "comparison": {"comparison", "comparison_logic", "comparison_utility", "alternatives", "options", "criteria"},
        "process": {"process", "how_it_works", "implementation", "workflow", "process_workflow", "steps"},
        "faq": {"faq"},
        "conclusion": {"conclusion", "final_verdict"},
    }

    COMPARISON_HEADING_SIGNALS: ClassVar[tuple[str, ...]] = (
        "compare", "comparison", "vs", "versus", "difference", "differences", "options",
        "ready", "under construction", "district", "districts", "area", "areas", "payment", "payments", "installment", "installments",
        "compound", "compounds", "gated community", "standalone", "standalone building", "standalone buildings", "independent building", "independent buildings",
        "مقارنة", "الفرق", "فروق", "جاهز", "تحت الانشاء", "تحت الإنشاء", "مناطق", "منطقة", "موقع", "مواقع", "تقسيط", "اقساط", "أقساط", "سداد", "شهري", "سنوي", "يومي",
        "كمبوند", "كمبوندات", "داخل كمبوند", "خارج كمبوند", "عمارات مستقلة", "عمارة مستقلة",
    )

    PRICE_HEADING_SIGNALS: ClassVar[tuple[str, ...]] = (
        "price", "prices", "pricing", "cost", "meter", "payment", "installment",
        "سعر", "اسعار", "أسعار", "تكلفة", "متر", "تقسيط", "اقساط", "أقساط",
    )

    PRICE_FACTOR_SUPPORT_SIGNALS: ClassVar[tuple[str, ...]] = (
        "factor", "factors", "impact", "affect", "affects", "influence", "difference", "differences", "driver", "drivers",
        "location", "proximity", "finish", "finishing", "condition", "district", "road", "roads", "axis", "axes", "meter", "size",
        "عامل", "العوامل", "مؤثر", "مؤثرة", "تاثير", "تأثير", "فرق", "فروق", "اختلاف", "اختلافات",
        "موقع", "الموقع", "قرب", "القرب", "تشطيب", "التشطيب", "مساحة", "مساحات", "محور", "محاور", "التسعين",
    )

    def __init__(self, ai_client=None, semantic_model=None):
        self.ai_client = ai_client
        self.semantic_model = semantic_model
        
        # Bootstrap default thresholds for tone intensity
        self.TONE_THRESHOLDS = {
            "informational": 5.0,
            "commercial": 8.0,
            "hybrid": 6.0
        }
        
        # Categorized Sales Markers for Tone Validation (v2.3)
        # Category A: Aggressive (BANNED in Introduction, High weight in Body)
        self.AGGRESSIVE_MARKERS = {
            "فرصة ذهبية": 3, "عائد خيالي": 3, "اتصل الآن": 3, "احجز الآن": 3,
            "سجل": 3, "لا تفوت": 3, "أسرع": 2, "exclusive": 3, "limited offer": 3,
            "roi": 3, "عائد استثماري": 2.5, "أفضل الأسعار": 2.5
        }
        
        # Category B: Soft (ALLOWED in Introduction, Low weight/Neutral)
        self.SOFT_MARKERS = {
            "luxury": 1.5, "prime location": 1, "strategic": 1, "investment": 1,
            "أفضل": 0.5, "أرقى": 0.5, "أسرع": 1, "أحدث": 0.5, "مميز": 0.5,
            "منصة": 0, "موقع": 0, "يساعدك": 0, "ثقة": 0, "خبرة": 0 # Zero weight for branding/trust help
        }
        
        # Category C: Abstract Jargon & Prestige Framing (BANNED in Intro, Weighted limit in Body) (v4.0)
        self.JARGON_MARKERS = {
            "استثمار": 1, "استثماري": 1, "استراتيجي": 1, "حصري": 1, "إليت": 1,
            "عائد": 1.0, "مستهدف": 1, "تحدي": 0.5, "roi": 1.0, "investment": 1,
            "strategic": 1.5, "premium": 1.5, "lifestyle": 1.5, "asset": 1.5,
            "competitive": 1.5, "prestige": 2, "elite": 2, "luxury": 1.5,
            # Prestige/Framing Patterns (Detox v4.0)
            "تموضع": 1.5, "التموضع": 1.5, "منظومه": 2, "المنظومه": 2, 
            "رياده": 2, "الرياده": 2, "سلطنه": 3, "executive": 2.5, 
            "positioning": 1.5, "analysis": 1.0, "framework": 2.0, "protocol": 2.0
        }
        
        # Combined pool for general density calculation
        self.SALES_MARKERS = {**self.AGGRESSIVE_MARKERS, **self.SOFT_MARKERS, **self.JARGON_MARKERS}

    def _check_plain_language_compliance(self, text: str) -> Dict[str, Any]:
        """
        Refined v3.1: Calculates jargon intensity based on context, density, and proximity.
        Ensures the text is accessible without distorting meaning.
        """
        if not text: return {"fail": False}
        
        # 1. Normalize and segment
        text_norm = self._normalize_arabic(text.lower())
        # Split into sentences using standard and Arabic delimiters
        sentences = [s.strip() for s in re.split(r'[.!?؟\n]', text_norm) if s.strip()]
        
        words_overall = re.findall(r'\b\w+\b', text_norm)
        total_word_count = max(len(words_overall), 1)
        
        cumulative_jargon_score = 0.0
        found_jargon = Counter()
        
        # 2. Analyze sentence-level context and proximity
        for sentence in sentences:
            sentence_jargon_score = 0.0
            sentence_jargon_count = 0
            
            # Find jargon markers in this specific sentence
            for marker, weight in self.JARGON_MARKERS.items():
                pattern = r'\b{}\b'.format(re.escape(self._normalize_arabic(marker)))
                matches = len(re.findall(pattern, sentence))
                if matches > 0:
                    # Basic weight
                    sentence_jargon_score += (matches * weight)
                    sentence_jargon_count += matches
                    found_jargon[marker] += matches
            
            # PROXIMITY PENALTY: If multiple jargon words share a sentence, boost the score
            if sentence_jargon_count > 1:
                sentence_jargon_score *= 1.5
                
            # SALES CONTEXT PENALTY: Check if aggressive markers share this sentence
            found_aggressive = False
            for agg_marker in self.AGGRESSIVE_MARKERS.keys():
                if self._normalize_arabic(agg_marker) in sentence:
                    found_aggressive = True
                    break
            
            if found_aggressive and sentence_jargon_count > 0:
                sentence_jargon_score *= 2.0
                
            cumulative_jargon_score += sentence_jargon_score
            
        # 3. Calculate Final Intensity (Normalized per 100 words)
        intensity = (cumulative_jargon_score / total_word_count) * 100
        
        # NEW THRESHOLD: 5.0 (v4.0 allows single natural occurrences)
        # Minimum absolute floor: 3.0
        max_rep = max(found_jargon.values()) if found_jargon else 0
        
        jargon_list = list(found_jargon.keys())
        msg = f"PLAIN_LANGUAGE_REQUIRED: Content feels too abstract, prestige-heavy, or uses expert-only framing (Intensity {intensity:.1f}, Score {cumulative_jargon_score:.1f}). Found: {', '.join(jargon_list)}. "
        msg += "You MUST write for a zero-knowledge reader. Do NOT do shallow synonym replacement. You must fully reconstruct the sentence in simple everyday language. Preserve the meaning, but explain the practical outcome using concrete actions or results. Do not keep the original investor-style sentence structure."
        
        if cumulative_jargon_score >= 3.0 and (intensity > 5.0 or max_rep > 3):
            return {
                "fail": True,
                "reason": "PLAIN_LANGUAGE_REQUIRED",
                "message": msg
            }
        elif cumulative_jargon_score >= 2.0 and intensity > 3.0:
            return {
                "fail": False,
                "warnings": [msg.replace("PLAIN_LANGUAGE_REQUIRED", "PLAIN_LANGUAGE_WARNING")]
            }
            
        return {"fail": False}

    def _check_intro_tone_profile(self, text: str) -> Dict[str, Any]:
        """
        Specialized validation for introductions. 
        Permits soft branding/trust language but bans aggressive triggers.
        """
        if not text: return {"fail": False}
        
        text_norm = self._normalize_arabic(text.lower())
        found_aggressive = []
        
        for marker in self.AGGRESSIVE_MARKERS.keys():
            marker_norm = self._normalize_arabic(marker.lower())
            if marker_norm in text_norm:
                found_aggressive.append(marker)
                
        # Check for direct standard link patterns which could be CTAs
        # (Though some links are allowed, direct CTA phrases in links are the problem)
        if found_aggressive:
            return {
                "fail": True, 
                "reason": "INTRO_TONE_PROFILE_MISMATCH", 
                "message": f"Introduction contains aggressive sales triggers: {', '.join(found_aggressive)}. The introduction is allowed to be commercial, but only in a soft, trust-based manner."
            }
            
        return {"fail": False}

    def _check_structural_integrity(self, content: str, target_format: str, heading_text: str) -> List[str]:
        """
        Refined v4.0.1: Validates that the visual presentation matches the logical structure.
        Checks for hidden subsections, decorative bullets, and format mismatches.
        """
        errors = []
        if not content: return errors
        
        paragraphs = [p.strip() for p in content.split("\n\n") if p.strip()]
        
        # 1. Detection of Hidden Subsections (Bold labels used as headers in paragraphs)
        # Look for 3+ occurrences of "**Label**:" or "**Label** " at the start of paragraphs/lines
        hidden_pattern = r'(?m)^\s*\*\*([^*:]+)\*\*[:\s]'
        hidden_labels = re.findall(hidden_pattern, content)
        
        # Heuristic: If 3+ labels found and each is followed by substantial text (> 15 words)
        if len(hidden_labels) >= 3:
             # Verify if they are true 'Mini-Stories'
             valid_hidden = 0
             for label in hidden_labels:
                 # Check the text following this specific label
                 escaped_label = re.escape(label)
                 pattern = r'\*\*{}\*\*[:\s](.*?)(?=\n\s*\*\*|\Z)'.format(escaped_label)
                 follow_up = re.search(pattern, content, re.DOTALL)
                 if follow_up and len(follow_up.group(1).split()) > 15:
                     valid_hidden += 1
             
             if valid_hidden >= 3:
                 errors.append(f"HIDDEN_SUBSECTIONS_DETECTED: Section '{heading_text}' uses bold labels for 3+ independent items with details. These MUST be converted into H3 subsections for better SEO and readability.")

        # 2. Refined Decorative Bullets Detection
        bullet_lines = [l.strip() for l in content.split("\n") if l.strip().startswith(("- ", "* ", "• "))]
        if bullet_lines:
            total_words = len(content.split())
            bullet_words = sum(len(l.split()) for l in bullet_lines)
            narrative_ratio = (total_words - bullet_words) / max(total_words, 1)
            
            # Condition A: Mostly narrative (>80%) with very few bullets (1-2)
            if narrative_ratio > 0.8 and len(bullet_lines) <= 2:
                # Condition B: Overlap Analysis (Do bullets just repeat paragraph content?)
                # Extract nouns/entities from bullets
                bullet_text = " ".join(bullet_lines).lower()
                # Get the paragraph immediately preceding the first bullet
                lines = content.split("\n")
                first_bullet_idx = next(i for i, l in enumerate(lines) if l.strip().startswith(("-", "*", "•")))
                preceding_text = ""
                for i in range(first_bullet_idx - 1, -1, -1):
                    if lines[i].strip() and not lines[i].strip().startswith(("#", "-", "*", "•")):
                        preceding_text = lines[i].strip().lower()
                        break
                
                if preceding_text:
                    # Check for token overlap
                    bullet_tokens = set(re.findall(r'\b\w{3,}\b', bullet_text))
                    preceding_tokens = set(re.findall(r'\b\w{3,}\b', preceding_text))
                    overlap = bullet_tokens.intersection(preceding_tokens)
                    
                    if len(overlap) / max(len(bullet_tokens), 1) > 0.6:
                        errors.append(f"DECORATIVE_BULLETS_DETECTED: The bullets in '{heading_text}' appear to be decorative, merely repeating info already stated in the narrative. Lists must add real structural value or independent details.")

        # 3. Structure Format Mismatch (Target vs Actual)
        if target_format == "h3_subsections":
            h3_count = len(re.findall(r'^###\s', content, re.MULTILINE))
            if h3_count < 2:
                errors.append(f"STRUCTURE_FORMAT_MISMATCH: Section '{heading_text}' was assigned 'h3_subsections' but contains {h3_count} H3 headers. Each independent item MUST have its own H3 header.")
        
        elif target_format == "direct_bullets":
            if not bullet_lines:
                errors.append(f"STRUCTURE_FORMAT_MISMATCH: Section '{heading_text}' was assigned 'direct_bullets' but contains no bulleted list.")
            else:
                # Check for narrative lead-in limit (v4.0.1)
                # Find lines before the first bullet that aren't headings
                lines = content.split("\n")
                lead_in_count = 0
                for l in lines:
                    stripped = l.strip()
                    if not stripped or stripped.startswith("#"): continue
                    if stripped.startswith(("-", "*", "•")): break
                    lead_in_count += 1
                
                if lead_in_count > 2:
                    errors.append(f"STRUCTURE_FORMAT_MISMATCH: 'direct_bullets' format allows max 2 intro lines. Found {lead_in_count} lines of narrative lead-in in '{heading_text}'. Start the list immediately.")

        elif target_format == "compact_narrative":
            if bullet_lines or "###" in content:
                # We allow it, but we warn if it's over-modularized for a compact topic
                # For now, let's just log or add a low-priority warning
                pass

        return errors

    def _normalize_arabic(self, text: str) -> str:
        """Standardizes Arabic characters to improve matching reliability."""
        if not text: return ""
        replacements = {
            "أ": "ا", "إ": "ا", "آ": "ا",
            "ة": "ه",
            "ى": "ي",
            "ئ": "ء", "ؤ": "ء",
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text.lower()

    def _normalize_heading_label(self, text: str) -> str:
        if not text:
            return ""
        normalized = self._normalize_arabic(str(text).lower())
        normalized = re.sub(r"[^\w\u0600-\u06FF\s]", " ", normalized)
        normalized = normalized.replace("_", " ")
        return re.sub(r"\s+", " ", normalized).strip()

    def _is_generic_visible_heading(self, text: str) -> bool:
        normalized = self._normalize_heading_label(text)
        if not normalized:
            return True

        if normalized in self.GENERIC_VISIBLE_HEADINGS:
            return True

        return any(
            re.match(pattern, normalized, re.IGNORECASE)
            for pattern in self.GENERIC_VISIBLE_HEADING_PATTERNS
        )

    def _is_valid_faq_question(self, text: str) -> bool:
        normalized = self._normalize_heading_label(text)
        if not normalized:
            return False
        first_word = normalized.split()[0]
        valid_starters = {"ما", "كيف", "هل", "كم", "لماذا", "متى", "اين", "أين", "ماذا", "بكم", "لمين", "من"}
        return first_word in valid_starters

    def _tokenize_search_phrase(self, text: str) -> List[str]:
        normalized = self._normalize_heading_label(text)
        if not normalized:
            return []
        return [
            token for token in normalized.split()
            if token and token not in self.LIGHT_SEO_STOPWORDS and len(token) > 1
        ]

    def _expand_token_variants(self, token: str) -> set[str]:
        variants = {token}
        if not token:
            return variants

        for prefix in ("وال", "بال", "كال", "فال", "لل", "ال", "و", "ب", "ل", "ف", "ك"):
            if token.startswith(prefix) and len(token) - len(prefix) >= 2:
                variants.add(token[len(prefix):])

        if token.endswith("s") and len(token) > 3:
            variants.add(token[:-1])

        arab_pairs = {
            "شقق": "شقه", "شقه": "شقق", "شقة": "شقق",
            "فلل": "فيلا", "فيلا": "فلل",
            "عيادات": "عياده", "عياده": "عيادات", "عيادة": "عيادات",
            "سيارات": "سياره", "سياره": "سيارات", "سيارة": "سيارات",
            "مكاتب": "مكتب", "مكتب": "مكاتب",
            "محلات": "محل", "محل": "محلات",
        }
        
        bases = list(variants)
        for base in bases:
            if base in arab_pairs:
                variants.add(arab_pairs[base])

        return {variant for variant in variants if variant}

    def _expanded_token_set(self, text: str) -> set[str]:
        expanded = set()
        for token in self._tokenize_search_phrase(text):
            expanded.update(self._expand_token_variants(token))
        return expanded

    def _derive_keyword_profile(self, primary_keyword: str, area: str = "") -> Dict[str, Any]:
        keyword_tokens = self._tokenize_search_phrase(primary_keyword)
        area_tokens = self._tokenize_search_phrase(area)

        head_entity = ""
        for token in keyword_tokens:
            if token not in self.ENTITY_SKIP_TOKENS:
                head_entity = token
                break
        if not head_entity and keyword_tokens:
            head_entity = keyword_tokens[0]

        # Robust Location Extraction
        location_tokens = []
        if area_tokens:
            location_tokens = [token for token in area_tokens if token in keyword_tokens]
        
        if not location_tokens:
            # Fallback heuristic: Try to find location in the primary keyword
            # Usually after 'في' or just the last token if we have 3+ tokens
            norm_pk = self._normalize_heading_label(primary_keyword)
            words = norm_pk.split()
            if "في" in words:
                idx = words.index("في")
                if idx + 1 < len(words):
                    location_tokens = [words[idx + 1]]
            elif len(keyword_tokens) >= 3:
                last_token = keyword_tokens[-1]
                if last_token != head_entity and last_token not in self.ENTITY_SKIP_TOKENS:
                    location_tokens = [last_token]
        
        # Intent is what's left after entity and location are removed
        intent_tokens = [
            token for token in keyword_tokens
            if token != head_entity and token not in location_tokens
        ]

        return {
            "primary_keyword": primary_keyword or "",
            "normalized_keyword": self._normalize_heading_label(primary_keyword),
            "keyword_tokens": keyword_tokens,
            "head_entity": head_entity,
            "location_tokens": location_tokens,
            "intent_tokens": intent_tokens,
        }

    def _heading_contains_keyword_anchor(self, heading_text: str, keyword_profile: Dict[str, Any]) -> bool:
        normalized_heading = self._normalize_heading_label(heading_text)
        normalized_keyword = keyword_profile.get("normalized_keyword", "")
        if normalized_keyword and normalized_keyword in normalized_heading:
            return True

        heading_tokens = self._expanded_token_set(heading_text)
        keyword_tokens = keyword_profile.get("keyword_tokens", [])
        head_entity = keyword_profile.get("head_entity", "")
        location_tokens = keyword_profile.get("location_tokens", [])
        intent_tokens = keyword_profile.get("intent_tokens", [])

        overlap = sum(1 for token in keyword_tokens if token in heading_tokens)
        if head_entity and head_entity in heading_tokens and overlap >= max(2, len(keyword_tokens) - 1):
            return True

        if head_entity and head_entity in heading_tokens:
            has_intent = not intent_tokens or any(token in heading_tokens for token in intent_tokens)
            has_location = not location_tokens or all(token in heading_tokens for token in location_tokens)
            if has_intent and has_location:
                return True

        return False

    def _heading_preserves_entity_focus(self, heading_text: str, keyword_profile: Dict[str, Any]) -> bool:
        head_entity = keyword_profile.get("head_entity", "")
        keyword_tokens = keyword_profile.get("keyword_tokens", [])
        if not head_entity or not keyword_tokens:
            return True

        heading_tokens = self._expanded_token_set(heading_text)
        head_entity_variants = self._expand_token_variants(head_entity)
        if head_entity_variants.intersection(heading_tokens):
            return True

        location_tokens = keyword_profile.get("location_tokens", [])
        intent_tokens = keyword_profile.get("intent_tokens", [])
        overlap = sum(1 for token in keyword_tokens if token in heading_tokens)
        has_location = not location_tokens or all(token in heading_tokens for token in location_tokens)
        has_intent = not intent_tokens or any(token in heading_tokens for token in intent_tokens)

        return overlap >= max(2, len(keyword_tokens) - 1) and has_location and has_intent

    def _brand_appears_in_heading(self, text: str, brand_name: str) -> bool:
        normalized_brand = self._normalize_heading_label(brand_name)
        if not normalized_brand:
            return False

        normalized_text = self._normalize_heading_label(text)
        if normalized_brand in normalized_text:
            return True

        brand_tokens = self._tokenize_search_phrase(brand_name)
        if not brand_tokens:
            return False

        text_tokens = self._expanded_token_set(text)
        return all(token in text_tokens for token in brand_tokens)

    def _heading_contains_exact_brand_name(self, text: str, brand_name: str) -> bool:
        normalized_brand = self._normalize_heading_label(brand_name)
        normalized_text = self._normalize_heading_label(text)
        
        if bool(normalized_brand) and normalized_brand in normalized_text:
            return True
            
        if len(brand_name) > 30:
            substitutes = ["المنصه", "المنصة", "الموقع", "الخدمه", "الخدمة", "منصة", "موقع", "خدمة"]
            if any(sub in normalized_text.split() for sub in substitutes):
                return True
                
        return False

    def _brand_heading_allowed(self, section_type: str) -> bool:
        return (section_type or "").lower() in self.BRAND_ALLOWED_HEADING_SECTION_TYPES

    def _commercial_flow_stage(self, section: Dict[str, Any]) -> str:
        section_type = (section.get("section_type") or "").lower().strip()
        for stage, aliases in self.COMMERCIAL_FLOW_SECTION_ALIASES.items():
            if section_type in aliases:
                return stage
        return ""

    def _contains_any_signal(self, text: str, signals: tuple[str, ...]) -> bool:
        normalized = self._normalize_heading_label(text)
        return any(self._normalize_heading_label(signal) in normalized for signal in signals)

    def _detect_optional_section_topics(self, text: str) -> set[str]:
        normalized = self._normalize_heading_label(text)
        hits = set()
        for topic, signals in self.OPTIONAL_SECTION_SIGNALS.items():
            if any(signal in normalized for signal in signals):
                hits.add(topic)
        return hits

    def _build_outline_support_blob(
        self,
        primary_keyword: str = "",
        content_strategy: Optional[Dict[str, Any]] = None,
        seo_intelligence: Optional[Dict[str, Any]] = None,
    ) -> str:
        parts = [primary_keyword or ""]

        if isinstance(content_strategy, dict) and content_strategy:
            for key in ("primary_angle", "market_angle", "target_reader_state", "local_strategy"):
                parts.append(str(content_strategy.get(key, "") or ""))
            parts.append(str(content_strategy.get("section_role_map", {}) or {}))

        market_analysis = (seo_intelligence or {}).get("market_analysis", {}) if isinstance(seo_intelligence, dict) else {}
        semantic_assets = market_analysis.get("semantic_assets", {}) if isinstance(market_analysis, dict) else {}
        market_insights = market_analysis.get("market_insights", {}) if isinstance(market_analysis, dict) else {}

        parts.append(str(semantic_assets.get("paa_questions", []) or []))
        parts.append(str(semantic_assets.get("related_searches", []) or []))
        parts.append(str(market_insights.get("keyword_clusters", []) or []))
        parts.append(str(market_insights.get("content_gaps", []) or []))

        return self._normalize_heading_label(" ".join(parts))

    def _optional_topic_is_justified(
        self,
        topic: str,
        support_blob: str,
    ) -> bool:
        signals = self.OPTIONAL_SECTION_SIGNALS.get(topic, ())
        return any(self._normalize_heading_label(signal) in support_blob for signal in signals)

    def prune_unsupported_optional_subheadings(
        self,
        outline: List[Dict[str, Any]],
        primary_keyword: str = "",
        content_strategy: Optional[Dict[str, Any]] = None,
        seo_intelligence: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Deterministically removes optional-topic H3s that are not justified by the keyword,
        SERP/PAA, or strategy. This is intentionally narrow and should never invent or rewrite
        headings; it only prunes unsupported child prompts before validation/retry.
        """
        support_blob = self._build_outline_support_blob(
            primary_keyword=primary_keyword,
            content_strategy=content_strategy,
            seo_intelligence=seo_intelligence,
        )

        cleaned_outline: List[Dict[str, Any]] = []
        for section in outline:
            cleaned_section = dict(section)
            subheadings = cleaned_section.get("subheadings", [])
            stage = self._commercial_flow_stage(cleaned_section)

            if isinstance(subheadings, list) and subheadings:
                cleaned_subheadings = []
                for subheading in subheadings:
                    subheading_text = str(subheading).strip()
                    unsupported_child_topics = [
                        topic for topic in self._detect_optional_section_topics(subheading_text)
                        if not self._optional_topic_is_justified(topic, support_blob)
                    ]
                    if stage == "comparison":
                        unsupported_child_topics = [
                            topic for topic in unsupported_child_topics
                            if topic != "financing_payment"
                        ]

                    if unsupported_child_topics:
                        logger.warning(
                            "[outline_repair] Pruned unsupported subheading '%s' (topics: %s).",
                            subheading_text,
                            ", ".join(unsupported_child_topics),
                        )
                        continue

                    cleaned_subheadings.append(subheading)

                cleaned_section["subheadings"] = cleaned_subheadings

            cleaned_outline.append(cleaned_section)

        return cleaned_outline

    def _faq_question_is_supported(self, question: str, keyword_profile: Dict[str, Any], support_blob: str) -> bool:
        question_tokens = self._expanded_token_set(question)
        
        # 1. Overlap with keyword intent or location
        location_tokens = set(keyword_profile.get("location_tokens", []))
        intent_tokens = set(keyword_profile.get("intent_tokens", []))
        if question_tokens.intersection(location_tokens) or question_tokens.intersection(intent_tokens):
            return True
            
        # 2. Overlap with head entity
        head_entity = keyword_profile.get("head_entity", "")
        head_variants = self._expand_token_variants(head_entity) if head_entity else set()
        if head_variants.intersection(question_tokens):
            return True
            
        # 3. Found in support blob (PAA, related searches)
        normalized_q = self._normalize_heading_label(question)
        # We check if any significant part of the question is in the support blob
        # (simplified: just check if the normalized question or key tokens are there)
        meaningful_tokens = [t for t in question_tokens if len(t) > 2]
        if meaningful_tokens and sum(1 for t in meaningful_tokens if t in support_blob) >= max(1, len(meaningful_tokens) // 2):
            return True
            
        # 4. Safe buyer-facing commercial signals (Always acceptable)
        # Normalized: سعر, اسعار, مساحه, مساحات, تشطيب, احياء, مناطق, حجز, معاينه, استلام
        safe_buyer_signals = ("سعر", "اسعار", "مساحه", "مساحات", "تشطيب", "احياء", "مناطق", "حجز", "معاينه", "استلام")
        if any(signal in question_tokens for signal in safe_buyer_signals):
            return True
            
        return False

    def _subheading_is_too_granular(self, text: str, stage: str = "") -> bool:
        normalized = self._normalize_heading_label(text)
        
        # 1. Banned granular detail signals (paragraph-level details)
        granular_signals = (
            "تشطيب", "تقسيم", "توزيع", "تهويه", "تهوية", "تكييف", "مواصفات", "تفاصيل", "داخليه", "داخلية", "جوده", "جودة", 
            "finishing", "layout", "ventilation", "conditioning", "specs", "details", "internal", "quality"
        )
        
        # 2. Section-Specific Rules
        if stage == "features":
            # Features section MUST focus on Unit Types / Categories
            # Normalized Unit Types: استوديو, عائلي, عائليه, دوبلكس, حديقه, حديقة, جاهزه, جاهزة, شقق, فيلا, فلل, بنتهاوس, تاون هاوس
            unit_types = ("استوديو", "عائلي", "عائليه", "دوبلكس", "حديقه", "حديقة", "جاهزه", "جاهزة", "شقق", "فيلا", "فلل", "بنتهاوس", "تاون هاوس")
            if any(ut in normalized for ut in unit_types):
                return False # Strong standalone bucket
            
            # If it's in features and not a unit type but contains granular signals, it's weak
            if any(gs in normalized for gs in granular_signals):
                return True

        # 3. Global Grain Check
        # If the H3 is purely a small spec/attribute (usually 1-3 words)
        words = normalized.split()
        if len(words) <= 3 and any(gs in normalized for gs in granular_signals):
            return True
            
        return False

    def repair_outline_deterministic(
        self,
        outline: List[Dict[str, Any]],
        primary_keyword: str = "",
        content_strategy: Optional[Dict[str, Any]] = None,
        seo_intelligence: Optional[Dict[str, Any]] = None,
        brand_name: str = "",
        area: str = "",
    ) -> List[Dict[str, Any]]:
        """
        Deterministically repairs validation issues:
        1. Proof/Pricing Intent: Ensures pricing headings keep intent + location.
        2. Brand Differentiation: Rewrites generic differentiation headings.
        3. Keyword Stuffing: Rewrites excess PK headings in non-protected sections.
        4. H3 Quality: Prunes multi-topic or granular subheadings.
        5. FAQ Cleanup: Prunes non-questions/unsupported.
        """
        keyword_profile = self._derive_keyword_profile(primary_keyword, area=area)
        normalized_pk = keyword_profile.get("normalized_keyword", "")
        head_entity = keyword_profile.get("head_entity", "")
        entity_phrase = keyword_profile.get("entity_phrase", "") or head_entity
        intent_tokens = keyword_profile.get("intent_tokens", [])
        location_tokens = keyword_profile.get("location_tokens", [])
        
        support_blob = self._build_outline_support_blob(
            primary_keyword=primary_keyword,
            content_strategy=content_strategy,
            seo_intelligence=seo_intelligence,
        )

        pk_count = 0
        cleaned_outline = []
        protected_sections = {"introduction", "proof", "differentiation", "faq", "conclusion"}

        for section in outline:
            cleaned_section = dict(section)
            heading_text = str(cleaned_section.get("heading_text", "")).strip()
            heading_level = (cleaned_section.get("heading_level") or "").upper()
            section_type = (cleaned_section.get("section_type") or "").lower()
            stage = self._commercial_flow_stage(cleaned_section)

            # 1. Proof/Pricing Repair (High Priority)
            is_pricing = (section_type == "proof" or self._contains_any_signal(heading_text, self.PRICE_HEADING_SIGNALS))
            if is_pricing and heading_level == "H2":
                normalized_h = self._normalize_heading_label(heading_text)
                has_intent = any(tok in normalized_h for tok in intent_tokens) if intent_tokens else True
                has_loc = all(tok in normalized_h for tok in location_tokens) if location_tokens else True
                
                if not has_intent or not has_loc:
                    entity_str = entity_phrase or head_entity or "العقار"
                    intent_str = ""
                    if intent_tokens and not any(tok in self._normalize_heading_label(entity_str) for tok in intent_tokens):
                        intent_str = " ".join(intent_tokens)
                    loc_str = f"في {' '.join(location_tokens)}" if location_tokens else ""
                    new_text = f"متوسط أسعار {entity_str} {intent_str} {loc_str} حسب المنطقة وأهم العوامل المؤثرة".replace("  ", " ").strip()
                    logger.warning(f"[outline_repair] Rebuilt weak pricing heading: '{heading_text}' -> '{new_text}'")
                    cleaned_section["heading_text"] = new_text
                    heading_text = new_text # Update for subsequent checks

            # 2. Brand Differentiation Repair
            if section_type == "differentiation" and heading_level == "H2":
                effective_brand = brand_name if brand_name and len(brand_name) <= 30 else "المنصة"
                has_brand = self._heading_contains_exact_brand_name(heading_text, brand_name)
                has_intent = normalized_pk and normalized_pk in self._normalize_heading_label(heading_text)
                
                if not has_brand or not has_intent:
                    anchor_topic = primary_keyword or entity_phrase or head_entity or "الخدمة"
                    new_heading = f"لماذا تختار {effective_brand} للبحث عن {anchor_topic}؟"
                    logger.warning(f"[outline_repair] Rewrote generic differentiation: '{heading_text}' -> '{new_heading}'")
                    cleaned_section["heading_text"] = new_heading
                    heading_text = new_heading

            # 3. Keyword Stuffing Repair (Only for non-protected sections)
            if heading_level == "H2" and section_type not in protected_sections and not is_pricing:
                normalized_h2 = self._normalize_heading_label(heading_text)
                if normalized_pk and normalized_pk in normalized_h2:
                    pk_count += 1
                    if pk_count > 1: # Keep first as anchor
                        replacement = entity_phrase or head_entity
                        new_text = heading_text.replace(primary_keyword, replacement).strip()
                        if new_text == heading_text:
                            new_text = f"تفاصيل {replacement} في الموقع"
                        logger.warning(f"[outline_repair] Rewrote stuffy heading: '{heading_text}' -> '{new_text}'")
                        cleaned_section["heading_text"] = new_text

            # 4. H3 Repairs
            subheadings = cleaned_section.get("subheadings", [])
            if isinstance(subheadings, list) and subheadings:
                valid_subs = []
                for sub in subheadings:
                    sub_text = str(sub).strip()
                    if section_type == "faq":
                        if not self._is_valid_faq_question(sub_text):
                            logger.warning(f"[outline_repair] Pruned non-question FAQ subheading: '{sub_text}'")
                            continue
                        if not self._faq_question_is_supported(sub_text, keyword_profile, support_blob):
                            logger.warning(f"[outline_repair] Pruned unsupported FAQ subheading: '{sub_text}'")
                            continue
                    else:
                        if self._foreign_entity_families(sub_text, keyword_profile):
                            logger.warning(f"[outline_repair] Pruned cross-entity subheading: '{sub_text}'")
                            continue
                        if self._subheading_is_too_granular(sub_text, stage):
                            logger.warning(f"[outline_repair] Pruned granular/weak subheading: '{sub_text}' in stage '{stage}'")
                            continue
                        if self._subheading_breaks_atomization(sub_text, stage):
                            logger.warning(f"[outline_repair] Pruned atomization-violating subheading: '{sub_text}'")
                            continue
                    valid_subs.append(sub)
                cleaned_section["subheadings"] = valid_subs

            cleaned_outline.append(cleaned_section)

        return cleaned_outline

    def _h3_supports_parent(
        self,
        parent_heading: str,
        child_heading: str,
        keyword_profile: Dict[str, Any],
    ) -> bool:
        parent_tokens = self._expanded_token_set(parent_heading)
        child_tokens = self._expanded_token_set(child_heading)
        
        # 1. Semantic overlap (Intersection of non-location tokens)
        location_tokens = set(keyword_profile.get("location_tokens", []))
        meaningful_parent = parent_tokens - location_tokens
        meaningful_child = child_tokens - location_tokens
        
        if meaningful_parent.intersection(meaningful_child):
            return True

        # 2. Bridge via head entity or shared intent
        head_entity = keyword_profile.get("head_entity", "")
        head_variants = self._expand_token_variants(head_entity) if head_entity else set()
        
        if head_variants.intersection(meaningful_parent) and head_variants.intersection(meaningful_child):
            return True

        # 3. Optional Topic alignment (Legal, Investment, etc.)
        parent_optional_topics = self._detect_optional_section_topics(parent_heading)
        child_optional_topics = self._detect_optional_section_topics(child_heading)
        if child_optional_topics and child_optional_topics.intersection(parent_optional_topics):
            return True

        # 4. Price/proof bridge: allow determinant-style H3s under price headings.
        parent_is_price_like = self._contains_any_signal(parent_heading, self.PRICE_HEADING_SIGNALS)
        if parent_is_price_like:
            if self._contains_any_signal(child_heading, self.PRICE_HEADING_SIGNALS):
                return True
            if self._contains_any_signal(child_heading, self.PRICE_FACTOR_SUPPORT_SIGNALS):
                return True

        # If it shares only location but not theme, it fails
        return False

    def _comparison_section_has_decision_angle(self, heading_text: str, subheadings: List[str]) -> bool:
        combined = " ".join([heading_text or ""] + [str(sub) for sub in (subheadings or [])])
        return self._contains_any_signal(combined, self.COMPARISON_HEADING_SIGNALS)

    def _subheading_looks_overpacked(self, subheading_text: str, parent_stage: str = "") -> bool:
        word_count = len(re.findall(r"\b\w+\b", subheading_text, re.UNICODE))
        if word_count < 6:
            return False

        group_markers = 0
        group_markers += len(re.findall(r"\sو\S+", subheading_text))
        group_markers += len(re.findall(r"\s(?:أو|او)\s", subheading_text))
        group_markers += subheading_text.count("،")
        group_markers += subheading_text.count(",")
        group_markers += subheading_text.count("/")

        if parent_stage == "comparison":
            return group_markers >= 2 or (group_markers >= 1 and word_count >= 10)

        return group_markers >= 2 or (group_markers >= 1 and word_count >= 9)

    def _subheading_breaks_atomization(self, subheading_text: str, parent_stage: str = "") -> bool:
        word_count = len(re.findall(r"\b\w+\b", subheading_text, re.UNICODE))
        if word_count < 6:
            return False

        group_markers = 0
        group_markers += len(re.findall(r"\s\u0648\S+", subheading_text))
        group_markers += len(re.findall(r"\s(?:\u0623\u0648|\u0627\u0648)\s", subheading_text))
        group_markers += subheading_text.count("\u060C")
        group_markers += subheading_text.count(",")
        group_markers += subheading_text.count("/")

        if parent_stage == "comparison":
            return group_markers >= 2 or (group_markers >= 1 and word_count >= 10)

        return group_markers >= 2 or (group_markers >= 1 and word_count >= 9)

    def _validate_commercial_heading_flow(self, outline: List[Dict[str, Any]], brand_name: str = "") -> List[str]:
        errors = []
        expected_flow = ["introduction", "offer", "features", "proof", "comparison", "process", "faq", "conclusion"]

        stage_positions: Dict[str, int] = {}
        first_visible_core_stage = ""
        differentiation_position: Optional[int] = None

        for idx, section in enumerate(outline):
            section_type = (section.get("section_type") or "").lower()
            heading_level = (section.get("heading_level") or "").upper()

            if section_type == "introduction":
                stage_positions.setdefault("introduction", idx)
                continue

            if heading_level != "H2":
                continue

            stage = self._commercial_flow_stage(section)
            if stage:
                stage_positions.setdefault(stage, idx)
                if stage == "differentiation":
                    differentiation_position = idx if differentiation_position is None else differentiation_position
                if stage not in {"faq", "conclusion", "differentiation"} and not first_visible_core_stage:
                    first_visible_core_stage = stage

        missing = [stage for stage in expected_flow if stage not in stage_positions]
        if missing:
            errors.append(
                f"COMMERCIAL_FLOW_MISSING: Commercial heading flow is missing required stages: {', '.join(missing)}."
            )

        if first_visible_core_stage and first_visible_core_stage != "offer":
            errors.append(
                "COMMERCIAL_FLOW_START_INVALID: The first visible core H2 in a commercial outline must be the offer/definition section."
            )

        ordered_positions = [stage_positions[stage] for stage in expected_flow if stage in stage_positions]
        if ordered_positions != sorted(ordered_positions):
            errors.append(
                "COMMERCIAL_FLOW_ORDER_INVALID: Commercial headings must follow the decision journey order: introduction -> offer -> features -> proof -> comparison -> process -> faq -> conclusion."
            )

        if brand_name:
            if differentiation_position is None:
                errors.append(
                    "BRAND_SECTION_MISSING: Commercial brand-led outlines should include one dedicated differentiation section."
                )
            else:
                features_position = stage_positions.get("features")
                proof_position = stage_positions.get("proof")
                comparison_position = stage_positions.get("comparison")
                if features_position is not None and differentiation_position <= features_position:
                    errors.append(
                        "BRAND_SECTION_ORDER_INVALID: Place the brand differentiation section after features, not before them."
                    )
                if proof_position is not None and differentiation_position <= proof_position:
                    errors.append(
                        "BRAND_SECTION_ORDER_INVALID: Place the brand differentiation section after proof, not before it."
                    )
                if comparison_position is not None and differentiation_position >= comparison_position:
                    errors.append(
                        "BRAND_SECTION_ORDER_INVALID: Place the brand differentiation section before the comparison section."
                    )

        return errors

    def _calculate_tone_intensity(self, text: str) -> float:
        """
        Calculates the sales pressure intensity score normalized per 100 words.
        Formula: (weighted_sales_score / word_count) * 100
        """
        if not text: return 0.0
        
        words = re.findall(r'\b\w+\b', text.lower())
        word_count = max(len(words), 1)
        
        total_score = 0.0
        text_lower = text.lower()
        text_normalized = self._normalize_arabic(text_lower)
        
        for marker, weight in self.SALES_MARKERS.items():
            pattern = re.escape(marker.lower())
            # For Arabic, check both raw and normalized
            count = len(re.findall(pattern, text_lower))
            if count == 0:
                count = len(re.findall(self._normalize_arabic(marker), text_normalized))
            
            total_score += (count * weight)
            
        intensity = (total_score / word_count) * 100
        return round(intensity, 2)

    def _check_topic_anchoring(self, text: str, entity_variants: List[str], location_variants: List[str], intent_variants: List[str]) -> Dict[str, Any]:
        """
        Checks for semantic anchoring of Subject, Context, and Intent.
        Returns a dictionary of found elements and a pass/fail status.
        """
        if not text: return {"pass": False, "found": []}
        
        text_norm = self._normalize_arabic(text)
        
        found_entity = any(self._normalize_arabic(v) in text_norm for v in entity_variants)
        found_location = any(self._normalize_arabic(v) in text_norm for v in location_variants)
        found_intent = any(self._normalize_arabic(v) in text_norm for v in intent_variants)
        
        # Hard fail if EITHER entity or location is missing
        is_anchored = found_entity and found_location
        
        return {
            "is_anchored": is_anchored,
            "has_entity": found_entity,
            "has_location": found_location,
            "has_intent": found_intent,
            "missing_hard": (not found_entity) or (not found_location)
        }

    def _check_geographic_drift(self, text: str, main_area: str, sub_area: str) -> Dict[str, Any]:
        """
        Checks for dominance of a sub-area over the main city-level area.
        Also checks if the sub-area hijacks the first sentence.
        """
        if not text or not main_area or not sub_area:
            return {"fail": False, "reason": ""}
            
        text_norm = self._normalize_arabic(text)
        main_norm = self._normalize_arabic(main_area)
        sub_norm = self._normalize_arabic(sub_area)
        
        # 1. First Sentence Check
        sentences = self.extract_sentences(text)
        if sentences:
            first_sentence_norm = self._normalize_arabic(sentences[0])
            if sub_norm in first_sentence_norm and main_norm not in first_sentence_norm:
                return {"fail": True, "reason": "CHILD_CONTEXT_HIJACK", "message": f"Sub-area '{sub_area}' established in the first sentence before the main city '{main_area}' context was anchored."}
        
        # 2. Mention Ratio Check
        main_count = text_norm.count(main_norm)
        sub_count = text_norm.count(sub_norm)
        
        if sub_count > main_count and sub_count > 1:
            return {"fail": True, "reason": "DOMINANCE_DRIFT", "message": f"Sub-area '{sub_area}' mentions ({sub_count}) exceed main area '{main_area}' mentions ({main_count}). The district is overshadowing the city context."}
            
        return {"fail": False}

    def validate_h1_length(self, h1: str) -> bool:
        """Enforces H1 length rules (55-75 chars) as per the framework."""
        return 55 <= len(h1) <= 75

    def validate_strategy_alignment(self, strategy: Dict[str, Any], primary_keyword: str, area: str) -> Tuple[bool, Optional[str]]:
        angle = strategy.get("primary_angle", "").lower()
        if primary_keyword.lower() not in angle:
            return False, "Primary keyword not reflected in strategy angle"

        if area and area.lower() not in strategy.get("market_angle","").lower():
            return False, "Local positioning missing"

        return True, None

    def validate_intent_from_serp(self, serp_analysis: dict) -> str:
        """Strengthened intent detection based on SERP structural intelligence."""
        structural = serp_analysis.get("structural_intelligence", {})

        page_type = structural.get("dominant_page_type", "")
        cta_pattern = structural.get("cta_intensity_pattern", "")
        pricing_ratio = structural.get("pricing_presence_ratio", 0)
        faq_ratio = structural.get("faq_presence_ratio", 0)

        commercial_score = 0
        informational_score = 0

        # Page type weight (strongest signal)
        if page_type in ["service", "homepage"]:
            commercial_score += 3
        elif page_type in ["guide", "comparison"]:
            informational_score += 3

        # Pricing presence
        if pricing_ratio > 0.4:
            commercial_score += 2

        # CTA intensity
        if cta_pattern in ["soft commercial", "aggressive"]:
            commercial_score += 2
        else:
            informational_score += 1

        # FAQ presence
        if faq_ratio > 0.4:
            informational_score += 1

        return "Commercial" if commercial_score >= informational_score else "Informational"

    def calculate_keyword_stats(self, markdown: str, keyword: str) -> Tuple[int, int, float]:
        """Calculates word count, keyword count, and keyword density."""
        if not markdown or not keyword:
            return 0, 0, 0.0

        # Remove markdown syntax
        clean_text = re.sub(r'[#>*`\-\[\]\(\)!]', '', markdown)

        words = re.findall(r'\b\w+\b', clean_text.lower())
        word_count = len(words)

        pattern = r'\b{}\b'.format(re.escape(keyword.lower()))
        keyword_count = len(re.findall(pattern, clean_text.lower()))

        density = 0.0
        if word_count > 0:
            density = (keyword_count / word_count) * 1000  # per 1000 words

        return word_count, keyword_count, round(density, 2)

    def check_competitor_mentions(self, text: str, prohibited_competitors: List[str]) -> Tuple[bool, Optional[str]]:
        """
        Checks if any prohibited competitor names appear in the generated content.
        """
        if not text or not prohibited_competitors:
            return False, None

        # Clean and normalize prohibited names
        clean_prohibited = [name.strip().lower() for name in prohibited_competitors if len(name) > 3]

        text_lower = text.lower()

        for competitor in clean_prohibited:
            # Check for exact matches with word boundaries for reliability
            pattern = rf'\b{re.escape(competitor)}\b'
            if re.search(pattern, text_lower):
                logger.warning(f"[Competitor Mention Alert] Found prohibited brand: '{competitor}'")
                return True, competitor

        return False, None

    def enforce_paragraph_structure(self, text: str) -> str:
        """
        Enforce max 3 sentences per paragraph WITHOUT breaking markdown tables/lists.
        """
        if not text:
            return text

        # 1) Protect table blocks first
        table_pattern = re.compile(r'((?:^\s*\|?.*\|.*\|?.*$\n?){2,})', re.MULTILINE)
        table_blocks = []

        def stash_table(m):
            table_blocks.append("\n".join([ln.rstrip() for ln in m.group(1).strip("\n").splitlines()]))
            return f"@@TABLE_BLOCK_{len(table_blocks)-1}@@"

        protected = table_pattern.sub(stash_table, text)

        # 2) Process normal paragraphs only
        paragraphs = [p.strip() for p in protected.split("\n\n") if p.strip()]
        fixed = []

        foreach_p_pattern = re.compile(r"^\d+\.\s")
        for p in paragraphs:
            if p.startswith("@@TABLE_BLOCK_") and p.endswith("@@"):
                fixed.append(p)
                continue

            if p.startswith("#") or p.startswith("- ") or p.startswith("* ") or foreach_p_pattern.match(p) or p.startswith("```"):
                fixed.append(p)
                continue

            # split long paragraph by sentences into chunks of max 3
            sentences = re.split(r'(?<=[.!؟])\s+', p)
            chunks = []
            for i in range(0, len(sentences), 3):
                chunk = " ".join(s for s in sentences[i:i+3] if s.strip()).strip()
                if chunk:
                    chunks.append(chunk)
            fixed.extend(chunks if chunks else [p])

        out = "\n\n".join(fixed)

        # 3) Restore tables
        for i, t in enumerate(table_blocks):
            out = out.replace(f"@@TABLE_BLOCK_{i}@@", t)

        return out

    def extract_sentences(self, text: str) -> List[str]:
        """Extracts sentences using regex that supports Arabic and English."""
        if not text:
            return []
        clean_text = re.sub(r'[#*`\-]', '', text)
        sentences = re.split(r'(?<=[.!؟])\s+', clean_text)
        return [s.strip() for s in sentences if s.strip()]

    def detect_repetition(self, text: str, global_used_phrases: List[str], threshold: int = 1) -> List[str]:
        """Detects repeated sentences within the text or against global memory."""
        if not text:
            return []

        sentences = self.extract_sentences(text)
        repeated = []

        # 1. Internal Repetition
        counts = Counter(sentences)
        internal_repeated = [s for s, c in counts.items() if c > threshold and len(s) > 30]
        repeated.extend(internal_repeated)

        # 2. Global Repetition
        for s in sentences:
            if len(s) > 40:
                if s in global_used_phrases:
                    repeated.append(s)

        return list(set(repeated))

    async def check_semantic_overlap(self, text: str, used_claims: List[str], threshold: float = 0.75) -> Tuple[bool, float, str]:
        """Checks if the new text has high semantic overlap with any previously used claims."""
        if not text or not used_claims:
            return False, 0.0, ""

        # --- High-Fidelity Semantic Mode ---
        if self.semantic_model:
            try:
                sentences = self.extract_sentences(text)
                # Filter for 'meaty' sentences that likely contain a unique claim/fact
                substantial_sentences = [s for s in sentences if len(s) > 45]

                if not substantial_sentences:
                    return False, 0.0, ""

                # Check each new substantial sentence against the global claim history
                for new_s in substantial_sentences:
                    # Optimized: Batch similarity check
                    scores = self.semantic_model.calculate_batch_similarity(new_s, used_claims)
                    max_score = max(scores) if scores else 0.0

                    if max_score > threshold:
                        overlapping_idx = scores.index(max_score)
                        overlapping_claim = used_claims[overlapping_idx]
                        logger.warning(f"[Semantic Overlap] High similarity ({max_score:.2f}) between current sentence and previous claim: '{overlapping_claim[:50]}...'")
                        return True, max_score, new_s

                return False, 0.0, ""
            except Exception as e:
                logger.error(f"Semantic overlap check failed, falling back to Lexical: {e}")

        # --- Basic Lexical Fallback (if no semantic model or batch failed) ---
        # We manually iterate and use our internal similarity engine (which has its own Jaccard fallback)
        sentences = self.extract_sentences(text)
        substantial_sentences = [s for s in sentences if len(s) > 40]

        for new_s in substantial_sentences:
            for claim in used_claims:
                score = self.calculate_similarity(new_s, claim)
                if score > threshold:
                    logger.warning(f"[Lexical Overlap Fallback] Similarity ({score:.2f}) detected: '{claim[:50]}...'")
                    return True, score, new_s

        return False, 0.0, ""

    def is_cta_link(self, text: str, is_html: bool = False) -> bool:
        """
        Detects if a link/button is a CTA based on a curated phrase/pattern list.
        Supports both Markdown and HTML structures.
        """
        if not text:
            return False
            
        # Curated CTA Patterns (Arabic + English)
        cta_patterns = [
            # Arabic CTAs
            r"تواصل\s+معنا", r"احجز\s+الآن", r"اطلب\s+عرض\s+سعر", r"اعرف\s+المزيد",
            r"اتصل\s+بنا", r"ابدأ\s+الآن", r"سجل\s+الآن", r"استشارة\s+مجانية",
            r"سجل\s+اهتمامك", r"تسوق\s+الآن",
            # English CTAs
            r"contact\s+us", r"book\s+now", r"get\s+started", r"request\s+a\s+quote",
            r"learn\s+more", r"call\s+us", r"register\s+now", r"free\s+consultation",
            r"shop\s+now"
        ]
        
        anchor_text = ""
        if is_html:
            # For HTML, we assume 'text' is the inner content of <a> or <button>
            anchor_text = text.lower().strip()
        else:
            # Extract the anchor text from [Anchor](URL)
            match = re.search(r"\[(.*?)\]", text)
            if not match:
                return False
            anchor_text = match.group(1).lower().strip()
        
        # Check against patterns
        for pattern in cta_patterns:
            if re.search(pattern, anchor_text, re.IGNORECASE):
                return True
        return False

    async def validate_section_output(self, content: str, section: Dict[str, Any], section_index: int = 0, total_sections: int = 0, area: str = "", blocked_domains: set = None, brand_url: str = "", content_type: str = "informational", **kwargs) -> Tuple[bool, List[str]]:
        """
        Hardens CTA validation based on the 'Earned CTA' and 'Structural Integrity' protocols.
        1. `commercial`: Transactional/Value terms (e.g., price, cost, ROI, fees, value, benefits).
        2. `geographic`: Localized/Spatial terms (e.g., neighborhoods, cities, landmarks, street names, specific locations).
        3. `entity`: Descriptive/Object terms (e.g., categories, types, versions, features, specifications, models).
        4. `action`: Engagement/Verb terms (e.g., choosing, comparing, finding, securing, starting, analyzing).
        Rules:
        1. No CTA in informational sections (ever).
        2. Permission != Requirement (cta_eligible check).
        3. Structural: No 1st paragraph, No post-heading.
        4. Quantitative: Max 1 CTA per section.
        5. Conclusion: Commercial must have CTA, Informational is optional soft.
        """
        errors = []
        if not content:
            return False, ["Content is empty"]

        heading_text = section.get('heading_text', 'Section')
        section_intent = section.get('section_intent', 'Informational').lower()
        cta_eligible = section.get('cta_eligible', False)
        section_type = (section.get('section_type') or '').lower().strip()
        valid_types = ['introduction', 'body', 'faq', 'conclusion']
        
        if not section_type or section_type not in valid_types:
            errors.append(f"SECTION_TYPE_CRITICAL_ERROR: section_type is missing or invalid ('{section_type}'). It MUST be explicitly defined as exactly one of: {valid_types}. Do NOT guess based on position.")
            return False, errors
            
        is_conclusion = section_type == 'conclusion'
        is_introduction = section_type == 'introduction'

        # 1. Structural Analysis
        paragraphs = [p.strip() for p in content.split("\n\n") if p.strip()]
        if not paragraphs:
             return False, ["No paragraphs found in content"]

        # Detect links and identify if they are CTAs
        def get_ctas_in_text(text):
            ctas = []
            # 1. Markdown Links [Text](URL) - Use pattern-based detection
            md_links = re.findall(r"\[.*?\]\(https?://.*?\)", text)
            ctas.extend([l for l in md_links if self.is_cta_link(l, is_html=False)])
            
            # 2. HTML <a> tags - Structural Detection (Explicit CTA blocks)
            # Find the full tag match, not just inner text
            html_links = re.findall(r"<a\b.*?>.*?</a>", text, re.IGNORECASE | re.DOTALL)
            ctas.extend(html_links)
            
            # 3. HTML <button> tags - Structural Detection (Explicit CTA blocks)
            buttons = re.findall(r"<button\b.*?>.*?</button>", text, re.IGNORECASE | re.DOTALL)
            ctas.extend(buttons)
            
            return ctas

        def has_cta(text):
            return len(get_ctas_in_text(text)) > 0

        # 2. Intent-Based & Eligibility Rules
        # - Section Intent Overrides Article Type (Golden Rule)
        if section_intent == 'informational' and not is_conclusion:
            if any(has_cta(p) for p in paragraphs):
                 errors.append(f"FORBIDDEN CTA: Informational section '{heading_text}' cannot contain promotional CTAs.")

        # - Conclusion Intent
        if is_conclusion:
            has_any_cta = any(has_cta(p) for p in paragraphs)
            if content_type == 'brand_commercial' and not has_any_cta:
                 # Signal the controller to re-generate or fix
                 errors.append("MISSING_CONCLUSION_CTA: Commercial conclusion must have a strong CTA.")
            elif content_type == 'informational':
                 # Count CTAs in informational conclusion
                 cta_count = sum(len(get_ctas_in_text(p)) for p in paragraphs)
                 if cta_count > 1:
                      errors.append("TOO_MANY_CTAs: Informational conclusion allows max 1 optional soft CTA.")

        # 3. Structural Constraints (Hard Rules)
        if is_introduction:
            # v4.0.2 Intent-Aware Paragraph Limit
            if content_type == 'brand_commercial':
                if not (2 <= len(paragraphs) <= 3):
                    errors.append(f"INTRO_STRUCTURE_VIOLATION: Commercial introduction '{heading_text}' must contain 2 to 3 distinct paragraphs.")
            else:
                if not (1 <= len(paragraphs) <= 2):
                    errors.append(f"INTRO_STRUCTURE_VIOLATION: Informational introduction '{heading_text}' must contain 1 to 2 distinct paragraphs.")
            
            if any(p.lstrip().startswith(("#", "###", "####")) for p in paragraphs):
                errors.append(f"INTRO_STRUCTURE_VIOLATION: Introduction '{heading_text}' must not contain nested headings.")
            if any("|" in p and "\n|" in p for p in paragraphs) or any(p.lstrip().startswith(("- ", "* ", "1. ")) for p in paragraphs):
                errors.append(f"INTRO_STRUCTURE_VIOLATION: Introduction '{heading_text}' must stay paragraph-only with no tables or lists.")

        if is_conclusion:
            if any(p.lstrip().startswith(("###", "####", "## ")) for p in paragraphs):
                errors.append(f"CONCLUSION_STRUCTURE_VIOLATION: Conclusion '{heading_text}' must not open new nested headings or sub-sections.")

        # - Paragraph density / readability guard
        paragraph_word_limit = 50 if (is_introduction or is_conclusion) else 60
        for idx, paragraph in enumerate(paragraphs, start=1):
            stripped = paragraph.lstrip()
            if stripped.startswith(("#", "|", "- ", "* ", "1. ", "2. ", "3. ")):
                continue

            word_count = len(re.findall(r"\S+", paragraph))
            if word_count > paragraph_word_limit:
                scope = "intro/conclusion" if (is_introduction or is_conclusion) else "body"
                errors.append(
                    f"READABILITY_VIOLATION: Paragraph {idx} in '{heading_text}' is too dense for {scope} content "
                    f"({word_count} words > {paragraph_word_limit}). Split it or convert enumerations into a list/table."
                )

        # - Audience language advisory (non-blocking, no blacklist)
        long_sentence_count = 0
        for paragraph in paragraphs:
            stripped = paragraph.lstrip()
            if stripped.startswith(("#", "|", "- ", "* ", "1. ", "2. ", "3. ")):
                continue
            for sentence in self.extract_sentences(paragraph):
                if len(re.findall(r"\S+", sentence)) > 28:
                    long_sentence_count += 1

        if long_sentence_count >= 3:
            errors.append(
                f"AUDIENCE_LANGUAGE_ADVISORY: Section '{heading_text}' contains several long or report-like sentences. "
                "Prefer simpler phrasing and explain specialized terms in plain language."
            )

        # - No CTA in first paragraph
        if paragraphs and has_cta(paragraphs[0]):
             errors.append(f"STRUCTURAL_VIOLATION: CTA detected in the first paragraph of '{heading_text}'.")

        # - No CTA immediately after a heading
        heading_indices = [i for i, p in enumerate(paragraphs) if p.startswith("#")]
        for idx in heading_indices:
            if idx + 1 < len(paragraphs) and has_cta(paragraphs[idx+1]):
                 errors.append(f"STRUCTURAL_VIOLATION: CTA detected immediately after a heading in '{heading_text}'.")

        # - Max 1 CTA per section
        total_section_ctas = sum(len(get_ctas_in_text(p)) for p in paragraphs)
        if total_section_ctas > 1:
             errors.append(f"QUANTITATIVE_VIOLATION: Section '{heading_text}' contains {total_section_ctas} CTAs. Max 1 is allowed.")

        # --- Original Logic (Paragraph Count, Keyword Density, Links) ---
        is_faq_or_pricing = section.get("section_type") in ["faq", "pricing"]
        if not is_faq_or_pricing and "|" not in content and "- " not in content:
            if len(paragraphs) < 2 or len(paragraphs) > 8:
                errors.append(f"Paragraph count is {len(paragraphs)}, must be 2-8")

        # --- PRIMARY KEYWORD RELEVANCE & DISTRIBUTION ---
        primary_kw = section.get("primary_keyword", "")
        requires_pk = section.get("requires_primary_keyword", False)

        if primary_kw and not is_faq_or_pricing:
            content_lower = content.lower()
            # Exact phrase count (ignoring case)
            exact_pattern = r'\b{}\b'.format(re.escape(primary_kw.lower()))
            exact_count = len(re.findall(exact_pattern, content_lower))

            # 1. Section Repetition Rule (Hard Cap)
            if exact_count > 1:
                errors.append(f"STUFFING_VIOLATION: Exact primary keyword '{primary_kw}' appears {exact_count} times in section '{heading_text}'. Max 1 is allowed per section.")

            # 2. Intro Semantic Anchoring Enforcement
            if is_introduction and paragraphs:
                first_para = paragraphs[0]
                
                # Enforce PRIMARY keyword placement with fuzzy/normalized matching (v4.0.3)
                # 1. Normalize strings
                pk_norm = self._normalize_arabic(primary_kw.lower())
                para_norm = self._normalize_arabic(first_para.lower())
                
                # 2. Extract core tokens
                stop_words = {"في", "من", "على", "عن", "الى", "الي", "ب", "ل", "ك", "ال"}
                pk_tokens = [w for w in re.findall(r'\b\w+\b', pk_norm) if w not in stop_words and len(w) > 2]
                
                # 3. Validation Rules
                if not pk_tokens:
                     has_pk = primary_kw.lower() in first_para.lower()
                else:
                     # Tokenized fuzzy match
                     matches = sum(1 for t in pk_tokens if t in para_norm)
                     # Accept if at least 70% of core tokens are present (allows slight variations)
                     has_pk = (matches / len(pk_tokens)) >= 0.7 if len(pk_tokens) > 1 else matches == 1
                     
                     # Detect forced/awkward insertion (e.g., placing the keyword isolated at the absolute start)
                     forced_pattern = r'^[\s\*\#\-\:]*{}(?:\s|$)'.format(re.escape(pk_tokens[0]))
                     if re.match(forced_pattern, para_norm):
                          errors.append(f"INTRO_PK_FORCED: Primary keyword '{primary_kw}' seems artificially forced at the very beginning (awkward/isolated). It MUST be naturally woven into a grammatical sentence.")
                
                if not has_pk:
                    errors.append(f"INTRO_PK_MISSING: The primary keyword '{primary_kw}' (or natural variation) must appear in the first paragraph of the introduction.")
                else:
                    # Soft warning for delayed placement
                    sentences = self.extract_sentences(first_para)
                    if len(sentences) > 2:
                        first_two_norm = self._normalize_arabic(" ".join(sentences[:2]).lower())
                        if pk_tokens and sum(1 for t in pk_tokens if t in first_two_norm) == 0:
                            logger.warning(f"INTRO_PK_DELAYED: Primary keyword '{primary_kw}' is present but delayed in section '{heading_text}'. Soft warning.")
                
                # Derive semantic sets from primary_kw and area
                entity_variants = [w for w in re.findall(r'\b\w+\b', primary_kw) if len(w) > 2][:2]
                location_variants = [area] if area else [primary_kw.split("في")[-1].strip()] if "في" in primary_kw else []
                intent_signals = ["بيع", "شراء", "حجز", "استكشاف", "بحث", "سعر", "اسعار", "تواصل"]
                
                anchor_results = self._check_topic_anchoring(
                    first_para, 
                    entity_variants=entity_variants, 
                    location_variants=location_variants, 
                    intent_variants=intent_signals
                )
                
                if anchor_results["missing_hard"]:
                    missing = []
                    if not anchor_results["has_entity"]: missing.append("Core Entity (Subject)")
                    if not anchor_results["has_location"]: missing.append("Main Location (Context)")
                    errors.append(f"INTRO_TOPIC_ANCHOR_MISSING: The introduction first paragraph fails to explicitly establish the article topic early. Missing: {', '.join(missing)}.")
                elif not anchor_results["has_intent"]:
                    # Intent is a weighted warning
                    errors.append(f"INTRO_INTENT_SIGNAL_WARNING: The introduction anchors the topic but lacks a clear 'Intent Signal' (e.g., buying, searching, or exploring).")

                # 3. Geographic Context Sentinel
                brief = section.get("brief", "").lower()
                sub_area_match = re.search(r'\b(التجمع|بيت الوطن|النرجس|الياسمين)\b', brief + " " + heading_text.lower())
                if sub_area_match and area:
                    sub_area = sub_area_match.group(1)
                    geo_check = self._check_geographic_drift(content, main_area=area, sub_area=sub_area)
                    if geo_check.get("fail"):
                        errors.append(f"INTRO_GEO_SCOPE_DRIFT: {geo_check['message']}")
                
                # 4. Intro Tone Profile Sentinel (v2.3)
                tone_profile = self._check_intro_tone_profile(content)
                if tone_profile.get("fail"):
                    errors.append(tone_profile["message"])

        # ===================================================
        # LAYER A: GLOBAL CLARITY ENFORCEMENT (ALL sections)
        # Applies to: introduction, body, faq, conclusion
        # ===================================================
        # 4. Tone Intensity Enforcement
        effective_intent = section_intent if not is_conclusion else "commercial"
        threshold = self.TONE_THRESHOLDS.get(effective_intent, 5.0)
        intensity_score = self._calculate_tone_intensity(content)
        if intensity_score > threshold:
            errors.append(f"TONE_INFLATION_HIGH: Section tone is overly sales-driven (Intensity {intensity_score} > Threshold {threshold}). Transition to a more helpful, expert-neighbor tone.")

        # 5. PLAIN_LANGUAGE_REQUIRED - Global Rule (v3.0)
        # Audits ALL sections for jargon density, cognitive difficulty, and corporate-speak.
        # This is NOT an intro-only rule. It applies to every section without exception.
        plain_lang_results = self._check_plain_language_compliance(content)
        if plain_lang_results.get("fail"):
            errors.append(f"PLAIN_LANGUAGE_REQUIRED: {plain_lang_results['message']}")

        # 3. Heading Relevance (For H2 sections assigned with PK) — independent of plain language
        heading_lvl = (section.get("heading_level") or "").upper()
        if heading_lvl == "H2" and requires_pk:
            heading_lower_check = heading_text.lower()
            has_pk_in_heading = re.search(exact_pattern, heading_lower_check)
            if not has_pk_in_heading:
                kw_comp = [w.lower() for w in re.findall(r'\b\w+\b', primary_kw) if len(w) > 2]
                found_comp = [w for w in kw_comp if w in heading_lower_check]
                if len(found_comp) / max(len(kw_comp), 1) < 0.5:
                    logger.warning(f"Heading relevance low for '{heading_text}'.")

        # 4. TOPIC_RELEVANCE_VIOLATION — NARROWED (v4.0.3)
        # Only fires when explicitly required by outline metadata OR for the introduction.
        # Does NOT accidentally enforce PK restatement on regular body sections.
        subtopic_alignment_required = section.get("requires_subtopic_alignment", False) or (
            requires_pk and is_introduction
        )
        if subtopic_alignment_required and requires_pk and exact_count == 0:
            kw_comp = [w.lower() for w in re.findall(r'\b\w+\b', primary_kw) if len(w) > 2]
            found_comp = [w for w in kw_comp if w in content_lower]
            coverage_ratio = len(found_comp) / max(len(kw_comp), 1)
            if coverage_ratio < 0.4:
                errors.append(
                    f"TOPIC_RELEVANCE_VIOLATION: Section '{heading_text}' is explicitly required "
                    f"to cover the subtopic '{primary_kw}' but lacks sufficient topic signals "
                    f"(coverage {coverage_ratio:.0%}). Ensure the section content clearly addresses this subtopic."
                )

        # ===================================================
        # LAYER B: INTRODUCTION-ONLY HOOK RULES
        # Applies to: section_type == "introduction" ONLY
        # Do NOT apply these to body, faq, or conclusion.
        # ===================================================
        if is_introduction and paragraphs:
            first_para = paragraphs[0]
            sentences = self.extract_sentences(first_para)
            first_sentence = sentences[0] if sentences else first_para

            # INTRO_HOOK_QUALITY_REQUIRED
            # Rejects three categories of bad openers:
            # 1. Flat/meta openers — "In this article we will..."
            # 2. Abstract openers — vague financial/investment framing with no human anchor
            # 3. Generic prestige openers — could apply to any article on any topic
            FLAT_OPENER_PATTERNS = [
                # Meta/self-referential patterns
                r'^في هذا (المقال|المحتوى|الدليل)',
                r'^(سنتحدث|سنتناول|سنشرح|سنستعرض) في هذا',
                r'^(هذا المقال|هذه المقالة)',
                r'^in this (article|guide|post|piece)',
                r'^this article (will|is about|covers|discusses)',
                r'^welcome to',
                r'^أهلاً وسهلاً',
                # Abstract prestige-heavy patterns (no real reader concern)
                r'^(الاستثمار العقاري|الاستثمار|التموضع|الريادة|سلطنة|المنظومة) (يعد|يُعد|يمثل|هو) (الخيار|الملاذ|الدرع|القاعدة)',
                r'^(يعد|تعد|يُعد|يمثل) (التموضع|الاستثمار|العقار|السوق|البروتوكول)',
                r'^(في ظل|في خضم) (تقلبات|التحولات|المنظومة الاستثنائية)',
                r'^(الأصول|الاستثمارات|البرامج) (العقارية|الآمنة|الاستراتيجية) (تظل|تبقى|هي)',
                # Generic prestige openers that fit any article
                r'^(يحلم|يسعى|يبحث) (الكثيرون|كثير) (عن|من) (امتلاك|الحصول)',
                r'^(اختيار|انتقاء) (المنزل|السكن|العقار) (المثالي|الصحيح|المناسب) (قرار|يُعد)',
            ]
            is_bad_opener = any(re.search(p, first_sentence, re.IGNORECASE) for p in FLAT_OPENER_PATTERNS)
            if is_bad_opener:
                errors.append(
                    f"INTRO_HOOK_QUALITY_REQUIRED: The opening line of '{heading_text}' is flat, generic, "
                    f"or abstract (investment-heavy). Replace it with a hook anchored to "
                    f"a specific, concrete reader concern or market reality."
                )

            # INTRO_HOOK_CLARITY_REQUIRED
            # Multi-signal clarity scoring (NOT word count alone).
            # A long but clear sentence passes. A short but abstract sentence fails.
            first_sentence_words = len(re.findall(r'\S+', first_sentence))

            # Signal 1: Sentence length (contributes to score but doesn't decide alone)
            length_score = 1 if first_sentence_words > 40 else (0.5 if first_sentence_words > 30 else 0)

            # Signal 2: Abstraction level — detect noun-heavy, process-free phrasing
            ABSTRACT_MARKERS = [
                r'\b(منظومة|منظومه|إطار|آلية|آليه|مسيرة|مسار|ركيزة|ركيزه|محور|منظور|توجه|استراتيجية|استراتيجيه|تموضع|ريادة|رياده|سلطنة|سلطنه)\b',
                r'\b(framework|paradigm|ecosystem|synergy|leverage|holistic|matrix|positioning|strategic|leadership)\b',
                r'\b(trajectory|momentum|landscape|dynamics|fundamentals|executive|elite|premium)\b',
            ]
            abstraction_hits = sum(1 for p in ABSTRACT_MARKERS if re.search(p, first_sentence, re.IGNORECASE))
            abstraction_score = min(abstraction_hits, 2)  # cap at 2

            # Signal 3: Directness — does the sentence directly address a reader action or situation?
            DIRECTNESS_ANCHORS = [
                r'\b(تبحث|تريد|تحتاج|تفكر|تخطط|هل)\b',
                r'\b(looking for|searching for|want to|need to|thinking about|planning)\b',
            ]
            has_directness = any(re.search(p, first_sentence, re.IGNORECASE) for p in DIRECTNESS_ANCHORS)
            directness_score = 0 if has_directness else 0.5  # penalty for lacking directness

            # Combined clarity score: higher = more unclear
            clarity_score = length_score + abstraction_score + directness_score
            if clarity_score >= 2.0:
                errors.append(
                    f"INTRO_HOOK_CLARITY_REQUIRED: The opening sentence in '{heading_text}' is unclear "
                    f"(clarity issue score: {clarity_score:.1f}/4). It may be too long ({first_sentence_words} words), "
                    f"too abstract, or too indirect. Rewrite it to be immediately understandable "
                    f"on first reading — direct, concrete, and human."
                )

        # Link Verification
        found_links = re.findall(r'\[.*?\]\((https?://.*?)\)', content)
        internal_domain = LinkManager.domain(brand_url) if brand_url else ""
        for link in found_links:
            link_domain = LinkManager.domain(link)
            if link_domain == internal_domain: continue
            if not await self._verify_external_link(link):
                errors.append(f"Broken external link: {link}")

        # --- Price analysis and payment-systems specific checks ---
        # --- Domain-Agnostic Metric & Formatting Checks ---
        heading_lower = heading_text.lower()
        
        # 1) Numeric Metric Enforcement (Price, Cost, Specs, Stats)
        # If the heading promises a quantifiable metric, we MUST find numbers in the content.
        try:
            metric_triggers = [
                "سعر", "price", "تكلفة", "cost", "قيمة", "value", 
                "راتب", "salary", "أجر", "wage", "رسوم", "fees",
                "مساحة", "area", "حجم", "size", "نسبة", "percentage",
                "عائد", "roi", "stats", "إحصائيات", "أرقام", "numbers",
                "نتيجة", "scores", "results", "points", "standing", "ranking"
            ]
            if any(kw in heading_lower for kw in metric_triggers):
                # Broad numeric check: Digit followed by some text (currency, unit, or % etc.)
                # Supports Arabic and English numbers/punctuation
                generic_numeric_pattern = re.compile(r"(\d[\d,\.\s]*)\s*[%/ \w\u0600-\u06FF]*", re.UNICODE)
                if not generic_numeric_pattern.search(content):
                    errors.append(f"METRIC_DATA_MISSING: Heading '{heading_text}' promises data/metrics, but the section contains no numeric values. Provide realistic estimates or ranges.")
        except re.error:
            pass

        # 2) Structural/Procedural Enforcement (Plans, Steps, Systems)
        # If the heading promises a system or a plan, it MUST use a visual format (Table or List).
        try:
            procedural_triggers = [
                "سداد", "payment", "خطة", "plan", "نظام", "system", 
                "خطوات", "steps", "طريقة", "method", "عملية", "process",
                "أنظمة", "schedules", "installment", "تقسيط",
                "جدول", "schedule", "ترتيب", "standing", "points"
            ]
            if any(k in heading_lower for k in procedural_triggers) or any(k in (section.get("section_type") or "").lower() for k in ["payment", "process", "workflow", "plans"]):
                # Require either a table or a list for procedural clarity
                has_table = bool(re.search(r"^\s*\|.+\n\s*\|[-: \t]+\n", content, re.MULTILINE))
                has_list = bool(re.search(r"^\s*[-*•]\s|^\s*\d+\.\s", content, re.MULTILINE))
                
                if not (has_table or has_list):
                    errors.append(f"VISUAL_FORMAT_MISSING: Heading '{heading_text}' implies a process or system. Use a Markdown Table or Bulleted List for clarity.")

                # 3) Specific Entity Bias Check (Dynamic)
                # If the article is general (not scoped to a brand), it shouldn't over-focus on one specific project/competitor.
                # We check for proper nouns (capitalized in EN) or specific phrases that appear too frequently.
                state_obj = kwargs.get("state") if isinstance(kwargs.get("state"), dict) else {}
                article_brand_name = (state_obj.get("brand_name") or "").lower()
                is_scoped = bool(state_obj.get("brand_url") or article_brand_name or str(state_obj.get("content_type", "")).lower() == "brand_commercial")
                
                if not is_scoped:
                    # Look for specific keywords that might indicate a limited focus (e.g., project-specific marketing)
                    known_bias_points = []
                    for bias in known_bias_points:
                        if bias in content.lower():
                            errors.append(f"POTENTIAL_BIAS: Section mentions specific entity '{bias}'. Ensure content remains general for the entire area/city.")
        except re.error:
            pass

        # 6. Structural Integrity Sentinel (v4.0.1)
        # Choose the ideal target format from section metadata, defaulting to 'compact_narrative'
        target_format = section.get("visual_format", "compact_narrative")
        structural_errors = self._check_structural_integrity(content, target_format, heading_text)
        errors.extend(structural_errors)

        return len(errors) == 0, errors

    async def _verify_external_link(self, url: str) -> bool:
        """Asynchronously checks if a URL is reachable and functional."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                response = await client.head(url)
                if response.status_code == 403:
                    logger.warning(f"External link {url} returned 403 (Forbidden). Likely bot block. Treating as valid but suspicious.")
                    return True
                if response.status_code >= 400:
                    response = await client.get(url)
                return 200 <= response.status_code < 400 or response.status_code == 403
        except Exception as e:
            logger.warning(f"Failed to verify external link {url}: {e}")
            return False

    def validate_local_seo(self, markdown: str, meta: dict, area: str) -> Tuple[bool, List[str]]:
        if not area:
            return True, []

        issues = []
        lower_md = markdown.lower()
        area_lower = area.lower()
        first_100 = " ".join(markdown.split()[:100]).lower()

        if area_lower not in first_100:
            issues.append("Local area missing in first 100 words")
        if area_lower not in lower_md.split("\n")[0]:
            issues.append("Local area missing in H1")
        if area_lower not in meta.get("meta_title", "").lower():
            issues.append("Local area missing in Meta Title")
        if area_lower not in meta.get("meta_description", "").lower():
            issues.append("Local area missing in Meta Description")

        return len(issues) == 0, issues

    def validate_content_angle(self, markdown: str, strategy: dict) -> Tuple[bool, Optional[str]]:
        angle = strategy.get("primary_angle")
        if not angle:
            return True, None

        h2s = re.findall(r'^##\s+(.*)', markdown, re.MULTILINE)
        if not h2s:
            return False, "No H2 found"
        if angle.lower() not in h2s[0].lower():
            return False, "Content angle not reflected in first H2"
        return True, None

    # --- SEMANTIC TOPIC ARCHITECTURE (PHASE 1.5) ---

    def validate_semantic_coverage(self, markdown: str, semantic_metadata: Dict[str, Any], outline: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Lightweight Semantic Validation Helper:
        Detects topical gaps and under-covered sections based on the Semantic Plan.
        Focuses on 'Topical Signals' rather than mechanical keyword matching.
        """
        if not markdown or not semantic_metadata:
            return {
                "covered_entities": [],
                "covered_concepts": [],
                "missing_concepts": [],
                "under_covered_sections": [],
                "intent_coverage": {},
                "semantic_coverage_ok": True
            }

        entities = semantic_metadata.get("semantic_entities", [])
        concepts = semantic_metadata.get("semantic_concepts", [])
        intent_clusters = semantic_metadata.get("intent_clusters", [])

        # Normalize total markdown for global signal checking
        content_lower = markdown.lower()

        def has_topic_signal(text: str, label: str, threshold: float = 0.5) -> bool:
            """Lightweight topical-signal check using exact phrase first, then major token presence."""
            if not text or not label:
                return False

            text_lower = text.lower()
            label_lower = label.lower()
            if label_lower in text_lower:
                return True

            tokens = [t for t in re.findall(r'\b\w+\b', label_lower) if len(t) > 3]
            if not tokens:
                return False

            matches = sum(1 for token in tokens if re.search(rf'\b{re.escape(token)}\b', text_lower))
            return (matches / len(tokens)) >= threshold
        
        # 1. Entity Coverage (Topical Signals)
        # Check whether the article shows clear topical signals covering the expected entities.
        covered_entities = []
        for ent in entities:
            if has_topic_signal(content_lower, ent, threshold=0.5):
                covered_entities.append(ent)

        # 2. Concept Coverage (Meaningful support)
        # Check whether the article meaningfully covers expected concepts using section content.
        covered_concepts = []
        missing_concepts = []
        
        for concept in concepts:
            if has_topic_signal(content_lower, concept, threshold=0.5):
                covered_concepts.append(concept)
            else:
                missing_concepts.append(concept)

        # 3. Under-covered Sections
        # Identify sections that are 'under-covered relative to the expected concept map'.
        # We look for sections whose content does not strongly support their assigned goal/angle/concept.
        under_covered_sections = []
        markdown_sections = [s.strip() for s in markdown.split("\n\n## ") if s.strip()]

        if outline and markdown_sections:
            h2_outline = [s for s in outline if (s.get("heading_level") or "").upper() == "H2"]
            for i, section_meta in enumerate(h2_outline[:len(markdown_sections)]):
                section_text = markdown_sections[i]
                heading = section_meta.get("heading_text", f"Section {i+1}")
                support_labels = [
                    heading,
                    section_meta.get("content_goal", ""),
                    section_meta.get("content_angle", ""),
                    section_meta.get("localized_angle", "")
                ]
                support_labels = [label for label in support_labels if label]
                has_support = any(has_topic_signal(section_text, label, threshold=0.4) for label in support_labels)

                if not has_support:
                    under_covered_sections.append({
                        "heading": heading,
                        "status": "under-supported relative to planned section goal"
                    })
        else:
            for i, section_text in enumerate(markdown_sections):
                heading_match = re.match(r'^(.*?)\n', section_text)
                heading = heading_match.group(1).strip() if heading_match else f"Section {i+1}"
                if len(re.findall(r'\b\w+\b', section_text)) < 80:
                    under_covered_sections.append({
                        "heading": heading,
                        "status": "under-supported relative to article semantic plan"
                    })

        # 4. Intent Coverage (Alignment Check)
        # Verify alignment between section metadata and the overall semantic plan.
        intent_stats = {
            "informational": False,
            "commercial": False,
            "comparison": False,
            "problem_solving": False
        }
        
        if outline:
            for s in outline:
                s_intent = s.get("section_intent", "").lower()
                s_type = s.get("section_type", "").lower()
                
                if "info" in s_intent: intent_stats["informational"] = True
                if "comm" in s_intent: intent_stats["commercial"] = True
                if "comp" in s_type or "comp" in s_intent: intent_stats["comparison"] = True
                if s_type in ["process", "common_mistakes", "troubleshooting"] or "implementation" in (s.get("decision_layer", "").lower()):
                    intent_stats["problem_solving"] = True

        for cluster in intent_clusters:
            cluster_lower = str(cluster).lower()
            if "problem" in cluster_lower or "solve" in cluster_lower:
                intent_stats["problem_solving"] = intent_stats["problem_solving"] or bool(
                    re.search(r'\b(how|problem|avoid|fix|improve|حل|مشكلة|تجنب|تحسين)\b', content_lower)
                )
            if "info" in cluster_lower:
                intent_stats["informational"] = intent_stats["informational"] or bool(
                    re.search(r'\b(what|how|why|what is|guide|دليل|ما هو|كيف|لماذا)\b', content_lower)
                )
            if "commercial" in cluster_lower or "decision" in cluster_lower:
                intent_stats["commercial"] = intent_stats["commercial"] or bool(
                    re.search(r'\b(compare|choose|pricing|buy|request|قارن|اختر|سعر|شراء)\b', content_lower)
                )
            if "comparison" in cluster_lower:
                intent_stats["comparison"] = intent_stats["comparison"] or bool(
                    re.search(r'\b(compare|vs|versus|comparison|مقارنة|مقابل)\b', content_lower)
                )
            if "metrics" in cluster_lower:
                intent_stats["metrics"] = intent_stats["metrics"] or bool(
                    re.search(r'\b(result|score|standing|stats|numbers|إحصائيات|أرقام|نتيجة)\b', content_lower)
                )

        return {
            "covered_entities": covered_entities,
            "covered_concepts": covered_concepts,
            "missing_concepts": missing_concepts,
            "under_covered_sections": under_covered_sections,
            "intent_coverage": intent_stats,
            "semantic_coverage_ok": len(missing_concepts) <= (len(concepts) // 3) # Advisory: PASS if at least 66% covered
        }

    def validate_paragraph_structure(self, text: str) -> bool:
        if not text:
            return True
        paragraphs = [p.strip() for p in text.split("\n\n") if len(p.strip()) > 30]
        for p in paragraphs:
            if p.startswith("|") or p.startswith("- ") or p.startswith("* ") or p.startswith("#"):
                continue
            sentences = self.extract_sentences(p)
            if len(sentences) > 4:
                return False
        return True

    def validate_final_cta(self, text: str, language: str) -> bool:
        """
        Checks if the final CTA exists and if it is structurally complete.
        Uses the curated pattern list via is_cta_link for consistency.
        """
        if not text:
            return False

        clean_text = text.strip()

        # 1. Structural Completeness
        if clean_text.endswith(("[", "(", "!", "*", "_")):
             return False
        if clean_text.count("[") != clean_text.count("]") or clean_text.count("(") != clean_text.count(")"):
             return False

        # 2. Pattern-Based CTA Detection (Consistent with is_cta_link)
        # We need to be aware that the article might end with a large FAQ section,
        # which can push the conclusion's CTA out of the final characters.
        # Let's find the content after the last main heading (likely the conclusion)
        # or simply search the last 2000 characters to be safe.
        
        # Split by H2 to try and find the last major section
        sections = re.split(r'\n##\s+', clean_text)
        last_section = sections[-1] if sections else clean_text
        
        # If the last section is too short (e.g. just a heading), maybe look at the last 2000 chars anyway
        search_chunk = last_section
        if len(search_chunk) < 500:
            search_chunk = clean_text[-2000:]
            
        # Look for markdown links in the target chunk
        links = re.findall(r"\[.*?\]\(https?://.*?\)", search_chunk)
        
        # If no links found in the last section, try the last 2000 characters as a fallback
        if not links and search_chunk != last_section:
            fallback_chunk = clean_text[-2000:]
            links = re.findall(r"\[.*?\]\(https?://.*?\)", fallback_chunk)
            
        return any(self.is_cta_link(l) for l in links)

    def repair_cutoff_cta(self, text: str) -> str:
        """Mechanically repairs or prunes a cutoff CTA to avoid broken markdown/fragmented user experience."""
        if not text:
            return text

        lines = text.strip().split("\n")
        if not lines:
            return text

        last_line = lines[-1].strip()

        # Check for obvious cutoff indicators
        is_cutoff = False
        if last_line.endswith(("[", "(", "!", "*", "_")):
            is_cutoff = True

        # Check for unclosed brackets
        if last_line.count("[") > last_line.count("]"):
             is_cutoff = True
        elif last_line.count("(") > last_line.count(")"):
             is_cutoff = True

        if is_cutoff:
            logger.warning(f"Repairing Cut-off CTA detected in last line: '{last_line[:30]}...'")
            # If it's a small fragment, just drop the line.
            # If it's just a missing bracket, we could try adding it, but dropping is safer for UX.
            return "\n".join(lines[:-1]).strip()

        return text.strip()

    # --- Outline Structure & Quality ---

    REQUIRED_STRUCTURE_BY_TYPE = {
        "brand_commercial": {
            "mandatory": {
                "introduction", "what_is", "key_features", "why_choose_us",
                "proof", "process", "faq", "conclusion"
            }
        },
        "informational": {
            "mandatory": {
                "introduction", "definition", "key_benefits", "core",
                "examples_or_use_cases", "common_mistakes", "faq", "conclusion"
            }
        },
        "comparison": {
            "mandatory": {
                "introduction", "comparison", "criteria", "pros_cons_each",
                "who_should_choose_what", "faq", "conclusion"
            }
        }
    }

    REQUIRED_COVERAGE_BY_TYPE = {
        "informational": {
            "intro_setup": {"section_types": {"introduction"}},
            "definition": {"section_types": {"definition", "what_is"}},
            "why_it_matters": {"section_types": {"key_benefits", "why_it_matters"}},
            "main_subtopics": {"section_types": {"core", "how_to", "process", "steps"}},
            "examples_or_tips": {"section_types": {"examples_or_use_cases", "tips", "practical_tips"}},
            "common_mistakes": {"section_types": {"common_mistakes", "warnings", "pitfalls"}},
            "faq": {"section_types": {"faq"}},
            "conclusion": {"section_types": {"conclusion"}},
        },
        "brand_commercial": {
            "problem_aware_intro": {"section_types": {"introduction"}},
            "offer_clarity": {"section_types": {"what_is", "definition", "offer_overview"}},
            "features_or_included": {"section_types": {"key_features", "features", "included"}},
            "differentiators": {"section_types": {"why_choose_us", "differentiators", "usp"}},
            "proof": {"section_types": {"proof", "case_study", "authority"}},
            "process": {"section_types": {"process", "how_it_works", "implementation"}},
            "objection_faq": {"section_types": {"faq"}},
            "comparison_utility": {"section_types": {"comparison", "pricing", "tiers", "alternatives", "comparison_utility"}},
            "decisive_close": {"section_types": {"conclusion"}},
        },
        "comparison": {
            "intro_setup": {"section_types": {"introduction"}},
            "comparison_frame": {"section_types": {"comparison", "criteria"}},
            "pros_cons": {"section_types": {"pros_cons_each", "pros_cons"}},
            "decision_guidance": {"section_types": {"who_should_choose_what", "recommendation"}},
            "faq": {"section_types": {"faq"}},
            "conclusion": {"section_types": {"conclusion"}},
        }
    }

    def _section_text_blob(self, section: Dict[str, Any]) -> str:
        return " ".join(
            str(section.get(k, "") or "")
            for k in ["heading_text", "content_goal", "content_angle", "localized_angle", "decision_layer"]
        ).lower()

    def evaluate_outline_coverage(self, outline: List[Dict[str, Any]], content_type: str) -> Dict[str, Any]:
        coverage_rules = self.REQUIRED_COVERAGE_BY_TYPE.get(content_type, {})
        results = {
            "covered": [],
            "missing": [],
            "matched_sections": {}
        }
        if not coverage_rules:
            return results

        normalized_sections = []
        for sec in outline:
            normalized_sections.append({
                "section": sec,
                "section_type": (sec.get("section_type") or "").lower().strip(),
                "text_blob": self._section_text_blob(sec)
            })

        for concept, rules in coverage_rules.items():
            aliases = {a.lower().strip() for a in rules.get("section_types", set())}
            matched = []
            for item in normalized_sections:
                sec_type = item["section_type"]
                blob = item["text_blob"]
                if sec_type in aliases or any(alias in blob for alias in aliases):
                    matched.append(item["section"]["heading_text"])

            if matched:
                results["covered"].append(concept)
                results["matched_sections"][concept] = matched
            else:
                results["missing"].append(concept)

        return results

    def enforce_outline_structure(self, outline: List[Dict[str, Any]], content_type: str) -> List[Dict[str, Any]]:
        present_types = {(s.get("section_type") or "").lower().strip() for s in outline}
        rules = self.REQUIRED_STRUCTURE_BY_TYPE.get(content_type)
        if rules:
            required = rules.get("mandatory", set())
            missing = required - present_types
            if missing:
                logger.error(f"[outline_validate] Missing mandatory sections for {content_type}: {missing}")

        coverage = self.evaluate_outline_coverage(outline, content_type)
        if coverage.get("missing"):
            logger.error(f"[outline_validate] Missing required topic coverage for {content_type}: {coverage['missing']}")

        for i, sec in enumerate(outline):
            if not sec.get("section_id"):
                sec["section_id"] = f"sec_{i+1:02d}"
        return outline

    def validate_article_cta_budget(self, full_markdown: str, word_count: int, content_type: str) -> Tuple[bool, Optional[str]]:
        """
        Enforces article-level dynamic CTA cap logic.
        max_ctas = min(4, ceil(word_count / 400))
        """
        if not full_markdown:
             return True, None

        # Detect all CTAs (HTML or Markdown links)
        cta_count = len(re.findall(r'<a\b|<button\b|\[.*?\]\(https?://', full_markdown))

        # Calculate dynamic cap
        dynamic_cap = min(4, int(-(word_count // -400))) # ceil(word_count/400)

        if cta_count > dynamic_cap:
             return False, f"Article total CTAs ({cta_count}) exceeds dynamic cap ({dynamic_cap}) for {word_count} words."

        return True, None

    def enforce_cta_budget(self, outline: List[Dict[str, Any]], article_size: str) -> List[Dict[str, Any]]:
        """Legacy placeholder: Article-level CTA budget is now handled by ValidationService.validate_article_cta_budget."""
        return outline

    def validate_heading_outline_quality(
        self,
        outline: List[Dict[str, Any]],
        content_type: str = "",
        area: str = "",
        primary_keyword: str = "",
        brand_name: str = "",
        content_strategy: Optional[Dict[str, Any]] = None,
        seo_intelligence: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        errors = []

        if not outline:
            return ["HEADING_OUTLINE_EMPTY: No outline sections were returned."]

        intro_positions = []
        faq_positions = []
        conclusion_positions = []
        normalized_h2 = set()

        visible_h2_sections = [
            section for section in outline
            if (section.get("heading_level") or "").upper() == "H2"
            and (section.get("section_type") or "").lower() != "introduction"
        ]
        core_h2_sections = [
            section for section in outline
            if (section.get("heading_level") or "").upper() == "H2"
            and (section.get("section_type") or "").lower() not in {"introduction", "faq", "conclusion"}
        ]
        keyword_profile = self._derive_keyword_profile(primary_keyword, area)
        support_blob = self._build_outline_support_blob(
            primary_keyword=primary_keyword,
            content_strategy=content_strategy,
            seo_intelligence=seo_intelligence,
        )

        if len(visible_h2_sections) < 4:
            errors.append(
                f"HEADING_OUTLINE_TOO_THIN: Only {len(visible_h2_sections)} visible H2 sections found. Generate at least 4 reader-facing H2 sections."
            )

        if primary_keyword and core_h2_sections:
            first_core_h2 = core_h2_sections[0].get("heading_text", "")
            if not self._heading_contains_keyword_anchor(first_core_h2, keyword_profile):
                errors.append(
                    f"PRIMARY_KEYWORD_ANCHOR_MISSING: The first visible core H2 '{first_core_h2}' must contain the full primary keyword or its closest natural full-intent form."
                )

            normalized_primary_keyword = keyword_profile.get("normalized_keyword", "")
            exact_pk_repeats = sum(
                1 for section in core_h2_sections
                if normalized_primary_keyword and f" {normalized_primary_keyword} " in f" {self._normalize_heading_label(section.get('heading_text', ''))} "
            )
            if exact_pk_repeats == 3:
                errors.append(
                    f"WARNING_PRIMARY_KEYWORD_STUFFING: The full primary keyword appears in {exact_pk_repeats} core H2 headings. Keep one clear anchor H2 and avoid repeating the exact full keyword everywhere."
                )
            elif exact_pk_repeats > 3:
                errors.append(
                    f"PRIMARY_KEYWORD_STUFFING_RISK: The full primary keyword appears in {exact_pk_repeats} core H2 headings. Keep one clear anchor H2 and avoid repeating the exact full keyword everywhere."
                )

        for idx, section in enumerate(outline):
            heading_text = (section.get("heading_text") or "").strip()
            heading_level = (section.get("heading_level") or "").upper()
            section_type = (section.get("section_type") or "").lower()
            subheadings = section.get("subheadings", [])
            stage = self._commercial_flow_stage(section)

            if section_type == "introduction":
                intro_positions.append(idx)
                if heading_level in {"H2", "H3"}:
                    errors.append(
                        "INTRO_HEADING_FORBIDDEN: The introduction must be an unheaded opening block, not an H2 or H3 heading."
                    )
                elif heading_level and heading_level not in {"INTRO", "OPENING", "NONE"}:
                    errors.append(
                        f"INVALID_HEADING_LEVEL: Introduction section '{heading_text or f'Section {idx + 1}'}' must use INTRO-style metadata, not '{heading_level}'."
                    )
            elif section_type == "faq":
                faq_positions.append(idx)
            elif section_type == "conclusion":
                conclusion_positions.append(idx)
            elif heading_level not in {"H2", "H3"}:
                errors.append(
                    f"INVALID_HEADING_LEVEL: Section '{heading_text or f'Section {idx + 1}'}' must use H2 or H3."
                )

            if not heading_text:
                errors.append(f"EMPTY_HEADING_TEXT: Section {idx + 1} is missing heading_text.")

            if section_type != "introduction":
                normalized_text = self._normalize_heading_label(heading_text)
                word_count = len(re.findall(r"\b\w+\b", heading_text, re.UNICODE))

                if self._is_generic_visible_heading(heading_text):
                    errors.append(
                        f"GENERIC_HEADING_LABEL: '{heading_text}' is too generic. Use a specific reader-facing promise instead."
                    )

                if word_count < 2:
                    errors.append(
                        f"HEADING_TOO_SHORT: '{heading_text}' is too short to communicate a clear section promise."
                    )

                if word_count > 14:
                    errors.append(
                        f"HEADING_TOO_LONG: '{heading_text}' is too long. Tighten it into a scannable heading."
                    )

                if heading_level == "H2":
                    if normalized_text in normalized_h2:
                        errors.append(
                            f"DUPLICATE_H2_HEADING: '{heading_text}' repeats an existing H2 angle."
                        )
                    normalized_h2.add(normalized_text)

                    if section_type not in {"introduction", "faq", "conclusion"}:
                        if primary_keyword and not self._heading_preserves_entity_focus(heading_text, keyword_profile):
                            errors.append(
                                f"HEAD_ENTITY_SCOPE_DRIFT: Heading '{heading_text}' drifts away from the main entity in the primary keyword. Keep the original subject explicit in core H2s."
                            )
                        elif primary_keyword and self._foreign_entity_families(heading_text, keyword_profile):
                            errors.append(
                                f"ENTITY_FAMILY_DRIFT: Heading '{heading_text}' introduces property types outside the main entity in the primary keyword. Keep the structure strictly focused on the original entity only."
                            )

                        unsupported_optional_topics = [
                            topic for topic in self._detect_optional_section_topics(heading_text)
                            if not self._optional_topic_is_justified(topic, support_blob)
                        ]
                        if stage == "comparison":
                            unsupported_optional_topics = [
                                topic for topic in unsupported_optional_topics
                                if topic != "financing_payment"
                            ]
                        if unsupported_optional_topics:
                            errors.append(
                                f"OPTIONAL_SECTION_NOT_JUSTIFIED: '{heading_text}' introduces {', '.join(unsupported_optional_topics)} without clear support from the keyword, SERP/PAA, or strategy."
                            )

                        if brand_name and self._brand_appears_in_heading(heading_text, brand_name) and not self._brand_heading_allowed(section_type):
                            errors.append(
                                f"BRAND_HEADING_LEAKAGE: Heading '{heading_text}' should not contain brand framing in this section."
                            )

                        if stage == "differentiation" and brand_name:
                            if not self._heading_contains_exact_brand_name(heading_text, brand_name):
                                errors.append(
                                    f"WARNING_BRAND_DIFFERENTIATION_NAME_MISSING: Differentiation heading '{heading_text}' must use the full official brand name '{brand_name}'."
                                )
                            if not self._heading_contains_keyword_anchor(heading_text, keyword_profile):
                                errors.append(
                                    f"WARNING_BRAND_KEYWORD_INTENT_MISSING: Differentiation heading '{heading_text}' must stay directly connected to the primary keyword intent."
                                )

                        detected_topics = self._detect_optional_section_topics(heading_text)

                        if stage == "features" and detected_topics.intersection({"investment", "financing_payment", "legal"}):
                            errors.append(
                                f"FEATURES_SECTION_DRIFT: '{heading_text}' reads like financing, legal, or investment framing. Keep features focused on the apartment itself."
                            )

                        if stage == "proof" and "investment" in detected_topics:
                            errors.append(
                                f"PROOF_SECTION_DRIFT: '{heading_text}' shifts into investment framing. Keep proof focused on apartment prices, demand, or buyer-facing market validation."
                            )

                        if stage in {"proof", "pricing"} and self._contains_any_signal(heading_text, self.PRICE_HEADING_SIGNALS) and not self._heading_contains_keyword_anchor(heading_text, keyword_profile):
                            errors.append(
                                f"PRICE_KEYWORD_INTENT_MISSING: '{heading_text}' is a pricing/proof heading and must preserve the product entity, sale intent, and location from the primary keyword in a natural commercial phrasing."
                            )
                        elif stage == "comparison" and self._contains_any_signal(heading_text, self.PRICE_HEADING_SIGNALS) and not self._heading_preserves_entity_focus(heading_text, keyword_profile):
                            errors.append(
                                f"PRICE_SCOPE_DRIFT: '{heading_text}' mentions pricing without staying anchored to the product entity '{keyword_profile.get('head_entity')}' in the primary keyword. In this section, the entity MUST be explicit."
                            )
                        elif self._contains_any_signal(heading_text, self.PRICE_HEADING_SIGNALS) and not self._heading_preserves_entity_focus(heading_text, keyword_profile):
                            # Soft warning for other sections
                            logger.warning(f"PRICE_SCOPE_DRIFT_WARNING: '{heading_text}' lacks entity anchoring, but it's in a non-core section.")

                        if stage == "process" and "legal" in detected_topics and not self._optional_topic_is_justified("legal", support_blob):
                            errors.append(
                                f"PROCESS_SECTION_DRIFT: '{heading_text}' introduces legal validation even though the heading flow should stay focused on the buying journey."
                            )

                        if stage == "comparison" and not self._comparison_section_has_decision_angle(heading_text, subheadings):
                            errors.append(
                                f"COMPARISON_SECTION_WEAK: '{heading_text}' should frame a real decision comparison such as ready vs under-construction, area differences, or payment differences."
                            )

                    if section_type == "faq" and brand_name and self._brand_appears_in_heading(heading_text, brand_name):
                        errors.append(
                            f"BRAND_HEADING_LEAKAGE: FAQ heading '{heading_text}' should not contain brand framing."
                        )

            if not isinstance(subheadings, list):
                errors.append(
                    f"INVALID_SUBHEADINGS: Section '{heading_text or f'Section {idx + 1}'}' must return subheadings as a list."
                )
                continue

            if section_type == "introduction" and subheadings:
                errors.append("INTRO_SUBHEADINGS_FORBIDDEN: The introduction opening block must not have H3 subheadings.")

            normalized_subs = set()
            parent_normalized = self._normalize_heading_label(heading_text)
            for subheading in subheadings:
                subheading_text = str(subheading).strip()
                normalized_sub = self._normalize_heading_label(subheading_text)
                sub_word_count = len(re.findall(r"\b\w+\b", subheading_text, re.UNICODE))

                if not subheading_text:
                    errors.append(
                        f"EMPTY_SUBHEADING: Section '{heading_text}' contains an empty H3 string."
                    )
                    continue

                if self._is_generic_visible_heading(subheading_text):
                    errors.append(
                        f"GENERIC_SUBHEADING_LABEL: '{subheading_text}' is too generic. Make the H3 specific."
                    )

                if sub_word_count < 2:
                    errors.append(
                        f"SUBHEADING_TOO_SHORT: '{subheading_text}' is too short to be useful."
                    )

                if normalized_sub == parent_normalized:
                    errors.append(
                        f"SUBHEADING_DUPLICATES_PARENT: '{subheading_text}' repeats its parent H2 '{heading_text}'."
                    )

                if normalized_sub in normalized_subs:
                    errors.append(
                        f"DUPLICATE_SUBHEADING: '{subheading_text}' is repeated inside '{heading_text}'."
                    )
                normalized_subs.add(normalized_sub)

                unsupported_child_topics = [
                    topic for topic in self._detect_optional_section_topics(subheading_text)
                    if not self._optional_topic_is_justified(topic, support_blob)
                ]
                if self._commercial_flow_stage(section) == "comparison":
                    unsupported_child_topics = [
                        topic for topic in unsupported_child_topics
                        if topic != "financing_payment"
                    ]
                if unsupported_child_topics:
                    errors.append(
                        f"OPTIONAL_SUBHEADING_NOT_JUSTIFIED: '{subheading_text}' introduces {', '.join(unsupported_child_topics)} without clear support from the keyword, SERP/PAA, or strategy."
                    )

                if brand_name and self._brand_appears_in_heading(subheading_text, brand_name) and not self._brand_heading_allowed(section_type):
                    errors.append(
                        f"BRAND_SUBHEADING_LEAKAGE: Subheading '{subheading_text}' should not contain brand framing in '{heading_text}'."
                    )

                if primary_keyword and self._foreign_entity_families(subheading_text, keyword_profile):
                    errors.append(
                        f"H3_ENTITY_FAMILY_DRIFT: Subheading '{subheading_text}' introduces a different property type than the main keyword entity. Keep H3s strictly aligned with the original entity only."
                    )

                if self._subheading_breaks_atomization(subheading_text, stage):
                    errors.append(
                        f"WARNING_H3_ATOMIZATION_VIOLATION: Subheading '{subheading_text}' combines multiple areas, segments, or ideas. Split it into one clear idea per H3."
                    )

                if self._subheading_is_too_granular(subheading_text, stage):
                    errors.append(
                        f"H3_QUALITY_GRANULARITY_VIOLATION: Subheading '{subheading_text}' describes a paragraph-level detail (finishing, layout, etc.). Use H3s only for standalone buckets like unit types, areas, or segments."
                    )

                if section_type == "faq":
                    if not self._is_valid_faq_question(subheading_text):
                        errors.append(
                            f"FAQ_NON_QUESTION: FAQ subheading '{subheading_text}' must be formatted as a buyer question (e.g., starting with ما, كيف, هل, كم)."
                        )
                    elif not self._faq_question_is_supported(subheading_text, keyword_profile, support_blob):
                        errors.append(
                            f"FAQ_UNSUPPORTED: FAQ subheading '{subheading_text}' is not supported by keyword intent, SERP/PAA signals, or strong commercial logic."
                        )
                elif not self._h3_supports_parent(heading_text, subheading_text, keyword_profile):
                    errors.append(
                        f"H3_PARENT_INTENT_MISMATCH: Subheading '{subheading_text}' does not clearly support the parent H2 '{heading_text}'."
                    )

        if len(intro_positions) != 1:
            errors.append(
                f"INTRO_SECTION_COUNT_INVALID: Expected exactly 1 introduction section, found {len(intro_positions)}."
            )
        elif intro_positions[0] != 0:
            errors.append("INTRO_SECTION_ORDER_INVALID: The introduction section must be the first outline item.")

        if len(faq_positions) > 1:
            errors.append(f"FAQ_SECTION_COUNT_INVALID: Expected at most 1 FAQ section, found {len(faq_positions)}.")
        elif faq_positions and faq_positions[0] < max(1, len(outline) - 3):
            errors.append("FAQ_SECTION_ORDER_INVALID: The FAQ section should appear near the end of the outline.")

        if len(conclusion_positions) > 1:
            errors.append(
                f"CONCLUSION_SECTION_COUNT_INVALID: Expected at most 1 conclusion section, found {len(conclusion_positions)}."
            )
        elif conclusion_positions and conclusion_positions[-1] != len(outline) - 1:
            errors.append("CONCLUSION_SECTION_ORDER_INVALID: The conclusion section must be the last outline item.")

        if (content_type or "").lower() == "brand_commercial":
            errors.extend(self._validate_commercial_heading_flow(outline, brand_name=brand_name))

        if area:
            area_norm = self._normalize_heading_label(area)
            early_h2_sections = visible_h2_sections[: min(3, len(visible_h2_sections))]
            if area_norm and early_h2_sections:
                has_area_early = any(
                    area_norm in self._normalize_heading_label(section.get("heading_text", ""))
                    for section in early_h2_sections
                )
                if not has_area_early:
                    errors.append(
                        f"LOCAL_SEO_HEADING_MISSING: Add the area '{area}' naturally into one of the first visible H2 headings."
                    )

        return errors

    def validate_outline_quality(self, outline: List[Dict[str, Any]], content_type: str = "") -> List[str]:
        errors = []

        # --- MANDATORY SECTION BRIEF CONTRACT FIELDS ---
        mandatory_brief_fields = [
            "section_promise", "reader_takeaway", "must_include_details",
            "must_not_repeat", "practical_decision_value", "evidence_expectation",
            "value_density_target", "allowed_generality_level", "subheading_policy"
        ]

        for idx, section in enumerate(outline):
            section_name = section.get("heading_text", f"Section {idx+1}")
            for field in mandatory_brief_fields:
                if field not in section:
                    errors.append(f"MISSING_CONTRACT_FIELD: Section '{section_name}' is missing '{field}'.")
                elif isinstance(section[field], str) and not section[field].strip():
                    errors.append(f"EMPTY_CONTRACT_FIELD: Section '{section_name}' has an empty value for '{field}'.")
                elif isinstance(section[field], list):
                    if field == "must_include_details" and not section[field]:
                        # Structural anchors can be lighter, but all value-carrying sections must provide concrete details.
                        if (section.get("section_type") or "").lower() not in ["introduction", "conclusion", "faq"]:
                            errors.append(f"EMPTY_DETAILS_FIELD: Section '{section_name}' must provide concrete 'must_include_details'.")
                    elif field == "must_not_repeat" and not section[field] and idx != 0:
                        errors.append(f"EMPTY_CONTRACT_FIELD: Section '{section_name}' must define 'must_not_repeat' for non-intro sections.")

        h2_sections = [s for s in outline if (s.get("heading_level") or "").upper() == "H2"]
        if len(h2_sections) < 3:
            errors.append(f"Outline too thin: only {len(h2_sections)} H2 sections found. Need at least 3-5.")

        texts = [s["heading_text"].lower() for s in h2_sections]
        if len(texts) != len(set(texts)):
            errors.append("Duplicate H2 headings detected. Each heading must be unique.")

        faq_section = next((s for s in outline if s.get("section_type") == "faq"), None)
        faq_count = len(faq_section.get("questions") or []) if faq_section else 0
        if faq_count > 0 and faq_count < 3:
            errors.append(f"Too few FAQ questions detected ({faq_count}). Minimum required is 3.")

        # --- PK 5-SLOT MAP VALIDATION ---
        h1_title = outline[0].get("heading_text", "") # Usually H1 is in title generator, but H2/Intro is first here
        pk_sections = [s for s in outline if s.get("requires_primary_keyword")]
        
        # Rule 1: Intro (Slot 1) must require PK
        if outline and not outline[0].get("requires_primary_keyword"):
             # We allow a slight leniency if it's the very first section even if not index-0
             intro_sec = next((s for s in outline if (s.get("section_type") or "").lower() == "introduction"), None)
             if intro_sec and not intro_sec.get("requires_primary_keyword"):
                  errors.append("Strategic Map Violation: Introduction section must be marked as 'requires_primary_keyword: true'.")

        # Rule 2: Exactly ONE H2 heading (Slot 2) must contain PK in its metadata requirement
        h2_pk_sections = [s for s in h2_sections if s.get("requires_primary_keyword")]
        if len(h2_pk_sections) != 1:
             errors.append(f"Strategic Map Violation: Exactly ONE H2 heading must require the Primary Keyword (found {len(h2_pk_sections)}).")

        # Rule 3: Total PK sections should be 4-5 (Slots 1, 2, 4, 5 + Conclusion)
        total_pk_reqs = len(pk_sections)
        if total_pk_reqs < 4:
             errors.append(f"Strategic Map Violation: Total PK assignment slots should be at least 4 (found {total_pk_reqs}).")

        coverage = self.evaluate_outline_coverage(outline, content_type)
        if coverage.get("missing"):
            errors.append(
                f"Outline coverage incomplete for {content_type or 'article'}: missing {', '.join(coverage['missing'])}."
            )
        return errors

    def consolidate_faq(self, outline: List[Dict]) -> List[Dict]:
        faq_sections = [s for s in outline if s.get("section_type") == "faq" or s.get("parent_section") == "sec_faq"]
        if not faq_sections:
            return outline

        first_faq = faq_sections[0]
        all_questions = []
        for s in faq_sections:
            if s.get("questions") and isinstance(s["questions"], list):
                all_questions.extend(s["questions"])
            elif s.get("heading_level") in ["H2", "H3"]:
                all_questions.append(s["heading_text"])

        safe_questions = []
        for q in all_questions:
            if isinstance(q, dict):
                safe_questions.append(str(q.get("question") or q.get("text", str(q))))
            else:
                safe_questions.append(str(q))

        first_faq["questions"] = list(dict.fromkeys(safe_questions))
        first_faq["section_type"] = "faq"
        first_faq["heading_level"] = "H2"
        first_faq.pop("parent_section", None)

        new_outline = []
        faq_anchored = False
        for s in outline:
            is_faq = s.get("section_type") == "faq" or s.get("parent_section") == "sec_faq"
            if is_faq:
                if not faq_anchored:
                    new_outline.append(first_faq)
                    faq_anchored = True
            else:
                new_outline.append(s)
        return new_outline

    def enforce_paa_sections(self, outline: List[Dict], paa_questions: List[str], min_percent: float = 0.15) -> Dict[str, Any]:
        h2_sections = [s for s in outline if (s.get("heading_level") or "").upper() == "H2"]
        total_h2 = max(len(h2_sections), 1)
        if not paa_questions:
            return {"paa_ok": True, "paa_ratio": 1.0, "missing_count": 0}

        safe_paa = [str(q.get("question") if isinstance(q, dict) else q).lower() for q in paa_questions]
        covered = sum(1 for sec in h2_sections if any(q_text in sec.get("heading_text", "").lower() for q_text in safe_paa))
        ratio = covered / total_h2
        required = max(1, int(total_h2 * min_percent))
        missing = max(0, required - covered)
        return {"paa_ok": ratio >= min_percent, "paa_ratio": round(ratio, 2), "missing_count": missing}

    def adjust_paa_by_intent(self, outline: List[Dict], intent: str) -> List[Dict]:
        if intent.lower() in ["transactional", "commercial"]:
            for s in outline:
                if s.get("source") == "paa":
                    s["heading_level"] = "H3"
                    s["parent_section"] = "sec_faq"
        return outline
    def enforce_intent_distribution(self, outline: List[Dict], intent: str, content_type: str) -> Tuple[List[Dict], List[str]]:
        errors = []
        h2_sections = [s for s in outline if (s.get("heading_level") or "").upper() == "H2"]

        if content_type == "brand_commercial":
            TARGET_COMMERCIAL_RATIO = 0.70
            PROTECTED_TYPES = {"faq", "conclusion", "introduction"}

            commercial_sections = [
                s for s in h2_sections
                if s.get("section_intent") in ["Commercial", "Transactional"]
            ]
            ratio = len(commercial_sections) / max(len(h2_sections), 1)

            if ratio < TARGET_COMMERCIAL_RATIO:
                needed = round(TARGET_COMMERCIAL_RATIO * len(h2_sections)) - len(commercial_sections)
                converted = 0
                for s in h2_sections:
                    if converted >= needed:
                        break
                    s_type = (s.get("section_type") or "").lower()
                    s_intent = s.get("section_intent", "")
                    if s_type in PROTECTED_TYPES:
                        continue
                    if s_intent not in ["Commercial", "Transactional"]:
                        s["section_intent"] = "Commercial"
                        s["sales_intensity"] = s.get("sales_intensity", "medium")
                        # NO CTA injection here. Writer/Validator handle it.
                        converted += 1

                commercial_now = [
                    s for s in h2_sections
                    if s.get("section_intent") in ["Commercial", "Transactional"]
                ]
                new_ratio = len(commercial_now) / max(len(h2_sections), 1)
                logger.info(f"[intent_distribution] Corrected commercial ratio: {ratio:.0%} → {new_ratio:.0%} (converted {converted} sections)")

                if new_ratio < 0.60:
                    errors.append(
                        f"Commercial intent distribution still too weak ({new_ratio:.0%}) after correction. "
                        f"Brand articles require at least 70% commercial/transactional H2 sections."
                    )

        if intent.lower() == "informational":
            for s in outline:
                # Force ALL sections to Informational intent
                s["section_intent"] = "Informational"
                s["sales_intensity"] = "low"

        return outline, errors

    def enforce_cta_policy(self, outline: List[Dict], content_type: str) -> List[Dict]:
        """Legacy: Policy is now handled by OutlineGenerator and Validator Layer."""
        return outline

    def inject_local_seo(self, outline: List[Dict], area: str) -> Tuple[List[Dict], List[str]]:
        if not area:
            return outline, []

        errors = []
        applied = False
        for s in outline:
            if s.get("section_type") == "core" and s.get("heading_level") == "H2" and not applied:
                s["local_context_required"] = True
                applied = True
            else:
                s.pop("local_context_required", None)

        first_h2 = next((s for s in outline if (s.get("heading_level") or "").upper() == "H2"), None)
        if first_h2 and area.lower() not in first_h2.get("heading_text", "").lower():
            h_text = first_h2.get("heading_text", "").strip(" .").lower()
            is_intro = h_text in ["introduction", "مقدمة", "مقدمه", "تمهيد"]
            if not is_intro:
                logger.warning(f"[local_seo_validate] Local area '{area}' not reflected in the first H2 heading.")

        return outline, errors

    def enforce_content_angle(self, outline: List[Dict], strategy: Dict[str, Any]) -> List[Dict]:
        if not strategy:
            return outline
        angle = strategy.get("primary_angle")
        if not angle:
            return outline

        applied = False
        for s in outline:
            if s.get("section_type") == "core" and s.get("heading_level") == "H2" and not applied:
                s["content_angle"] = angle
                applied = True
            else:
                s.pop("content_angle", None)
        return outline

    def calculate_sales_density(self, text: str, intent: str, language: str, structural_intel: Dict[str, Any]) -> bool:
        if intent.lower() != "commercial":
           return True

        terms = ["اتصل", "تواصل", "احجز", "اطلب", "سعر", "خدمة", "شركة"] if language == "ar" else ["contact", "call", "book", "order", "price", "service", "agency"]
        paragraphs = [p for p in text.split("\n") if len(p.strip()) > 30]
        if not paragraphs:
            return False

        sales_count = sum(any(term.lower() in p.lower() for term in terms) for p in paragraphs)
        ratio = sales_count / len(paragraphs)
        intensity = structural_intel.get("cta_intensity_pattern", "soft commercial")
        required_ratio = 0.5 if intensity == "aggressive" else 0.3
        return ratio >= required_ratio

    def validate_sales_intro(self, markdown: str, intent: str) -> Tuple[bool, Optional[str]]:
        if intent not in ["Transactional", "Commercial"]:
            return True, None
        first_200_words = " ".join(markdown.split()[:200]).lower()
        cta_keywords = ["تواصل", "احصل على", "اطلب", "استشارة", "عرض سعر", "contact", "get a quote", "book", "call us"]
        if any(k in first_200_words for k in cta_keywords):
            return True, None
        return False, "Missing CTA in first 200 words for sales article"

    def validate_local_context(self, text: str, area: str, language: str) -> bool:
        if not area:
            return True
        text_lower = text.lower()
        return area.lower() in text_lower

    def deduplicate_paragraphs_in_markdown(self, markdown: str, threshold: float = 0.85) -> str:
        """
        Splits markdown into paragraphs and removes any that are too similar to a previous one.
        This is a mechanical fail-safe for AI repetition.
        """
        if not markdown:
            return markdown

        paragraphs = markdown.split("\n\n")
        seen_paragraphs = []
        unique_paragraphs = []

        for p in paragraphs:
            p_strip = p.strip()
            if not p_strip:
                unique_paragraphs.append("")
                continue

            # Skip very short paragraphs (headings, labels)
            if len(p_strip) < 50:
                unique_paragraphs.append(p_strip)
                # Don't add to seen_paragraphs for deduplication if too short to be a 'claim'
                continue

            is_duplicate = False
            for i, prev in enumerate(seen_paragraphs):
                if len(prev) < 50: continue

                similarity = self.calculate_similarity(p_strip, prev)
                if similarity > threshold:
                    logger.warning(f"[Semantic Deduplicator] Near-duplicate detected (similarity {similarity:.2f}). Triggering Semantic Pivot...")

                    # ASYNC REWRITE ATTEMPT
                    # If we have an AI client, try to pivot the idea
                    if self.ai_client:
                        try:
                            # Instead of a full async call here (which would break this sync loop),
                            # we mark it for a 'Pivot' and handled it or just prune if it's too much overhead.
                            # BUT, to follow the user's request for 'Changing the Idea', we'll implement a
                            # separate pass or a simplified logic.
                            # For now, we follow the merge/prune strategy as a robust logic.
                            pass
                        except Exception: pass

                    # Strategy: If current has unique info or is longer, we logically merge (keep better)
                    if len(p_strip) > len(prev):
                         seen_paragraphs[i] = p_strip

                    is_duplicate = True
                    break

            if not is_duplicate:
                unique_paragraphs.append(p_strip)
                seen_paragraphs.append(p_strip)

        return "\n\n".join(unique_paragraphs)

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """
        Calculates similarity between two texts.
        Uses High-Fidelity Semantic Similarity (Sentence-Transformers) if model is available,
        otherwise falls back to Lexical Jaccard Similarity.
        """
        if not text1 or not text2:
            return 0.0

        # --- High-Fidelity Semantic Mode ---
        if self.semantic_model:
            try:
                # Delegate to SemanticService
                return self.semantic_model.calculate_similarity(text1, text2)
            except Exception as e:
                logger.error(f"Semantic similarity failed, falling back to Jaccard: {e}")

        # --- Lexical Jaccard Fallback ---
        def get_words(text):
            # Focus on significant words (5+ chars) to capture meaning over grammar
            return set(re.findall(r'\b\w{5,}\b', text.lower()))

        words1 = get_words(text1)
        words2 = get_words(text2)

        if not words1 or not words2:
            # Check if one is a subset of the other for very short strings
            if text1.lower() in text2.lower() or text2.lower() in text1.lower():
                return 0.8 # High enough to trigger 'similar' for short text
            return 0.0

        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))

        return intersection / union

    def _normalize_heading_label(self, text: str) -> str:
        if not text:
            return ""
        text = self._normalize_arabic(str(text).lower())
        text = re.sub(r"[^\w\u0600-\u06FF\s]", " ", text)
        return " ".join(text.split())

    def _expand_token_variants(self, token: str) -> set[str]:
        if not token:
            return set()
        token = self._normalize_heading_label(token)
        variants = {token}

        for prefix in ("وال", "بال", "كال", "فال", "لل", "ال", "و", "ب", "ل", "ف", "ك"):
            if token.startswith(prefix) and len(token) - len(prefix) >= 2:
                variants.add(token[len(prefix):])

        if token.endswith("ات"):
            variants.add(token[:-2])
        if token.endswith("ون") or token.endswith("ين"):
            variants.add(token[:-2])

        real_estate_map = {
            "شقه": {"شقق"},
            "شقق": {"شقه"},
            "عقار": {"عقارات"},
            "عقارات": {"عقار"},
            "وحده": {"وحدات"},
            "وحدات": {"وحده"},
            "محل": {"محلات"},
            "محلات": {"محل"},
            "مكتب": {"مكاتب"},
            "مكاتب": {"مكتب"},
            "فيلا": {"فلل", "فيلات"},
            "فلل": {"فيلا", "فيلات"},
            "فيلات": {"فيلا", "فلل"},
            "شاليه": {"شاليهات"},
            "شاليهات": {"شاليه"},
            "ارض": {"اراضي"},
            "اراضي": {"ارض"},
        }
        variants.update(real_estate_map.get(token, set()))

        return variants

    def _entity_family_for_token(self, token: str) -> str:
        normalized = self._normalize_heading_label(token)
        families = {
            "apartment": {
                "شقه", "شقق", "apartment", "apartments", "flat", "flats",
                "ستوديو", "استوديو", "studio", "دوبلكس", "duplex", "بنتهاوس", "penthouse",
            },
            "villa": {"فيلا", "فلل", "فيلات", "villa", "villas"},
            "chalet": {"شاليه", "شاليهات", "chalet", "chalets"},
            "office": {"مكتب", "مكاتب", "office", "offices"},
            "shop": {"محل", "محلات", "shop", "shops", "store", "stores"},
            "land": {"ارض", "اراضي", "land", "lands", "plot", "plots"},
            "generic": {"عقار", "عقارات", "property", "properties", "unit", "units", "وحده", "وحدات"},
        }
        for family, signals in families.items():
            if normalized in signals:
                return family
        return ""

    def _detect_entity_families_in_text(self, text: str) -> set[str]:
        normalized = self._normalize_heading_label(text)
        tokens = self._expanded_token_set(text)
        families = {
            "apartment": {
                "شقه", "شقق", "apartment", "apartments", "flat", "flats",
                "ستوديو", "استوديو", "studio", "دوبلكس", "duplex", "بنتهاوس", "penthouse",
            },
            "villa": {"فيلا", "فلل", "فيلات", "villa", "villas"},
            "chalet": {"شاليه", "شاليهات", "chalet", "chalets"},
            "office": {"مكتب", "مكاتب", "office", "offices"},
            "shop": {"محل", "محلات", "shop", "shops", "store", "stores"},
            "land": {"ارض", "اراضي", "land", "lands", "plot", "plots"},
            "generic": {"عقار", "عقارات", "property", "properties", "unit", "units", "وحده", "وحدات"},
        }
        detected = set()
        for family, signals in families.items():
            normalized_signals = {self._normalize_heading_label(signal) for signal in signals}
            if normalized_signals.intersection(tokens) or any(signal in normalized for signal in normalized_signals):
                detected.add(family)
        return detected

    def _foreign_entity_families(self, text: str, profile: Dict[str, Any]) -> set[str]:
        head_family = profile.get("entity_family") or self._entity_family_for_token(profile.get("head_entity", ""))
        if not head_family or head_family == "generic":
            return set()

        detected = self._detect_entity_families_in_text(text)
        detected.discard("generic")
        detected.discard(head_family)
        return detected

    def _token_matches_area_hint(self, token: str, area_tokens: List[str]) -> bool:
        normalized = self._normalize_heading_label(token)
        if not normalized or not area_tokens:
            return False
        variants = self._expand_token_variants(normalized)
        return any(area_token in variants for area_token in area_tokens)

    def _derive_keyword_profile(self, primary_keyword: str, area: str = "") -> Dict[str, Any]:
        normalized = self._normalize_heading_label(primary_keyword)
        tokens = normalized.split()

        entities = {
            "شقه", "شقق", "عقار", "عقارات", "وحده", "وحدات", "محل", "محلات",
            "فيلا", "فلل", "فيلات", "شاليه", "شاليهات", "مكتب", "مكاتب", "ارض", "اراضي",
        }
        skip_tokens = {"افضل", "ارخص", "دليل", "مقارنه", "مقارنة", "كيف", "ما", "متى", "اين", "احدث", "اليوم", "عام"}
        head_entity = next((token for token in tokens if token in entities), "")
        if not head_entity:
            head_entity = next((token for token in tokens if token not in skip_tokens), tokens[0] if tokens else "")

        intents = {"بيع", "للبيع", "شراء", "ايجار", "للايجار", "حجز", "للحجز", "استثمار", "تقسيط", "استئجار"}
        intent_tokens = [token for token in tokens if token in intents]

        location_tokens = self._normalize_heading_label(area).split() if area else []
        if not location_tokens and "في" in tokens:
            idx = tokens.index("في")
            location_tokens = tokens[idx + 1:]

        boundary_tokens = {"في", "فى", "in", "near", "vs", "مقارنة", "مقارنه"}
        strong_property_heads = (entities - {"مكتب", "مكاتب"}) | {"عقار", "عقارات", "وحده", "وحدات"}
        ambiguous_property_heads = {"مكتب", "مكاتب", "office", "offices"}
        compound_service_heads = {"شركه", "مكتب", "عياده", "مركز", "وكاله", "مؤسسه", "منصه", "خدمه", "خدمات"}

        phrase_tokens = [head_entity] if head_entity else []
        descriptor_tokens: List[str] = []
        if head_entity:
            try:
                head_index = tokens.index(head_entity)
            except ValueError:
                head_index = -1

            if head_index >= 0:
                normalized_head = self._normalize_heading_label(head_entity)
                entity_family = self._entity_family_for_token(head_entity)
                is_property_like = normalized_head in strong_property_heads or (
                    normalized_head in ambiguous_property_heads and bool(intent_tokens)
                ) or entity_family in {"apartment", "villa", "chalet", "shop", "land", "generic"}
                is_compound_service = normalized_head in compound_service_heads

                for token in tokens[head_index + 1:]:
                    if token in boundary_tokens or re.fullmatch(r"\d{4}", token):
                        break
                    if self._token_matches_area_hint(token, location_tokens):
                        break
                    
                    if is_property_like:
                        # For properties (real estate), we stop at the head noun and don't include intent/location in the phrase
                        break
                        
                    phrase_tokens.append(token)
                    if is_compound_service:
                        descriptor_tokens.append(token)

        entity_phrase = " ".join(phrase_tokens).strip() or head_entity
        service_phrase = " ".join(descriptor_tokens).strip() if descriptor_tokens else entity_phrase

        return {
            "head_entity": head_entity,
            "entity_phrase": entity_phrase,
            "service_phrase": service_phrase,
            "entity_descriptor_tokens": descriptor_tokens,
            "entity_family": self._entity_family_for_token(head_entity),
            "keyword_tokens": tokens,
            "intent_tokens": intent_tokens,
            "location_tokens": location_tokens,
            "normalized_keyword": normalized,
        }

    def _is_valid_faq_question(self, text: str) -> bool:
        """
        Validates if a string is formatted as an Arabic question.
        """
        prefixes = {"ما", "كيف", "هل", "كم", "متى", "اين", "أين", "لماذا"}
        text = text.strip().lower()
        words = text.split()
        if not words: return False
        
        # Must start with a question word
        if words[0] in prefixes:
            return True
        # Or start with a prefix like 'ما هي'
        if len(words) > 1 and words[0] == "ما":
            return True
            
        return False

    def _subheading_breaks_atomization(self, text: str, stage: str) -> bool:
        """
        Detects H3s that combine multiple ideas (e.g. "Area A and Area B").
        """
        # Arabic 'and' (و) is often attached to the next word.
        normalized = self._normalize_heading_label(text)
        words = normalized.split()
        
        if len(words) < 2: return False
        
        for i in range(1, len(words)):
            word = words[i]
            if word.startswith("و") and len(word) > 2:
                common_w_words = {"وحده", "وحدات", "وسط", "وجهه", "واجهه", "وادي", "وزاره", "وفق", "وصول", "وضع", "وضعنا"}
                if word not in common_w_words:
                    if len(words) >= 3:
                        return True
        
        if " مع " in f" {text} ":
            return True
            
        return False

    def _faq_question_is_supported(self, question: str, profile: Dict[str, Any], support_blob: Dict[str, Any]) -> bool:
        normalized_question = self._normalize_heading_label(question)
        q_tokens = self._expanded_token_set(question)
        conditional_commercial = {
            "تقسيط", "تمويل", "دفع", "دفعات", "شهري", "سنوي", "سداد", "تسديد", "تسهيلات",
            "اقساط", "أقساط", "قانوني", "اوراق", "عقد", "ملكيه",
        }
        contains_conditional_topic = any(
            self._normalize_heading_label(signal) in normalized_question for signal in conditional_commercial
        )

        supported_text = " ".join(support_blob.get("supported_terms", []))
        if contains_conditional_topic:
            return any(self._normalize_heading_label(signal) in supported_text for signal in conditional_commercial)

        head_entity = profile.get("head_entity", "")
        head_variants = self._expand_token_variants(head_entity)
        if q_tokens.intersection(head_variants):
            return True

        safe_commercial = {
            "سعر", "اسعار", "مساحه", "مساحات", "تشطيب", "احياء", "الاحياء", "مناطق", "المناطق",
            "خدمات", "الخدمات", "مرافق", "المرافق", "حجز", "معاينه", "استلام", "مفروشه", "مفروش",
        }

        if any(self._normalize_heading_label(signal) in normalized_question for signal in safe_commercial):
            return True

        location_tokens = set(profile.get("location_tokens", []))
        intent_tokens = set(profile.get("intent_tokens", []))
        if q_tokens.intersection(location_tokens) and q_tokens.intersection(intent_tokens.union(head_variants)):
            return True

        return False

    def _subheading_is_too_granular(self, text: str, stage: str) -> bool:
        normalized_text = self._normalize_heading_label(text)
        tokens = self._expanded_token_set(text)
        words = normalized_text.split()
        granular_signals = {
            "تشطيب", "التشطيب", "تقسيم", "تقسيمات", "توزيع", "غرف", "الغرف", "مستوى", "مستويات",
            "جوده", "جودة", "تكييف", "التكييف", "عزل", "العزل", "تهويه", "تهوية", "مواصفات",
            "تفاصيل", "ديكور", "داخليه", "داخلية", "layout", "finishing", "quality",
            "ventilation", "insulation", "conditioning", "specs", "details", "internal",
        }
        standalone_feature_buckets = {
            "شقه", "شقق", "ستوديو", "استوديو", "دوبلكس", "بنتهاوس", "عائلي", "عائليه",
            "مفروشه", "مفروش", "جاهزه", "جاهز", "حديقه", "duplex", "penthouse", "studio", "family",
        }

        has_granular_signal = any(
            self._normalize_heading_label(signal) in normalized_text or self._normalize_heading_label(signal) in tokens
            for signal in granular_signals
        )
        if not has_granular_signal:
            return False

        if stage == "features":
            if not any(bucket in tokens or bucket in normalized_text for bucket in standalone_feature_buckets):
                return True
            if any(detail in normalized_text for detail in ("تشطيب", "التشطيب", "تقسيم", "تقسيمات", "تكييف", "عزل")):
                return True

        if len(words) <= 5:
            return True

        return True

    def _subheading_breaks_atomization_v2(self, text: str, stage: str) -> bool:
        """
        Detects H3s that combine multiple ideas (e.g. "Area A and Area B").
        """
        conjunctions = {" و ", " مع "}
        if any(conj in f" {text} " for conj in conjunctions):
            if len(text.split()) > 3:
                return True
        return False

    def _heading_contains_exact_brand_name(self, text: str, brand_name: str) -> bool:
        normalized_brand = self._normalize_heading_label(brand_name)
        normalized_text = self._normalize_heading_label(text)
        
        if bool(normalized_brand) and normalized_brand in normalized_text:
            return True
            
        if len(brand_name) > 30:
            substitutes = ["المنصه", "المنصة", "الموقع", "الخدمه", "الخدمة", "منصة", "موقع", "خدمة"]
            if any(sub in normalized_text.split() for sub in substitutes):
                return True
        return False

    def _heading_contains_keyword_anchor(self, text: str, profile: Dict[str, Any]) -> bool:
        normalized_text = self._normalize_heading_label(text)
        normalized_pk = profile.get("normalized_keyword", "")
        if normalized_pk and normalized_pk in normalized_text:
            return True

        heading_tokens = self._expanded_token_set(text)
        keyword_tokens = profile.get("keyword_tokens", [])
        head_entity = profile.get("head_entity", "")
        entity_phrase = profile.get("entity_phrase", "")
        descriptor_tokens = profile.get("entity_descriptor_tokens", [])
        location_tokens = profile.get("location_tokens", [])
        intent_tokens = profile.get("intent_tokens", [])

        if entity_phrase and entity_phrase in normalized_text:
            return True

        overlap = sum(1 for token in keyword_tokens if token in heading_tokens)
        if head_entity and self._expand_token_variants(head_entity).intersection(heading_tokens):
            has_location = not location_tokens or all(token in heading_tokens for token in location_tokens)
            has_intent = not intent_tokens or any(token in heading_tokens for token in intent_tokens)
            has_descriptor = not descriptor_tokens or any(token in heading_tokens for token in descriptor_tokens)
            if has_location and has_intent and has_descriptor:
                return True

        if not keyword_tokens:
            return True
        return overlap >= max(2, len(keyword_tokens) - 1)

    def _heading_preserves_entity_focus(self, text: str, profile: Dict[str, Any]) -> bool:
        head_entity = profile.get("head_entity", "")
        if not head_entity:
            return True

        normalized_text = self._normalize_heading_label(text)
        heading_tokens = self._expanded_token_set(text)
        head_variants = self._expand_token_variants(head_entity)
        descriptor_tokens = profile.get("entity_descriptor_tokens", [])
        entity_phrase = profile.get("entity_phrase", "")
        if head_variants.intersection(heading_tokens):
            if descriptor_tokens:
                if entity_phrase and entity_phrase in normalized_text:
                    return True
                if any(token in heading_tokens for token in descriptor_tokens):
                    return True
                return False
            return True

        keyword_tokens = profile.get("keyword_tokens", [])
        location_tokens = profile.get("location_tokens", [])
        intent_tokens = profile.get("intent_tokens", [])
        overlap = sum(1 for token in keyword_tokens if token in heading_tokens)
        has_location = not location_tokens or all(token in heading_tokens for token in location_tokens)
        has_intent = not intent_tokens or any(token in heading_tokens for token in intent_tokens)
        return overlap >= max(2, len(keyword_tokens) - 1) and has_location and has_intent

    def _brand_appears_in_heading(self, text: str, brand_name: str) -> bool:
        return self._heading_contains_exact_brand_name(text, brand_name)

    def _brand_heading_allowed(self, section_type: str) -> bool:
        allowed = {"differentiation", "introduction", "conclusion"}
        return section_type.lower() in allowed

    def _commercial_flow_stage(self, section: Dict[str, Any]) -> str:
        section_type = (section.get("section_type") or "").lower().strip()
        for stage, aliases in self.COMMERCIAL_FLOW_SECTION_ALIASES.items():
            if section_type in aliases:
                return stage
        return section_type

    def _contains_any_signal(self, text: str, signals) -> bool:
        normalized = self._normalize_heading_label(text)
        return any(self._normalize_heading_label(str(sig)) in normalized for sig in signals)

    PRICE_HEADING_SIGNALS = {"سعر", "اسعار", "تكلفه", "قيمه", "متر"}

    def _build_outline_support_blob(self, primary_keyword: str, content_strategy: Optional[Dict], seo_intelligence: Optional[Dict]) -> Dict[str, Any]:
        supported_terms = set()
        if primary_keyword:
            supported_terms.update(self._normalize_heading_label(primary_keyword).split())
        if content_strategy:
            supported_terms.update(self._normalize_heading_label(str(content_strategy)).split())
        if seo_intelligence:
            supported_terms.update(self._normalize_heading_label(str(seo_intelligence)).split())
        return {"supported_terms": list(supported_terms)}

    def _expanded_token_set(self, text: str) -> set[str]:
        normalized = self._normalize_heading_label(text)
        tokens = set(normalized.split())
        expanded = set()
        for t in tokens:
            expanded.update(self._expand_token_variants(t))
        return expanded

    def _detect_optional_section_topics(self, text: str) -> set[str]:
        topics = set()
        mapping = {
            "legal": {"قانوني", "عقد", "اوراق", "ملكيه", "تسجيل"},
            "financing_payment": {"تقسيط", "تمويل", "بنك", "قرض"},
            "investment": {"استثمار", "عائد", "ارباح", "ROI"}
        }
        normalized = self._normalize_heading_label(text)
        for topic, signals in mapping.items():
            if any(sig in normalized for sig in signals):
                topics.add(topic)
        return topics

    def _optional_topic_is_justified(self, topic: str, support_blob: Dict[str, Any]) -> bool:
        supported = set(support_blob.get("supported_terms", []))
        mapping = {
            "legal": {"قانوني", "عقد", "اوراق", "ملكيه"},
            "financing_payment": {"تقسيط", "تمويل", "بنك"},
            "investment": {"استثمار", "عائد"}
        }
        return bool(mapping.get(topic, set()).intersection(supported))

    def prune_redundant_intros(self, text: str) -> str:
        """
        Removes repetitive 'Vision 2030' or 'Digital Transformation' style filler intros.
        """
        if not text:
            return text

        patterns = [
            r'(رؤية المملكة 2030.*?\.){2,}',
            r'(Vision 2030.*?\.){2,}',
            r'(التحول الرقمي.*?\.){2,}',
            r'(Digital Transformation.*?\.){2,}'
        ]

        cleaned = text
        for p in patterns:
            cleaned = re.sub(p, r'\1', cleaned, flags=re.IGNORECASE | re.DOTALL)

        lines = cleaned.split("\n\n")
        if len(lines) < 2:
            return cleaned

        pruned_lines = [lines[0]]
        for i in range(1, len(lines)):
            current = lines[i].strip()
            prev = pruned_lines[-1].strip()

            if not current or not prev:
                pruned_lines.append(current)
                continue

            cur_words = current.split()[:5]
            prev_words = prev.split()[:5]

            if cur_words == prev_words and len(cur_words) >= 3:
                # Similarity too high at start, just keep it for now but log
                pruned_lines.append(current)
            else:
                pruned_lines.append(current)

        return "\n\n".join(pruned_lines)

    def auto_split_long_paragraphs(self, text: str) -> str:
        """Ensures that each paragraph has max 4 sentences by splitting if necessary."""
        if not text:
            return text

        paragraphs = text.split("\n\n")
        new_paragraphs = []

        for p in paragraphs:
            p = p.strip()
            if not p: continue

            # Skip tables, lists, and headings
            if p.startswith(("|", "-", "*", "#")):
                new_paragraphs.append(p)
                continue

            sentences = self.extract_sentences(p)
            if len(sentences) <= 4:
                new_paragraphs.append(p)
                continue

            # Split into chunks of 4 sentences
            chunks = [sentences[i:i + 4] for i in range(0, len(sentences), 4)]
            for chunk in chunks:
                new_paragraphs.append(" ".join(chunk))

        return "\n\n".join(new_paragraphs)

    async def inject_commercial_ctas(self, markdown: str, language: str, brand_url: str = "", brand_name: str = "") -> str:
        """Legacy: Fallback is now handled by workflow_controller with a targeted regeneration pass."""
        return markdown
