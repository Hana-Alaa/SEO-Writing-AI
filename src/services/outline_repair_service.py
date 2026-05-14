import logging
from typing import Dict, Any, List, Optional
from collections import Counter
import re

logger = logging.getLogger(__name__)

class OutlineRepairService:
    """Service for deterministic structural repairs to generated outlines."""

    VISITOR_INTENT_H3_SIGNALS = {
        "location": ["location", "how to get there", "access", "address", "map", "موقع", "عنوان", "الوصول"],
        "hours": ["hours", "opening times", "schedule", "أوقات", "مواعيد", "ساعات العمل"],
        "tickets": ["tickets", "pricing", "prices", "entry fee", "booking", "تذاكر", "أسعار", "اسعار", "حجز"],
        "parking": ["parking", "valet", "مواقف", "باركنج"],
        "entry": ["entry", "gate", "admission", "دخول", "بوابات"]
    }

    def promote_visitor_intents(
        self, 
        outline: List[Dict[str, Any]], 
        primary_keyword: str, 
        entity_phrase: str,
        serp_brief: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Detects generic 'Visitor Information' H2s and promotes high-value H3 intents to standalone H2s.
        Prevents over-splitting (e.g. parking vs location) unless strongly supported by SERP.
        """
        if not outline:
            return outline
            
        new_outline = []
        lang = "ar" if any("\u0600" <= c <= "\u06FF" for c in primary_keyword) else "en"
        
        promoted_count = 0
        # Track existing H2 intents to prevent over-splitting
        existing_h2_intents = set()
        for section in outline:
            if str(section.get("heading_level", "")).upper() == "H2":
                h_text = section.get("heading_text", "")
                intent = self._detect_visitor_intent(h_text, lang)
                if intent:
                    existing_h2_intents.add(intent)

        for section in outline:
            heading_text = section.get("heading_text", "")
            heading_level = str(section.get("heading_level") or "").upper()
            section_type = str(section.get("section_type") or "").lower()
            subheadings = section.get("subheadings", [])

            # Target: Visitor Information H2
            is_visitor_h2 = section_type == "visitor_information" or self._is_generic_visitor_heading(heading_text, lang)
            
            if heading_level == "H2" and is_visitor_h2 and isinstance(subheadings, list) and subheadings:
                promoted = []
                remaining_subs = []
                
                for sub in subheadings:
                    intent = self._detect_visitor_intent(sub, lang)
                    if intent:
                        # TASK 3: Stricter promotion logic
                        strong_standalone_intents = ["location", "hours", "tickets"]
                        
                        should_promote = True
                        if intent not in strong_standalone_intents:
                            # Strict check for parking/entry/services: only promote if strongly supported by SERP
                            if not self._is_strongly_supported_in_serp(intent, serp_brief, lang):
                                should_promote = False
                                logger.info(f"[OutlineRepairService] Suppressed {intent} promotion for '{entity_phrase}' - weak SERP evidence.")
                        
                        if should_promote:
                            # Create a new H2 section for this intent
                            promoted_h2 = self._create_promoted_h2(sub, intent, entity_phrase or primary_keyword, lang, section)
                            promoted.append(promoted_h2)
                            promoted_count += 1
                        else:
                            remaining_subs.append(sub)
                    else:
                        remaining_subs.append(sub)
                
                if promoted:
                    # Insert promoted H2s
                    new_outline.extend(promoted)
                    
                    # If there are remaining subheadings, keep the original section but maybe update it
                    if remaining_subs:
                        section["subheadings"] = remaining_subs
                        new_outline.append(section)
                    # else: skip original section as all H3s were promoted
                else:
                    new_outline.append(section)
            else:
                new_outline.append(section)

        if promoted_count > 0:
            logger.info(f"[OutlineRepairService] Promoted {promoted_count} visitor intents to H2 for '{entity_phrase or primary_keyword}'")
            return self._resequence_ids(new_outline)
        
        return outline

    def _is_generic_visitor_heading(self, text: str, lang: str) -> bool:
        generics = {
            "en": ["visitor information", "practical information", "plan your visit", "essential info"],
            "ar": ["معلومات الزوار", "معلومات تهمك", "دليل الزيارة", "معلومات عملية"]
        }
        text_lower = text.lower()
        return any(g in text_lower for g in generics.get(lang, generics["en"]))

    def _detect_visitor_intent(self, text: str, lang: str) -> Optional[str]:
        text_lower = text.lower()
        for intent, signals in self.VISITOR_INTENT_H3_SIGNALS.items():
            if any(s in text_lower for s in signals):
                return intent
        return None

    def _create_promoted_h2(self, sub_text: str, intent: str, entity: str, lang: str, parent: Dict[str, Any]) -> Dict[str, Any]:
        """Creates a descriptive H2 based on the H3 intent and entity."""
        # Templates for descriptive headings
        templates = {
            "location": {
                "en": f"Location of {entity} and How to Get There",
                "ar": f"موقع {entity} وكيفية الوصول إليه"
            },
            "hours": {
                "en": f"Opening Hours and Best Time to Visit {entity}",
                "ar": f"مواعيد عمل {entity} وأفضل أوقات الزيارة"
            },
            "tickets": {
                "en": f"Ticket Prices and Booking for {entity}",
                "ar": f"أسعار تذاكر {entity} وطرق الحجز"
            },
            "parking": {
                "en": f"Parking and Transport Services at {entity}",
                "ar": f"مواقف السيارات وخدمات النقل في {entity}"
            },
            "entry": {
                "en": f"Entry Requirements for {entity}",
                "ar": f"شروط الدخول إلى {entity}"
            }
        }
        
        # Default fallback
        new_text = templates.get(intent, {}).get(lang, sub_text)
        
        # Clone parent structure to keep contracts
        new_sec = parent.copy()
        new_sec["heading_text"] = new_text
        new_sec["heading_level"] = "H2"
        new_sec["subheadings"] = []
        new_sec["section_id"] = f"promoted_{intent}_{re.sub(r'\W+', '_', entity.lower())[:20]}"
        
        # Adjust contract fields if present to match the new scope
        if "section_promise" in new_sec:
             new_sec["section_promise"] = f"Provide detailed {intent} information for {entity}."
        if "reader_takeaway" in new_sec:
             new_sec["reader_takeaway"] = f"Understand the {intent} details for {entity}."
        
        return new_sec

    def _is_strongly_supported_in_serp(self, intent: str, serp_brief: Optional[Dict[str, Any]], lang: str) -> bool:
        """Checks if an intent (e.g. parking) is strongly observed in SERP data."""
        if not serp_brief:
            return False
        
        signals = self.VISITOR_INTENT_H3_SIGNALS.get(intent, [])
        if not signals:
            return False
            
        # Check observed_topics
        observed_topics = serp_brief.get("observed_topics", [])
        for topic_obj in observed_topics:
            topic_text = ""
            if isinstance(topic_obj, dict):
                topic_text = topic_obj.get("topic", "").lower()
            elif isinstance(topic_obj, str):
                topic_text = topic_obj.lower()
            
            if any(s in topic_text for s in signals):
                return True
                
        # Check secondary_keyword_phrases
        secondary = serp_brief.get("secondary_keyword_phrases", [])
        for phrase in secondary:
            if any(s in phrase.lower() for s in signals):
                return True
                
        # Check heading_candidates
        candidates = serp_brief.get("heading_candidates", [])
        for candidate in candidates:
            if any(s in candidate.lower() for s in signals):
                return True
                
        return False

    def _resequence_ids(self, outline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for idx, section in enumerate(outline):
            section["section_id"] = f"sec_{idx + 1}"
        return outline

    def dedupe_faq_against_h2(self, outline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Removes FAQ subheadings that merely repeat the core intent of an existing H2.
        """
        if not outline:
            return outline
            
        h2_intents = []
        faq_section_idx = -1
        
        for i, section in enumerate(outline):
            h_text = section.get("heading_text", "").lower()
            s_type = section.get("section_type", "").lower()
            h_level = section.get("heading_level", "")
            
            if s_type == "faq" or "faq" in h_text or "أسئلة شائعة" in h_text:
                faq_section_idx = i
            elif h_level == "H2":
                h2_intents.append(h_text)
                
        if faq_section_idx == -1:
            return outline
            
        faq_section = outline[faq_section_idx]
        subheadings = faq_section.get("subheadings", [])
        if not isinstance(subheadings, list) or not subheadings:
            return outline
            
        overlap_map = {
            "hours": ["ساعات", "أوقات", "مواعيد", "عمل", "متى يفتح", "متى يغلق", "وقت الافتتاح", "وقت الإغلاق", "hour", "time", "open", "close"],
            "location": ["موقع", "أين يقع", "كيف أصل", "وصول", "طريق", "location", "where", "how to get", "access", "direction"],
            "tickets": ["أسعار", "سعر", "تذاكر", "تذكرة", "بكم", "حجز", "رسوم", "تكلفة", "ticket", "price", "cost", "book", "fee"],
            "activities": ["أنشطة", "فعاليات", "تجارب", "أبرز ما يمكن", "ماذا يوجد", "ألعاب", "activity", "event", "thing to do"],
            "planning": ["أفضل وقت", "وقت الزيارة", "الزحام", "الوقت المناسب", "تخطيط", "متى أزور", "best time", "when to visit", "planning"]
        }
        
        existing_h2_intents = set()
        for h2 in h2_intents:
            for intent_name, keywords in overlap_map.items():
                if any(kw in h2 for kw in keywords):
                    existing_h2_intents.add(intent_name)
                    
        if existing_h2_intents:
            logger.info(f"[OutlineRepairService] dedupe_faq_against_h2 detected H2 intents: {existing_h2_intents}")
                    
        filtered_subheadings = []
        removed_count = 0
        
        distinct_variation_keywords = [
            "أطفال", "عائل", "مجاني", "خصم", "مسبق", "تختلف", "brand", "child", "family", "free", "discount", "advance", "vary", "differ", "هل"
        ]
        
        for sub in subheadings:
            sub_text = sub.get("heading_text", "").lower() if isinstance(sub, dict) else str(sub).lower()
            
            has_distinct_variation = False
            variation_reason = ""
            for dk in distinct_variation_keywords:
                if dk in sub_text:
                    has_distinct_variation = True
                    variation_reason = dk
                    break
            
            if has_distinct_variation:
                logger.info(f"[OutlineRepairService] Preserved FAQ: '{sub_text}' (contains distinct variation: '{variation_reason}')")
                filtered_subheadings.append(sub)
                continue
                
            is_overlap = False
            for intent_name in existing_h2_intents:
                if any(kw in sub_text for kw in overlap_map[intent_name]):
                    is_overlap = True
                    break
                    
            if is_overlap:
                removed_count += 1
                logger.info(f"[OutlineRepairService] Removed FAQ: '{sub_text}' (overlapped with H2 intent)")
            else:
                logger.info(f"[OutlineRepairService] Preserved FAQ: '{sub_text}' (no overlap detected)")
                filtered_subheadings.append(sub)
                
        if removed_count > 0:
            logger.info(f"[OutlineRepairService] Removed {removed_count} duplicate FAQ(s).")
            
        faq_section["subheadings"] = filtered_subheadings
        outline[faq_section_idx] = faq_section
        
        return outline

    def refill_faq_after_dedupe(
        self,
        outline: List[Dict[str, Any]],
        entity_phrase: str = "",
        min_faq_count: int = 4
    ) -> List[Dict[str, Any]]:
        """
        If FAQ count dropped below min_faq_count after deduplication,
        appends deterministic practical FAQs covering unresolved visitor topics.
        Does NOT re-add any intent already covered by an H2.
        """
        if not outline:
            return outline

        # Locate FAQ section
        faq_section_idx = -1
        for i, section in enumerate(outline):
            s_type = section.get("section_type", "").lower()
            h_text = section.get("heading_text", "").lower()
            if s_type == "faq" or "أسئلة شائعة" in h_text or "faq" in h_text:
                faq_section_idx = i
                break

        if faq_section_idx == -1:
            return outline

        faq_section = outline[faq_section_idx]
        subheadings = faq_section.get("subheadings", [])
        if not isinstance(subheadings, list):
            subheadings = []

        current_count = len(subheadings)
        if current_count >= min_faq_count:
            return outline  # Already sufficient

        experience_signals = [
            "visit", "visitor", "venue", "destination", "attraction", "event",
            "ticket", "tickets", "booking", "mall", "museum", "park", "restaurant",
            "زيارة", "زوار", "وجهة", "فعالية", "فعاليات", "تذاكر", "حجز",
            "مول", "متحف", "حديقة", "مطعم", "بوليفارد", "سيتي",
        ]
        if not self._topic_has_any_signal(outline, {}, entity_phrase, experience_signals):
            logger.info(
                "[OutlineRepairService] refill_faq_after_dedupe: skipped visitor FAQ refill for non-experience topic."
            )
            return outline

        # Collect existing H2 text for guard-checking
        h2_texts_combined = " ".join(
            s.get("heading_text", "").lower()
            for s in outline
            if s.get("heading_level") == "H2"
        )
        # Collect existing FAQ text for dedup guard
        existing_faq_text = " ".join(
            (s.get("heading_text", "") if isinstance(s, dict) else str(s)).lower()
            for s in subheadings
        )

        entity = entity_phrase.strip() if entity_phrase else ""

        # Ordered pool of practical recovery candidates.
        # Each entry: (topic_guard_keywords, arabic_question, english_question)
        # topic_guard_keywords: if ANY appear in h2_texts_combined, skip this candidate.
        recovery_pool = [
            (
                ["موقف", "مواقف", "parking"],
                f"هل تتوفر مواقف سيارات بالقرب من {entity or 'المكان'}؟",
                f"Is parking available near {entity or 'the venue'}?"
            ),
            (
                ["نقل", "مواصلات", "transport", "metro", "مترو"],
                f"كيف يمكن الوصول إلى {entity or 'الوجهة'} بالمواصلات العامة؟",
                f"How can I reach {entity or 'the venue'} by public transport?"
            ),
            (
                ["أطفال", "عائل", "children", "family"],
                f"هل {entity or 'المكان'} مناسب للعائلات والأطفال؟",
                f"Is {entity or 'the venue'} suitable for families with children?"
            ),
            (
                ["إمكانية الوصول", "ذوي الاحتياجات", "accessibility", "wheelchair"],
                f"هل {entity or 'المكان'} مناسب لذوي الاحتياجات الخاصة؟",
                f"Is {entity or 'the venue'} accessible for people with disabilities?"
            ),
            (
                ["دفع", "payment", "بطاقة", "كاش", "نقد"],
                f"ما هي طرق الدفع المتاحة في {entity or 'المكان'}؟",
                f"What payment methods are accepted at {entity or 'the venue'}?"
            ),
            (
                ["مدة", "duration", "وقت الزيارة", "كم من الوقت"],
                f"كم يستغرق الوقت المناسب لزيارة {entity or 'المكان'}؟",
                f"How long does a typical visit to {entity or 'the venue'} take?"
            ),
            (
                ["زحام", "crowd", "ازدحام"],
                f"ما هي أوقات الذروة والزحام في {entity or 'المكان'}؟",
                f"What are the peak/busy hours at {entity or 'the venue'}?"
            ),
            (
                ["طعام", "food", "أكل", "مأكولات", "مطعم"],
                f"هل يُسمح بإحضار الطعام إلى {entity or 'المكان'}؟",
                f"Is outside food allowed at {entity or 'the venue'}?"
            ),
        ]

        # Topic-specific recovery pools
        rental_signals = ["ايجار", "إيجار", "rent", "apartment", "listing", "شقة", "شقق", "سكن"]
        is_rental_topic = self._topic_has_any_signal(outline, {}, entity_phrase, rental_signals)

        if is_rental_topic:
            rental_pool = [
                (
                    ["شهري", "سنوي", "مدة الإيجار", "monthly", "yearly"],
                    f"هل تتوفر شقق للايجار الشهري في {entity or 'هذه المنطقة'}؟",
                    f"Are monthly rental apartments available in {entity or 'this area'}?"
                ),
                (
                    ["مفروش", "غير مفروش", "furnish"],
                    f"ما الفرق بين الشقق المفروشة وغير المفروشة من حيث التكلفة؟",
                    f"What is the difference between furnished and unfurnished apartments in terms of cost?"
                ),
                (
                    ["أفضل أحياء", "منطقة", "best area", "neighborhood"],
                    f"ما هي أفضل أحياء {entity or 'المدينة'} للبحث عن شقة للايجار؟",
                    f"What are the best neighborhoods in {entity or 'the city'} to search for an apartment?"
                ),
                (
                    ["معاينة", "عقد", "viewing", "contract"],
                    f"هل يمكن معاينة الشقة والتأكد من حالتها قبل توقيع العقد؟",
                    f"Is it possible to view the apartment and check its condition before signing the contract?"
                ),
                (
                    ["عوامل", "سعر", "تحديد", "factors", "price"],
                    f"ما هي العوامل الأساسية التي تحدد سعر إيجار الشقة في {entity or 'هذه المنطقة'}؟",
                    f"What are the main factors that determine the rental price of an apartment in {entity or 'this area'}?"
                ),
                (
                    ["دفع", "payment", "سداد"],
                    f"ما هي طرق الدفع المتاحة للإيجار (دفعة واحدة أم دفعات)؟",
                    f"What are the available payment methods for rent (single payment or installments)?"
                ),
            ]
            # Prepend rental pool to recovery pool for rental topics
            recovery_pool = rental_pool + recovery_pool

        added = 0
        needed = min_faq_count - current_count

        for guard_keywords, arabic_q, _ in recovery_pool:
            if added >= needed:
                break

            # Skip if the topic is already covered by an H2
            if any(kw in h2_texts_combined for kw in guard_keywords):
                continue

            # Skip if already present in FAQ
            if any(kw in existing_faq_text for kw in guard_keywords):
                continue

            subheadings.append(arabic_q)
            existing_faq_text += " " + arabic_q.lower()
            added += 1
            logger.info(f"[OutlineRepairService] FAQ refill: appended '{arabic_q}'")

        if added > 0:
            logger.info(
                f"[OutlineRepairService] refill_faq_after_dedupe: added {added} question(s). "
                f"Final FAQ count: {len(subheadings)}"
            )

        faq_section["subheadings"] = subheadings
        outline[faq_section_idx] = faq_section
        return outline



    def _subheading_text(self, subheading: Any) -> str:
        if isinstance(subheading, dict):
            return str(subheading.get("heading_text", "") or "").strip()
        return str(subheading or "").strip()

    def _collect_outline_context(
        self,
        outline: List[Dict[str, Any]],
        serp_brief: Optional[Dict[str, Any]] = None,
        entity_phrase: str = "",
    ) -> str:
        parts: List[str] = [entity_phrase or ""]
        for section in outline or []:
            parts.append(str(section.get("heading_text", "") or ""))
            parts.append(str(section.get("section_type", "") or ""))
            for subheading in section.get("subheadings", []) or []:
                parts.append(self._subheading_text(subheading))

        serp_brief = serp_brief or {}
        for key in (
            "observed_topics",
            "secondary_keyword_phrases",
            "heading_candidates",
            "must_consider_sections",
        ):
            for item in serp_brief.get(key, []) or []:
                if isinstance(item, dict):
                    parts.append(str(item.get("topic") or item.get("heading") or item.get("text") or ""))
                else:
                    parts.append(str(item or ""))
        return " ".join(parts).lower()

    def _topic_has_any_signal(
        self,
        outline: List[Dict[str, Any]],
        serp_brief: Optional[Dict[str, Any]],
        entity_phrase: str,
        signals: List[str],
    ) -> bool:
        context = self._collect_outline_context(outline, serp_brief, entity_phrase)
        return any(signal.lower() in context for signal in signals)

    def _brand_utility_mode(
        self,
        outline: List[Dict[str, Any]],
        serp_brief: Optional[Dict[str, Any]],
        entity_phrase: str,
    ) -> str:
        strong_booking_signals = [
            "ticket", "tickets", "booking", "reservation", "entry fee", "venue",
            "destination", "attraction", "event", "visit", "visitor", "mall",
            "museum", "park", "restaurant", "festival", "show", "boulevard",
            "تذكرة", "تذاكر", "حجز", "دخول", "زيارة", "زوار", "وجهة",
            "فعالية", "فعاليات", "حفل", "حفلات", "موسم", "بوليفارد", "سيتي",
            "مول", "متحف", "حديقة", "مطعم", "مسرح", "معرض", "أوقات", "مواعيد",
            "موقع", "الوصول",
        ]
        price_signals = [
            "pricing", "price", "prices", "cost", "fee",
            "أسعار", "اسعار", "تكلفة", "تكاليف", "رسوم",
        ]
        implementation_signals = [
            "seo", "sem", "ppc", "marketing", "strategy", "campaign", "digital",
            "ads", "google ads", "content marketing", "implementation", "setup",
            "service", "services", "agency", "company", "سيو", "تحسين محركات البحث",
            "التسويق عبر محركات البحث", "تسويق", "استراتيجية", "استراتيجيات",
            "حملات", "إعلانات", "اعلانات", "تنفيذ", "تطبيق", "إدارة", "خدمات",
            "شركة", "وكالة", "ميزانية", "مشروع",
        ]

        has_strong_booking = self._topic_has_any_signal(outline, serp_brief, entity_phrase, strong_booking_signals)
        has_price = self._topic_has_any_signal(outline, serp_brief, entity_phrase, price_signals)
        has_implementation = self._topic_has_any_signal(outline, serp_brief, entity_phrase, implementation_signals)

        if has_strong_booking:
            return "booking"
        if has_implementation:
            return "implementation"
        if has_price:
            return "booking"
        return ""

    def _brand_implementation_label(self, entity_phrase: str) -> str:
        label = re.sub(r"\s+", " ", str(entity_phrase or "").strip())
        cleanup_prefixes = [
            r"^الفرق\s+بين\s+",
            r"^مقارنة\s+بين\s+",
            r"^difference\s+between\s+",
            r"^comparison\s+between\s+",
            r"^compare\s+",
        ]
        for pattern in cleanup_prefixes:
            label = re.sub(pattern, "", label, flags=re.IGNORECASE).strip()
        label = re.sub(r"\bseo\b", "SEO", label, flags=re.IGNORECASE)
        label = re.sub(r"\bsem\b", "SEM", label, flags=re.IGNORECASE)
        label = re.sub(r"\bppc\b", "PPC", label, flags=re.IGNORECASE)
        return label or "هذه الاستراتيجية"

    def _candidate_matches_brand_mode(self, candidate: str, mode: str) -> bool:
        candidate_lower = candidate.lower()
        booking_terms = ["ticket", "tickets", "book", "booking", "reservation", "حجز", "تذاكر", "تذكرة"]
        implementation_terms = ["implement", "implementation", "strategy", "تنفيذ", "استراتيجية", "استراتيجيات"]
        if mode == "implementation":
            return not any(term in candidate_lower for term in booking_terms)
        if mode == "booking":
            return any(term in candidate_lower for term in booking_terms) or not any(term in candidate_lower for term in implementation_terms)
        return False

    def normalize_heading_only_section_types(self, outline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fix model-selected section_type labels that conflict with the heading role."""
        if not outline:
            return outline

        definition_terms = [
            "definition", "basic concepts", "what is", "تعريف", "المفاهيم الأساسية",
            "ما هو", "ما هي", "المقصود",
        ]
        for section in outline:
            heading = str(section.get("heading_text", "") or "").lower()
            if str(section.get("heading_level", "")).upper() != "H2":
                continue
            if any(term in heading for term in definition_terms):
                if str(section.get("section_type", "")).lower() == "offer":
                    logger.info(
                        "[OutlineRepairService] Normalized definition section_type from offer to core_or_benefits: %s",
                        section.get("heading_text", ""),
                    )
                section["section_type"] = "core_or_benefits"
        return outline

    def clean_echo_and_repetition(self, outline: List[Dict[str, Any]], title: str, primary_keyword: str) -> List[Dict[str, Any]]:
        """
        Removes H1 echo from the first H2 and reduces primary keyword repetition across headings.
        """
        if not outline:
            return outline

        title_lower = title.lower()
        keyword_lower = primary_keyword.lower()
        
        # Step 1: Anti-Echo Rule for the first visible H2
        first_visible_h2 = None
        for section in outline:
            if str(section.get("heading_level", "")).upper() == "H2":
                first_visible_h2 = section
                break
        
        if first_visible_h2:
            h2_text = first_visible_h2.get("heading_text", "")
            h2_text_lower = h2_text.lower()
            
            # Detect echo patterns: "[keyword]: suffix" or "[title]: suffix"
            echo_pattern = False
            if h2_text_lower.startswith(keyword_lower) or h2_text_lower.startswith(title_lower):
                # Check for colon or dash suffix
                if ":" in h2_text or "-" in h2_text or "–" in h2_text:
                    echo_pattern = True
            
            # Also detect generic "Introduction to [topic]"
            if any(p in h2_text_lower for p in ["مقدمة عن", "تعريف بـ", "introduction to", "overview of"]):
                echo_pattern = True
                
            if echo_pattern:
                # Attempt to transform into a natural orientation question if it's a comparison
                if " vs " in keyword_lower or " و " in keyword_lower or "الفرق بين" in keyword_lower:
                    # Try to extract entities
                    entities = []
                    if "الفرق بين" in keyword_lower:
                        # Simple split for "الفرق بين X و Y"
                        parts = re.split(r"\s+و\s+", keyword_lower.replace("الفرق بين", "").strip())
                        entities = [p.strip() for p in parts if p.strip()]
                    elif " vs " in keyword_lower:
                        entities = [p.strip() for p in keyword_lower.split(" vs ") if p.strip()]
                    
                    if len(entities) >= 2:
                        # Transform to "What is X and what is Y?"
                        if any("\u0600" <= c <= "\u06FF" for c in h2_text):
                            e1 = entities[0].upper() if entities[0].lower() in ["seo", "sem", "ppc"] else entities[0]
                            e2 = entities[1].upper() if entities[1].lower() in ["seo", "sem", "ppc"] else entities[1]
                            first_visible_h2["heading_text"] = f"ما هو {e1} وما هو {e2}؟"
                            logger.info(f"[OutlineRepairService] Anti-Echo: Transformed '{h2_text}' to '{first_visible_h2['heading_text']}'")
                        else:
                            first_visible_h2["heading_text"] = f"What is {entities[0]} and {entities[1]}?"
                            logger.info(f"[OutlineRepairService] Anti-Echo: Transformed '{h2_text}' to '{first_visible_h2['heading_text']}'")

        # Step 2: Repetition Reduction
        keyword_usage_count = 0
        for section in outline:
            if str(section.get("heading_level", "")).upper() != "H2":
                continue
                
            heading = section.get("heading_text", "")
            heading_lower = heading.lower()
            
            if keyword_lower in heading_lower:
                keyword_usage_count += 1
                
                # If we've already used the keyword too many times, try to simplify
                if keyword_usage_count > 2:
                    # Check if it's the FAQ
                    s_type = section.get("section_type", "").lower()
                    if s_type == "faq" or "أسئلة شائعة" in heading_lower or "faq" in heading_lower:
                        if any("\u0600" <= c <= "\u06FF" for c in heading):
                            # Try to extract entities for a lighter variant
                            entities = [p.strip() for p in re.split(r"\s+و\s+| vs ", keyword_lower.replace("الفرق بين", "").strip()) if p.strip()]
                            if len(entities) >= 2:
                                e1 = entities[0].upper() if entities[0].lower() in ["seo", "sem", "ppc"] else entities[0]
                                e2 = entities[1].upper() if entities[1].lower() in ["seo", "sem", "ppc"] else entities[1]
                                section["heading_text"] = f"أسئلة شائعة عن {e1} و {e2}"
                                logger.info(f"[OutlineRepairService] Repetition Guard: Simplified FAQ heading to '{section['heading_text']}'")
                        else:
                            section["heading_text"] = "Frequently Asked Questions"
                            logger.info(f"[OutlineRepairService] Repetition Guard: Simplified FAQ heading to '{section['heading_text']}'")

        return outline

    def apply_strategic_map_and_roles(
        self,
        outline: List[Dict[str, Any]],
        primary_keyword: str,
        content_type: str,
        brand_name: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Hardens the outline by enforcing strategic keyword mapping and coverage roles.
        1. Ensures coverage_role is set for brand_commercial.
        2. Splitting PK requirement into:
           - contains_exact_primary_keyword (Heading visible match, exactly ONE H2)
           - requires_primary_keyword (Body copy writing obligation, multiple slots)
        3. Cleans PK from H3s.
        4. Ensures total PK slots (requires_primary_keyword) >= 4.
        """
        if not outline:
            return outline

        is_commercial = content_type == "brand_commercial"
        pk_lower = str(primary_keyword or "").lower()
        
        # Step 0: Deduplicate H2 Headings
        seen_h2 = set()
        for section in outline:
            if str(section.get("heading_level", "")).upper() == "H2":
                text = section.get("heading_text", "").strip().lower()
                if text in seen_h2:
                    # Append a differentiator to the heading text
                    suffix = " (تفاصيل إضافية)" if any("\u0600" <= c <= "\u06FF" for c in text) else " (Additional Details)"
                    section["heading_text"] = section["heading_text"].strip() + suffix
                seen_h2.add(section.get("heading_text", "").strip().lower())

        # 0. Initialize PK flags
        for section in outline:
            section["contains_exact_primary_keyword"] = False
            # We don't reset requires_primary_keyword here yet, we'll do it strategically

        # 1. Infer/Fix coverage roles and Clean H3s
        for section in outline:
            h_text = str(section.get("heading_text", "")).lower()
            s_type = str(section.get("section_type", "")).lower()
            h_level = str(section.get("heading_level", "")).upper()
            
            # Forbid 'body' section_type in commercial
            if is_commercial and s_type == "body":
                 if "faq" in h_text or "أسئلة" in h_text:
                     section["section_type"] = "faq"
                 elif any(kw in h_text for kw in ["خطوات", "كيف", "طريقة", "كيفية", "process"]):
                     section["section_type"] = "process"
                 else:
                     section["section_type"] = "offer"
                 s_type = section["section_type"]

            # Coverage role mapping if missing
            if is_commercial and not section.get("coverage_role"):
                section["coverage_role"] = self._infer_coverage_role(section)
            
            # Final role consistency: ensure section_type matches coverage_role if possible
            if is_commercial:
                role = section.get("coverage_role")
                if role == "features_or_included":
                    section["section_type"] = "features"
                elif role == "differentiators":
                    section["section_type"] = "differentiation"
                elif role == "offer_clarity":
                    section["section_type"] = "offer"
                elif role == "process_or_how":
                    section["section_type"] = "process"
                elif role == "proof":
                    section["section_type"] = "proof"

            # Clean PK from H3s
            subheadings = section.get("subheadings", [])
            if subheadings and isinstance(subheadings, list) and pk_lower:
                new_subs = []
                for sub in subheadings:
                    sub_text = self._subheading_text(sub)
                    if pk_lower in sub_text.lower():
                        # Strip the PK or rephrase slightly to avoid exact match in H3
                        pattern = re.compile(re.escape(primary_keyword), re.IGNORECASE)
                        cleaned_text = pattern.sub("", sub_text).strip(" :")
                        
                        if cleaned_text:
                            if isinstance(sub, dict):
                                sub["heading_text"] = cleaned_text 
                                sub["text"] = cleaned_text 
                                new_subs.append(sub)
                            else:
                                new_subs.append(cleaned_text)
                        else:
                            fallback = "تفاصيل إضافية" if "ar" in h_text else "Details"
                            new_subs.append(fallback)
                    else:
                        new_subs.append(sub)
                section["subheadings"] = new_subs

        # 2. Strategic PK Stamping
        pk_slots_count = 0
        
        # Slot 1: Introduction (always gets requires_primary_keyword=True, but NEVER contains_exact=True)
        for section in outline:
            if section.get("section_type") == "introduction":
                section["requires_primary_keyword"] = True
                section["contains_exact_primary_keyword"] = False
                pk_slots_count += 1
                break
        
        # Reset other sections to start fresh for strategic mapping
        for section in outline:
            if section.get("section_type") != "introduction":
                section["requires_primary_keyword"] = False

        # Slot 2: Exactly ONE visible H2 anchor (gets BOTH)
        h2_anchor_found = False
        # Prefer an H2 that already has the PK
        for section in outline:
            h_level = str(section.get("heading_level", "")).upper()
            h_text = str(section.get("heading_text", "")).lower()
            if h_level == "H2" and not h2_anchor_found:
                if pk_lower and pk_lower in h_text:
                    section["contains_exact_primary_keyword"] = True
                    section["requires_primary_keyword"] = True
                    h2_anchor_found = True
                    pk_slots_count += 1
        
        # Fallback to the first available H2 if none had the PK
        if not h2_anchor_found:
            for section in outline:
                if str(section.get("heading_level", "")).upper() == "H2":
                    section["contains_exact_primary_keyword"] = True
                    section["requires_primary_keyword"] = True
                    h2_anchor_found = True
                    pk_slots_count += 1
                    break
        
        # Slots 3+: Body sections (assign requires_primary_keyword=True to middle sections)
        if pk_slots_count < 4:
            potential_body_sections = [
                s for s in outline 
                if s.get("section_type") not in ["introduction", "faq", "conclusion"]
                and not s.get("requires_primary_keyword")
            ]
            for s in potential_body_sections:
                if pk_slots_count >= 4:
                    break
                s["requires_primary_keyword"] = True
                pk_slots_count += 1
        
        # Final Slot: Near conclusion if still under 5
        if pk_slots_count < 5:
             for i in range(len(outline)-1, -1, -1):
                 s = outline[i]
                 if s.get("section_type") == "conclusion":
                     continue
                 if not s.get("requires_primary_keyword") and s.get("section_type") != "introduction":
                     s["requires_primary_keyword"] = True
                     pk_slots_count += 1
                     break

        # 4. Deterministic Coverage Role Finalization
        if is_commercial:
            outline = self.finalize_brand_commercial_coverage_roles(outline, primary_keyword, brand_name)

        return outline

    def finalize_brand_commercial_coverage_roles(
        self, 
        outline: List[Dict[str, Any]], 
        primary_keyword: str, 
        brand_name: str
    ) -> List[Dict[str, Any]]:
        """
        Final deterministic pass to ensure all mandatory commercial roles are present.
        """
        if not outline:
            return outline

        mandatory_roles = [
            "offer_clarity", "features_or_included", "differentiators", 
            "proof", "comparison", "process_or_how", "faq", "conclusion"
        ]
        
        # 1. Normalize and Infer
        for section in outline:
            h_text = str(section.get("heading_text", "")).lower()
            goal = str(section.get("content_goal", "")).lower()
            
            # Ensure coverage_role exists (read from both field variants)
            role = section.get("coverage_role")
            if not role and section.get("coverage_roles"):
                roles = section.get("coverage_roles")
                role = roles[0] if isinstance(roles, list) and roles else None
            
            if not role:
                role = self._infer_coverage_role(section)
            
            section["coverage_role"] = role

        # 2. Check for missing mandatory roles
        present_roles = {s.get("coverage_role") for s in outline if s.get("coverage_role")}
        
        # 3. Deterministic Reassignment & Heading Refinement
        for role in mandatory_roles:
            if role in present_roles:
                continue

            # Try to find a redundant section to reassign
            # Strategy: 
            # 1. Prefer sections that are NOT already one of the mandatory roles we've filled
            # 2. Prefer 'offer_clarity' or 'process_or_how' if they are redundant
            reassign_candidates = [
                s for s in outline 
                if s.get("coverage_role") not in mandatory_roles
                and s.get("section_type") not in ["introduction", "faq", "conclusion"]
            ]
            
            if not reassign_candidates:
                # If everything is already a mandatory role, only reassign if we have duplicates of a role
                role_counts = Counter(s.get("coverage_role") for s in outline)
                reassign_candidates = [
                    s for s in outline
                    if role_counts[s.get("coverage_role")] > 1
                    and s.get("section_type") not in ["introduction", "faq", "conclusion"]
                ]

            # Final fallback: ONLY pick a candidate if it's NOT a mandatory role or if we have duplicates
            # (Prevent overwriting one mandatory role with another if we are short on sections)
            target = next((s for s in reassign_candidates if s.get("coverage_role") != role), None)
            
            # If no target found, we skip this role (we've filled all we can without overwriting)
            if not target:
                continue

            if target:
                target["coverage_role"] = role
                # Sync section_type for validator structure checks
                role_to_type = {
                    "offer_clarity": "offer",
                    "features_or_included": "features",
                    "differentiators": "differentiation",
                    "proof": "proof",
                    "comparison": "comparison",
                    "process_or_how": "process",
                    "faq": "faq",
                    "conclusion": "conclusion"
                }
                target["section_type"] = role_to_type.get(role, target.get("section_type"))
                
                # REWRITE HEADING to make the role explicit (v5.0 Hardening)
                if role == "features_or_included":
                    target["heading_text"] = f"المزايا والخدمات التي تحصل عليها عند اختيار {primary_keyword}"
                elif role == "differentiators":
                    if brand_name:
                        target["heading_text"] = f"ما الذي يميز {brand_name} عند البحث عن {primary_keyword}؟"
                    else:
                        target["heading_text"] = f"أهم ما يميز خدمتنا في {primary_keyword}"
                elif role == "proof":
                    target["heading_text"] = f"دليل الأسعار والعوامل المؤثرة على {primary_keyword}"
                
                present_roles.add(role)
                logger.info(f"[OutlineRepairService] Finalized missing role '{role}' by reassigning section '{target.get('heading_text')}'")

        # Debug Logging
        missing = [r for r in mandatory_roles if r not in present_roles]
        logger.info(f"[OutlineRepairService] brand_commercial_roles_present: {list(present_roles)}")
        if missing:
            logger.warning(f"[OutlineRepairService] brand_commercial_roles_MISSING: {missing}")

        return outline

    def _infer_coverage_role(self, section: Dict[str, Any]) -> str:
        s_type = str(section.get("section_type", "")).lower()
        h_text = str(section.get("heading_text", "")).lower()
        
        if s_type == "introduction": return "introduction"
        if s_type == "faq": return "faq"
        if s_type == "conclusion": return "conclusion"
        if s_type == "offer": return "offer_clarity"
        if s_type == "features": return "features_or_included"
        if s_type == "proof": return "proof"
        if s_type == "process": return "process_or_how"
        if s_type == "differentiation": return "differentiators"
        if s_type == "comparison": return "comparison"
        
        # Text based inference for 'body' or mixed types
        if any(kw in h_text for kw in ["مميزات", "مزايا", "خصائص", "features", "amenities", "specifications", "مواصفات"]): return "features_or_included"
        if any(kw in h_text for kw in ["لماذا", "نتميز", "أفضل", "differentiator", "why us", "best", "الفوارق", "مميزاتنا"]): return "differentiators"
        if any(kw in h_text for kw in ["سعر", "تكلفة", "أسعار", "price", "cost", "pricing", "حقائق", "واقع"]): return "proof"
        if any(kw in h_text for kw in ["خطوات", "كيفية", "طريقة", "how", "process", "steps", "طرق", "إجراءات"]): return "process_or_how"
        if any(kw in h_text for kw in ["مقارنة", "الفرق", "vs", "compare", "comparison", "بين"]): return "comparison"
        if any(kw in h_text for kw in ["أنواع", "خدمات", "options", "types", "services", "نظرة", "overview"]): return "offer_clarity"
        
        return "offer_clarity" # Default fallback

    def enrich_brand_utility_faq(
        self, 
        outline: List[Dict[str, Any]], 
        serp_brief: Dict[str, Any], 
        brand_context: str, 
        content_type: str,
        entity_phrase: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Deterministically appends ONE utility-oriented brand FAQ if conditions are met.
        Does not apply to commercial outlines.
        """
        if not outline:
            return outline
        if not brand_context:
            logger.info("[OutlineRepairService] enrich_brand_utility_faq: brand_context missing, skipped.")
            return outline
        if content_type != "informational":
            logger.info(f"[OutlineRepairService] enrich_brand_utility_faq: content_type is '{content_type}', skipped.")
            return outline

        brand_mode = self._brand_utility_mode(outline, serp_brief, entity_phrase)
        if not brand_mode:
            logger.info(
                "[OutlineRepairService] enrich_brand_utility_faq: no eligible brand utility mode for topic, skipped."
            )
            return outline

        candidates = serp_brief.get("brand_utility_candidates", [])
        candidate = ""
        if candidates and isinstance(candidates, list):
            candidate = str(candidates[0]).strip()
            if candidate and not self._candidate_matches_brand_mode(candidate, brand_mode):
                logger.info(
                    "[OutlineRepairService] Suppressed brand candidate that does not match topic mode '%s': %s",
                    brand_mode,
                    candidate,
                )
                candidate = ""
        
        # Deterministic fallback: synthesize from brand_context + entity_phrase
        if not candidate:
            entity_label = entity_phrase.strip() if entity_phrase else ""
            if brand_mode == "implementation":
                topic_label = self._brand_implementation_label(entity_label)
                candidate = f"كيف تساعدك {brand_context} في تنفيذ استراتيجية {topic_label} متكاملة؟"
            elif entity_label:
                candidate = f"هل يمكن حجز تذاكر {entity_label} عبر {brand_context}؟"
            else:
                candidate = f"هل يمكن الحجز عبر {brand_context}؟"
            logger.info(f"[OutlineRepairService] enrich_brand_utility_faq: synthesized candidate '{candidate}'")
        
        if not candidate:
            logger.info("[OutlineRepairService] enrich_brand_utility_faq: no candidate available, skipped.")
            return outline
            
        # Safety Filter
        banned_phrases = ["أفضل منصة", "لماذا تختار", "احجز الآن", "book now", "why choose", "best platform"]
        candidate_lower = candidate.lower()
        if any(banned in candidate_lower for banned in banned_phrases):
            logger.info(f"[OutlineRepairService] Suppressed brand FAQ enrichment: Promotional wording detected '{candidate}'")
            return outline
            
        # Find FAQ section
        faq_section_idx = -1
        for i, section in enumerate(outline):
            s_type = section.get("section_type", "").lower()
            h_text = section.get("heading_text", "").lower()
            if s_type == "faq" or "faq" in h_text or "أسئلة شائعة" in h_text:
                faq_section_idx = i
                break
                
        if faq_section_idx == -1:
            return outline
            
        faq_section = outline[faq_section_idx]
        subheadings = faq_section.get("subheadings", [])
        if not isinstance(subheadings, list):
            subheadings = []
        subheadings = [
            self._subheading_text(subheading)
            for subheading in subheadings
            if self._subheading_text(subheading)
        ]
        faq_section["subheadings"] = subheadings
        outline[faq_section_idx] = faq_section
            
        # brand_context is now the raw brand name (e.g. "تيك ايفينت")
        brand_name = brand_context.strip()

        # Generic placeholder phrases the LLM may have used instead of the real brand name
        generic_booking_phrases = [
            "المنصة الرسمية", "منصة الحجز", "الموقع الرسمي",
            "الموقع الإلكتروني الرسمي", "الموقع الالكتروني الرسمي",
            "booking platform", "official platform", "official website",
            "المنصة"
        ]

        # Check if the exact brand name is already present → skip entirely
        if brand_name and brand_name.lower() in str(subheadings).lower():
            logger.info(f"[OutlineRepairService] Brand '{brand_name}' already present in FAQ, skipped.")
            return outline

        # Detect generic booking placeholder in existing FAQ → replace it with exact brand name
        for idx, sub in enumerate(subheadings):
            sub_text = self._subheading_text(sub)
            sub_lower = sub_text.lower()
            if any(ph.lower() in sub_lower for ph in generic_booking_phrases):
                corrected_text = sub_text
                for ph in generic_booking_phrases:
                    if ph.lower() in sub_lower:
                        corrected_text = sub_text.replace(ph, brand_name)
                        break
                logger.info(
                    "[OutlineRepairService] Replaced generic brand placeholder with exact brand: %s",
                    brand_name,
                )
                subheadings[idx] = corrected_text
                faq_section["subheadings"] = subheadings
                outline[faq_section_idx] = faq_section
                return outline

        # Check if the exact synthesized candidate is already there
        for sub in subheadings:
            if candidate.lower() in self._subheading_text(sub).lower():
                return outline

        # Append / replace weakest
        new_sub = candidate

        # If FAQ exceeds 5, replace the weakest one to prevent bloating
        if len(subheadings) >= 5:
            strong_keywords = [
                "price", "ticket", "book", "cost", "fee", "location", "access",
                "hour", "time", "child", "family", "kids", "strategy", "implement",
                "combine", "measure", "سعر", "تذكر", "حجز", "رسوم", "موقع", "وصول",
                "ساع", "مواعيد", "وقت", "طفل", "أطفال", "عائل", "استراتيجية",
                "تنفيذ", "جمع", "قياس", "تكاليف",
            ]
            weakest_idx = -1
            for idx in range(len(subheadings)-1, -1, -1):
                sub = subheadings[idx]
                text = self._subheading_text(sub).lower()
                if not any(kw in text for kw in strong_keywords):
                    weakest_idx = idx
                    break
            if weakest_idx != -1:
                subheadings[weakest_idx] = new_sub
            else:
                logger.info(f"[OutlineRepairService] FAQ is full and all questions are strong. Brand utility FAQ skipped.")
                return outline
        else:
            subheadings.append(new_sub)

        faq_section["subheadings"] = subheadings
        outline[faq_section_idx] = faq_section
        
        logger.info(f"[OutlineRepairService] Appended brand utility FAQ: '{candidate}'")
        return outline

    def clean_conclusion_heading(self, outline: List[Dict[str, Any]], entity_phrase: str = "") -> List[Dict[str, Any]]:
        """
        Cleans up editorial conclusion headings and replaces them with practical alternatives.
        """
        if not outline:
            return outline
            
        editorial_phrases = ["تجربة زيارة", "تجربة متكاملة", "استكشاف المزيد", "خطواتك القادمة", "أهم ما يجب معرفته", "خاتمة"]
        
        for section in outline:
            if section.get("section_type", "").lower() == "conclusion" or section.get("heading_level", "") == "H2":
                h_text = section.get("heading_text", "")
                
                # If it's the last H2 and contains editorial phrasing
                is_conclusion_like = section.get("section_type", "").lower() == "conclusion" or section == outline[-1]
                
                if is_conclusion_like and any(ep in h_text for ep in editorial_phrases):
                    replacement = "خلاصة ونصائح قبل الزيارة"
                    if entity_phrase:
                        replacement = f"خلاصة ونصائح قبل زيارة {entity_phrase}"
                        
                    logger.info(f"[OutlineRepairService] clean_conclusion_heading triggered. Original: '{h_text}' -> Final: '{replacement}'")
                    section["heading_text"] = replacement
                elif is_conclusion_like:
                    logger.info(f"[OutlineRepairService] clean_conclusion_heading examined: '{h_text}' - no editorial phrasing detected, preserved.")
                    
        return outline
