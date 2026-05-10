import logging
from typing import Dict, Any, List, Optional
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

    def enrich_brand_utility_faq(
        self, 
        outline: List[Dict[str, Any]], 
        serp_brief: Dict[str, Any], 
        brand_context: str, 
        content_type: str
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
            
        candidates = serp_brief.get("brand_utility_candidates", [])
        if not candidates or not isinstance(candidates, list):
            logger.info("[OutlineRepairService] enrich_brand_utility_faq: brand_utility_candidates empty, skipped.")
            return outline
            
        candidate = str(candidates[0]).strip()
        if not candidate:
            logger.info("[OutlineRepairService] enrich_brand_utility_faq: candidate is blank, skipped.")
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
            
        # Check if brand is already in the outline (at least in FAQ)
        # To be safe, we check if ANY subheading in FAQ mentions the brand (or generic candidate words)
        brand_match = re.search(r"Official brand:\s*([^.]+)", brand_context)
        brand_name = brand_match.group(1).strip() if brand_match else ""
        if brand_name and brand_name.lower() in str(subheadings).lower():
            return outline
            
        # We also check if the exact candidate or similar is already there
        for sub in subheadings:
            if isinstance(sub, dict) and candidate.lower() in sub.get("heading_text", "").lower():
                return outline
            elif isinstance(sub, str) and candidate.lower() in sub.lower():
                return outline
                
        # Append the candidate
        new_sub = {
            "heading_text": candidate,
            "heading_level": "H3",
            "section_type": "faq",
            "section_id": f"faq_brand_utility_{re.sub(r'\W+', '_', candidate)[:15]}"
        }
        
        # If FAQ exceeds 5, replace the weakest one to prevent bloating
        if len(subheadings) >= 5:
            # Strong intents that shouldn't be replaced
            strong_keywords = ["price", "ticket", "book", "cost", "fee", "location", "access", "hour", "time", "child", "family", "kids", "سعر", "تذكر", "حجز", "رسوم", "موقع", "وصول", "ساع", "مواعيد", "وقت", "طفل", "أطفال", "عائل"]
            
            weakest_idx = -1
            # Search backwards to replace the last weak one
            for idx in range(len(subheadings)-1, -1, -1):
                sub = subheadings[idx]
                text = sub.get("heading_text", "").lower() if isinstance(sub, dict) else str(sub).lower()
                if not any(kw in text for kw in strong_keywords):
                    weakest_idx = idx
                    break
            
            if weakest_idx != -1:
                subheadings[weakest_idx] = new_sub
            else:
                # If all are strong, do not insert
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
