import re
import logging
from collections import Counter
from typing import Dict, Any, List, Optional, Set

logger = logging.getLogger(__name__)

class SERPTopicMiner:
    """Service to extract high-value topics and patterns from SERP data."""

    # Experience-based signals for "Visitor Intent" mining
    VISITOR_INTENT_SIGNALS = {
        "en": [
            "hours", "opening times", "tickets", "pricing", "prices", "location", "address", 
            "how to get there", "access", "parking", "entry", "entry fee", "booking", 
            "reservation", "events", "activities", "attractions", "visitor guide", 
            "practical information", "facilities"
        ],
        "ar": [
            "أوقات", "مواعيد", "تذاكر", "أسعار", "اسعار", "موقع", "عنوان", "كيفية الوصول", 
            "طريقة الوصول", "باركنج", "مواقف", "دخول", "رسوم الدخول", "حجز", "فعاليات", 
            "أنشطة", "انشطة", "معالم", "دليل الزوار", "معلومات عملية", "مرافق"
        ]
    }

    # Signal to type mapping
    SIGNAL_TYPE_MAP = {
        "hours": "visitor_info",
        "opening times": "visitor_info",
        "أوقات": "visitor_info",
        "مواعيد": "visitor_info",
        "tickets": "visitor_info",
        "تذاكر": "visitor_info",
        "pricing": "visitor_info",
        "prices": "visitor_info",
        "أسعار": "visitor_info",
        "اسعار": "visitor_info",
        "location": "visitor_info",
        "موقع": "visitor_info",
        "address": "visitor_info",
        "عنوان": "visitor_info",
        "activities": "activity",
        "أنشطة": "activity",
        "انشطة": "activity",
        "attractions": "attraction",
        "معالم": "attraction",
        "events": "event",
        "فعاليات": "event"
    }

    # TASK 1: Filler tails to trim
    FILLER_TAILS = {
        "ar": [
            "المكونات الرئيسية", "التجربة المتكاملة", "المزايا الأساسية", "التجربة الفريدة",
            "أهم العناصر", "كل ما تحتاج معرفته", "دليل شامل", "العناصر الأساسية", "أهم المزايا",
            "الخدمات والمزايا", "الخدمات الرئيسية", "نظرة عامة", "دليل كامل", "شامل", "تجربة متكاملة"
        ],
        "en": [
            "complete guide", "key components", "main features", "unique experience",
            "essential elements", "everything you need to know", "main highlights",
            "full details", "services and features", "key highlights", "overview", 
            "comprehensive guide", "integrated experience"
        ]
    }

    # Practical task intent signals for brand utility
    PRACTICAL_TASK_SIGNALS = {
        "en": [
            "booking", "ticketing", "registration", "appointment", "purchase", 
            "quote request", "contact", "access", "ordering", "download", "setup"
        ],
        "ar": [
            "حجز", "تذاكر", "تسجيل", "موعد", "شراء", "طلب سعر", "اتصال", "دخول", "طلب", "تحميل", "تثبيت"
        ]
    }

    def mine_topics(self, serp_data: Dict[str, Any], primary_keyword: str, brand_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Extracts and classifies topics, secondary keywords, and candidates from raw SERP data.
        Returns a dict with 'topics', 'secondary_keyword_phrases', 'heading_candidates', and 'guidance'.
        """
        top_results = serp_data.get("top_results", [])
        if not top_results:
            return {"topics": [], "secondary_keyword_phrases": [], "heading_candidates": [], "guidance": []}

        lang = "ar" if any("\u0600" <= c <= "\u06FF" for c in primary_keyword) else "en"
        
        all_headings = []
        all_titles = []
        all_snippets = []
        
        for res in top_results:
            if not isinstance(res, dict): continue
            all_titles.append(res.get("title", ""))
            all_snippets.append(res.get("snippet", ""))
            
            headings = res.get("headings", {})
            if isinstance(headings, dict):
                all_headings.extend(headings.get("h2", []))
                all_headings.extend(headings.get("h3", []))

        # 1. Mine for Visitor Intent
        visitor_topics = self._mine_visitor_intent(all_headings + all_titles, lang, primary_keyword)

        # 2. Mine for Attribute-based Topics
        attribute_topics = self._mine_attribute_topics(all_headings + all_titles, primary_keyword, lang)

        # 3. Consolidation
        combined_topics = self._consolidate_topics(visitor_topics + attribute_topics)

        # 4. TASK 2: Secondary Keyword & Heading Candidate Mining
        secondary_phrases = self._mine_secondary_phrases(all_headings + all_titles + all_snippets, serp_data.get("lsi_keywords", []), primary_keyword, lang)
        
        # Heading candidates are high-frequency or high-intent phrases found in SERP headings/titles
        heading_candidates = self._mine_heading_candidates(all_headings + all_titles, primary_keyword, lang)

        # 5. TASK 1: Filler Tail Cleanup Logic
        guidance = []
        for h in all_headings:
            cleaned = self.clean_heading_filler(h, lang)
            if cleaned != h.strip():
                guidance.append(f"Trim filler tail from '{h}' -> prefer '{cleaned}' if intent is preserved.")

        # 6. TASK 3: Utility-Oriented Brand Guidance
        if brand_name:
            brand_guidance = self.generate_brand_utility_guidance(combined_topics, brand_name, lang)
            if brand_guidance:
                guidance.append(brand_guidance)

        return {
            "topics": combined_topics,
            "secondary_keyword_phrases": secondary_phrases,
            "heading_candidates": heading_candidates,
            "guidance": list(dict.fromkeys(guidance))[:5] # Deduplicate and limit
        }

    def _mine_visitor_intent(self, texts: List[str], lang: str, pk: str) -> List[Dict[str, Any]]:
        results = []
        signals = self.VISITOR_INTENT_SIGNALS.get(lang, self.VISITOR_INTENT_SIGNALS["en"])
        
        seen_topics = set()
        for text in texts:
            if not text: continue
            text_lower = text.lower()
            for signal in signals:
                if signal in text_lower:
                    cleaned = self._clean_topic_phrase(text, signal)
                    if cleaned and cleaned.lower() not in seen_topics:
                        topic_type = self._resolve_topic_type(signal)
                        results.append({
                            "topic": cleaned,
                            "type": topic_type,
                            "source_signal": signal,
                            "relevance": "high"
                        })
                        seen_topics.add(cleaned.lower())
        return results

    def _mine_attribute_topics(self, texts: List[str], pk: str, lang: str) -> List[Dict[str, Any]]:
        # Extract 2-3 word phrases that appear multiple times
        # Excluding the primary keyword itself
        phrases = []
        pk_norm = pk.lower().strip()
        
        for text in texts:
            if not text: continue
            # Basic cleanup
            cleaned = re.sub(r'[^\w\s\u0600-\u06FF]', ' ', text.lower())
            words = [w.strip() for w in cleaned.split() if len(w.strip()) > 2]
            
            # Bigrams
            for i in range(len(words) - 1):
                phrase = f"{words[i]} {words[i+1]}"
                if pk_norm not in phrase and len(phrase) > 5:
                    phrases.append(phrase)
            
            # Trigrams
            for i in range(len(words) - 2):
                phrase = f"{words[i]} {words[i+1]} {words[i+2]}"
                if pk_norm not in phrase and len(phrase) > 8:
                    phrases.append(phrase)

        counts = Counter(phrases)
        # Keep phrases that appear at least twice or are long enough
        top_phrases = [p for p, count in counts.items() if count >= 2 and len(p.split()) >= 2]
        logger.debug(f"[SERPTopicMiner] Attribute phrases: extracted={len(phrases)}, unique={len(counts)}, kept={len(top_phrases)}")

        # Sort by frequency
        sorted_phrases = sorted(top_phrases, key=lambda x: counts[x], reverse=True)
        
        # Limit to top 10
        return [{"topic": p.title() if lang == "en" else p, "type": "attribute", "frequency": counts[p]} for p in sorted_phrases[:10]]

    def clean_heading_filler(self, heading: str, lang: str) -> str:
        """Trims generic filler tails from a heading while preserving intent punctuation."""
        if not heading: return ""
        tails = self.FILLER_TAILS.get(lang, self.FILLER_TAILS["en"])
        
        text = heading.strip()
        
        # Sort tails by length descending to match longest first
        sorted_tails = sorted(tails, key=len, reverse=True)
        
        for tail in sorted_tails:
            # Match tail preceded by separator (comma, dash, colon, or space)
            # Use \b for English, and handle Arabic space/punctuation
            if lang == "en":
                pattern = rf"[\s,\-\:\|]+{re.escape(tail)}\b\s*$"
            else:
                # Arabic doesn't have \b in the same way for all characters
                pattern = rf"[\s,\-\:\|]+{re.escape(tail)}\s*$"
            
            new_text = re.sub(pattern, "", text, flags=re.IGNORECASE)
            if new_text != text:
                # If we trimmed, check if what's left is meaningful (at least 2 words)
                words_left = new_text.split()
                if len(words_left) >= 2:
                    return new_text.strip(". :|-")
        
        return text

    def _mine_secondary_phrases(self, texts: List[str], lsi_keywords: List[str], pk: str, lang: str) -> List[str]:
        """Extracts natural secondary keyword phrases."""
        phrases = []
        pk_norm = pk.lower()
        
        # 1. Add observed LSI keywords
        for k in lsi_keywords:
            if k and k.lower() != pk_norm:
                phrases.append(k)
        
        # 2. Extract high-intent patterns from headings/titles
        # e.g. "حجز [entity]", "أسعار [entity]"
        intent_patterns = {
            "ar": [r"حجز\s+\w+", r"أسعار\s+\w+", r"اسعار\s+\w+", r"مواعيد\s+\w+", r"موقع\s+\w+"],
            "en": [r"book\s+\w+", r"ticket\s+prices", r"location\s+of\s+\w+", r"opening\s+hours"]
        }
        
        patterns = intent_patterns.get(lang, intent_patterns["en"])
        for text in texts:
            if not text: continue
            for p in patterns:
                matches = re.findall(p, text, re.IGNORECASE)
                phrases.extend(matches)

        # Deduplicate and return top 10
        kept = list(dict.fromkeys(phrases))[:10]
        logger.debug(f"[SERPTopicMiner] Secondary phrases: extracted={len(phrases)}, kept={len(kept)}")
        return kept

    def _mine_heading_candidates(self, headings: List[str], pk: str, lang: str) -> List[str]:
        """Identifies promising heading candidates from SERP."""
        candidates = []
        pk_norm = pk.lower()
        
        for h in headings:
            if not h: continue
            h_clean = self.clean_heading_filler(h, lang)
            # If the heading contains the PK or close entity phrase and isn't too long
            if (pk_norm in h_clean.lower() or any(w in h_clean.lower() for w in pk_norm.split())) and len(h_clean.split()) <= 7:
                candidates.append(h_clean)
        
        # Frequency analysis on candidates
        counts = Counter(candidates)
        top_candidates = [c for c, count in counts.most_common(10)]
        logger.debug(f"[SERPTopicMiner] Heading candidates: extracted={len(candidates)}, unique={len(counts)}, kept={len(top_candidates)}")
        return top_candidates

    def generate_brand_utility_guidance(self, topics: List[Dict[str, Any]], brand_name: str, lang: str) -> Optional[str]:
        """Generates guidance for contextual brand mention if a practical task is identified."""
        candidates = self.generate_brand_utility_candidates(topics, brand_name, lang)
        if candidates:
            if lang == "ar":
                return f"يمكن دمج إشارة سياقية لـ {brand_name} في قسم 'الحجز' أو 'الأسئلة الشائعة' لمساعدة المستخدم على إكمال المهمة (مثلاً: {candidates[0]})."
            else:
                return f"A contextual mention of {brand_name} may fit in the 'Booking' or 'FAQ' section to help the user complete the task (e.g., {candidates[0]})."
        return None

    def generate_brand_utility_candidates(self, topics: List[Dict[str, Any]], brand_name: str, lang: str) -> List[str]:
        """Generates utility-oriented FAQ-style suggestions for a brand."""
        task_signals = self.PRACTICAL_TASK_SIGNALS.get(lang, self.PRACTICAL_TASK_SIGNALS["en"])
        
        found_task = False
        for t in topics:
            topic_text = t["topic"].lower()
            if any(signal in topic_text for signal in task_signals):
                found_task = True
                break
        
        if not found_task:
            return []

        if lang == "ar":
            return [f"كيفية إتمام الحجز باستخدام {brand_name}"]
        else:
            return [f"How to complete your booking via {brand_name}"]

    def _clean_topic_phrase(self, text: str, signal: str) -> str:
        # If the heading is very long, it might be a sentence. Truncate or ignore.
        words = text.strip().split()
        if len(words) > 8:
            # Try to find a substring around the signal?
            # For now, just ignore if too long to avoid noisy topics
            return ""
        return text.strip(". :|")

    def _resolve_topic_type(self, signal: str) -> str:
        return self.SIGNAL_TYPE_MAP.get(signal, "visitor_info")

    def _consolidate_topics(self, topics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Sort by relevance/priority: visitor_info > attraction > attribute
        priority = {"visitor_info": 0, "attraction": 1, "activity": 1, "event": 1, "attribute": 2}
        sorted_topics = sorted(topics, key=lambda x: priority.get(x["type"], 99))
        
        seen = set()
        final = []
        for t in sorted_topics:
            norm = t["topic"].lower().strip()
            if norm not in seen:
                final.append(t)
                seen.add(norm)
        
        return final
