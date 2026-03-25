import re
import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple, ClassVar
from collections import Counter
from src.utils.link_manager import LinkManager

logger = logging.getLogger(__name__)

class ValidationService:
    """Service dedicated to content validation, quality checks, and structure enforcement."""

    def __init__(self, ai_client=None, semantic_model=None):
        self.ai_client = ai_client
        self.semantic_model = semantic_model

    def validate_h1_length(self, h1: str) -> bool:
        """Enforces H1 length rules (55-75 chars) as per the framework."""
        return 55 <= len(h1) <= 75

    def validate_strategy_alignment(self, strategy: Dict[str, Any], primary_keyword: str, area: str) -> Tuple[bool, Optional[str]]:
        angle = strategy.get("primary_angle", "").lower()
        if primary_keyword.lower() not in angle:
            return False, "Primary keyword not reflected in strategy angle"

        if area and area.lower() not in strategy.get("strategic_positioning","").lower():
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
        if not self.semantic_model or not text or not used_claims:
            return False, 0.0, ""
            
        sentences = self.extract_sentences(text)
        sentences = [s for s in sentences if len(s) > 40]
        
        if not sentences:
            return False, 0.0, ""
            
        try:
            from sentence_transformers import util
            import torch
            new_embeddings = self.semantic_model.encode(sentences, convert_to_tensor=True)
            claim_embeddings = self.semantic_model.encode(used_claims, convert_to_tensor=True)
            
            cosine_scores = util.cos_sim(new_embeddings, claim_embeddings)
            max_score = float(torch.max(cosine_scores))
            
            if max_score > threshold:
                max_idx = int(torch.argmax(cosine_scores).item())
                row_idx = max_idx // cosine_scores.shape[1]
                overlapping_sentence = sentences[row_idx]
                return True, max_score, overlapping_sentence
                
            return False, max_score, ""
        except Exception as e:
            logger.error(f"Semantic overlap check failed: {e}")
            return False, 0.0, ""

    async def validate_section_output(self, content: str, section: Dict[str, Any], section_index: int, total_sections: int, area: str, cta_type: str, blocked_domains: set = None, brand_url: str = "") -> Tuple[bool, List[str]]:
        """Strictly validates a section's output against counting and structural rules."""
        errors = []
        if not content:
            return False, ["Content is empty"]

        # 1. Paragraph Count Validation
        is_faq_or_pricing = section.get("section_type") in ["faq", "pricing"]
        paragraphs = [p for p in content.split("\n\n") if p.strip()]
        has_complex_structure = "|" in content or "- " in content or "* " in content
        
        if not is_faq_or_pricing and not has_complex_structure:
            num_paragraphs = len(paragraphs)
            if num_paragraphs < 2 or num_paragraphs > 6:
                errors.append(f"Paragraph count is {num_paragraphs}, must be 3-5")
                
        # 2. Sentence Count Validation per Paragraph
        for p in paragraphs:
            if not p.startswith("#") and not p.startswith("-") and not p.startswith("*") and not "|" in p:
                sentences = re.split(r'(?<=[.!؟])\s+', p.strip())
                num_sentences = len([s for s in sentences if len(s.strip()) > 5])
                if num_sentences > 5:
                    errors.append("Paragraphs are too dense (> 4 sentences)")
                    break

        # 3. Local Mention Check
        if area and section_index == 0:
            if area.lower() not in content.lower():
                errors.append(f"Missing mandatory local area mention: {area}")

        # 4. CTA Architecture Check
        has_link_or_button = "]" in content and "(" in content
        has_cta_verb = any(verb in content for verb in ["احصل", "اطلب", "تواصل", "ابدأ", "Get", "Request", "Start"])
        looks_like_cta = has_link_or_button or has_cta_verb

        is_first = (section_index == 0)
        is_last = (section_index == total_sections - 1)

        if is_first and cta_type in ["primary", "strong"] and not looks_like_cta:
            errors.append("Missing required Primary CTA in Introduction")
        elif is_last and cta_type in ["primary", "strong"] and not looks_like_cta:
            errors.append("Missing required Decisive CTA in Conclusion")

        # 5. Primary Keyword Density Check (Fuzzy/Semantic Match)
        primary_kw = section.get("assigned_keywords", [""])[0] if section.get("assigned_keywords") else ""
        if primary_kw and not is_faq_or_pricing:
            kw_words = [w.lower() for w in re.findall(r'\b\w+\b', primary_kw) if len(w) > 1]
            content_lower = content.lower()
            
            # Check if all major words of the keyword exist in the section
            # This allows for "Web design in Riyadh" to match "Web Design Riyadh"
            found_words = [w for w in kw_words if w in content_lower]
            
            # We require at least 80% of the keyword's words to be present to pass
            match_ratio = len(found_words) / max(len(kw_words), 1)
            
            if match_ratio < 0.8:
                errors.append(f"Primary keyword components '{primary_kw}' missing or too fragmented in core content")

        # 6. Flexible External Link Validation
        found_links = re.findall(r'\[.*?\]\((https?://.*?)\)', content)
        internal_domain = LinkManager.domain(brand_url) if brand_url else ""
        blocked_domains = blocked_domains or set()

        for link in found_links:
            link_domain = LinkManager.domain(link)
            if link_domain == internal_domain:
                continue
            
            if link_domain in blocked_domains or any(comp in link_domain for comp in blocked_domains):
                 errors.append(f"External link to a potential competitor detected: {link}. Links must be to non-competing authority/credible sources.")
                 continue

            if not await self._verify_external_link(link):
                errors.append(f"External link appears to be broken or unreachable (404/Timeout): {link}")

        return len(errors) == 0, errors

    async def _verify_external_link(self, url: str) -> bool:
        """Asynchronously checks if a URL is reachable and functional."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                response = await client.head(url)
                if response.status_code >= 400:
                    response = await client.get(url)
                return 200 <= response.status_code < 400
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
        if not text:
            return False
        clean_text = text.strip()
        if language == "ar":
            if clean_text.endswith(("الآن.", "اليوم.", "الآن!", "اليوم!")):
                return True
        
        terms = ["contact", "call", "book", "order", "price", "service", "agency", "اتصل", "تواصل", "احجز", "اطلب", "سعر", "خدمة", "شركة"]
        last_300 = clean_text[-300:].lower()
        return any(term.lower() in last_300 for term in terms)

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

    def enforce_outline_structure(self, outline: List[Dict[str, Any]], content_type: str) -> List[Dict[str, Any]]:
        present_types = {(s.get("section_type") or "").lower().strip() for s in outline}
        rules = self.REQUIRED_STRUCTURE_BY_TYPE.get(content_type)
        if rules:
            required = rules.get("mandatory", set())
            missing = required - present_types
            if missing:
                logger.error(f"[outline_validate] Missing mandatory sections for {content_type}: {missing}")
        
        for i, sec in enumerate(outline):
            if not sec.get("section_id"):
                sec["section_id"] = f"sec_{i+1:02d}"
        return outline

    def calculate_max_ctas(self, article_size_input: str) -> int:
        """Calculates the allowed number of CTAs based on article size (3 per 1,000 words)."""
        if article_size_input == "core_dynamic_expansion":
            # For dynamic expansion (1000-5000), we'll allow a flexible budget.
            # Base budget for 1000 words is 3. We'll allow up to 9 for 3000-5000 articles.
            return 9 
        
        try:
            size = int(re.sub(r'\D', '', article_size_input))
            return max(1, (size // 1000) * 3)
        except:
            return 3 # Default fallback

    def enforce_cta_budget(self, outline: List[Dict[str, Any]], article_size: str) -> List[Dict[str, Any]]:
        """Ensures the total number of CTAs does not exceed the budget."""
        max_ctas = self.calculate_max_ctas(article_size)
        
        cta_sections = [s for s in outline if s.get("cta_type") not in [None, "none", ""]]
        
        if len(cta_sections) > max_ctas:
            logger.info(f"Reducing CTAs from {len(cta_sections)} to {max_ctas} to fit budget.")
            # Keep intro and conclusion CTAs, prune middle ones
            intro = cta_sections[0] if cta_sections[0].get("section_type") == "introduction" else None
            conclusion = cta_sections[-1] if cta_sections[-1].get("section_type") == "conclusion" else None
            
            middle_ctas = [s for s in cta_sections if s != intro and s != conclusion]
            # Prune middle CTAs to fit budget
            to_keep = max_ctas - (1 if intro else 0) - (1 if conclusion else 0)
            
            for i, s in enumerate(middle_ctas):
                if i >= to_keep:
                    s["cta_type"] = "none"
                    s["cta_position"] = "none"
        
        return outline

    def validate_outline_quality(self, outline: List[Dict[str, Any]]) -> List[str]:
        errors = []
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
                        
                        # CTA Budgeting: 3 CTAs per 1000 words logic
                        # Simplified check: limit CTAs based on total expected word count
                        # We'll calculate the maximum allowed CTAs later or use a safe heuristic here
                        if s.get("cta_type") in [None, "none", ""]:
                            s["cta_type"] = "moderate"
                            s["cta_position"] = "last_sentence"
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
                # Force ALL sections to Informational intent, but ALLOW CTAs
                s["section_intent"] = "Informational"
                s["sales_intensity"] = "low"
                
                # Allow the AI's requested CTA type unless it's 'strong' in a non-conclusion
                if s.get("cta_type") == "strong" and s.get("section_type") != "conclusion":
                    s["cta_type"] = "primary" # Downgrade to informational primary

        return outline, errors

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
        context_terms = ["السوق", "العملاء في", "شركات في", "المنافسة في"] if language == "ar" else ["market in", "businesses in", "companies in", "competition in"]
        text_lower = text.lower()
        if area.lower() not in text_lower:
            return False
        return any(term.lower() in text_lower for term in context_terms)

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculates Jaccard Similarity between two texts."""
        if not text1 or not text2:
            return 0.0
            
        def get_words(text):
            return set(re.findall(r'\b\w{5,}\b', text.lower()))
            
        words1 = get_words(text1)
        words2 = get_words(text2)
        
        if not words1 or not words2:
            return 0.0
            
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return intersection / union

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
        """AI-driven fallback to ensure a high-conversion CTA in commercial articles."""
        if not markdown or not self.ai_client:
            return markdown
            
        if self.validate_final_cta(markdown, language):
            return markdown # Already has a strong CTA
            
        logger.info("[CTA_REFINER] Conclusion is weak. Triggering AI-driven CTA refinement...")
        
        prompt = f"""You are a conversion optimization expert.
Refine the conclusion of the following article to include a POWERFUL, EMPATHETIC, and DIRECT Call-To-Action.

Brand: {brand_name}
Link: {brand_url}
Language: {language}

Constraints:
- Return ONLY the refined conclusion text (last 2-3 paragraphs).
- Must include a bolded link like [**Text**]({brand_url}).
- Tone must be high-authority and persuasive.

Current Article Content (Snippet):
\"\"\"
{markdown[-1500:]}
\"\"\"

Refined Conclusion:"""

        try:
            res = await self.ai_client.send(prompt, step="cta_refinement")
            refined = res["content"].strip()
            if not refined:
                 return markdown
            
            # Replace the last paragraph or append
            paragraphs = markdown.split("\n\n")
            if len(paragraphs) > 1:
                return "\n\n".join(paragraphs[:-1]) + "\n\n" + refined
            return markdown + "\n\n" + refined
        except Exception as e:
            logger.error(f"CTA Refinement AI call failed: {e}")
            return markdown
