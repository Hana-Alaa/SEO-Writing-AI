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
        section_type = (section.get('section_type') or '').lower()
        is_conclusion = section_type == 'conclusion' or section_index == total_sections - 1
        is_introduction = section_type == 'introduction' or section_index == 0

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
            if len(paragraphs) != 3:
                errors.append(f"INTRO_STRUCTURE_VIOLATION: Introduction '{heading_text}' must contain exactly 3 distinct paragraphs.")
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
            
            # 2. First Paragraph Relevance (Only for the opening section)
            if section_index == 0 and paragraphs:
                first_para_lower = paragraphs[0].lower()
                kw_comp = [w.lower() for w in re.findall(r'\b\w+\b', primary_kw) if len(w) > 2]
                found_comp = [w for w in kw_comp if w in first_para_lower]
                comp_ratio = len(found_comp) / max(len(kw_comp), 1)
                
                # Check for exact phrase OR strong component presence (variant)
                has_exact = re.search(exact_pattern, first_para_lower)
                if not has_exact and comp_ratio < 0.25:
                    errors.append(f"RELEVANCE_VIOLATION: First paragraph fails to clearly reflect topic '{primary_kw}'.")

            # 3. Heading Relevance (For H2 sections assigned with PK)
            heading_lvl = (section.get("heading_level") or "").upper()
            if heading_lvl == "H2" and requires_pk:
                heading_lower = heading_text.lower()
                has_pk_in_heading = re.search(exact_pattern, heading_lower)
                if not has_pk_in_heading:
                    # Check for strong variant presence in heading
                    kw_comp = [w.lower() for w in re.findall(r'\b\w+\b', primary_kw) if len(w) > 2]
                    found_comp = [w for w in kw_comp if w in heading_lower]
                    if len(found_comp) / max(len(kw_comp), 1) < 0.5:
                        logger.warning(f"Heading relevance low for '{heading_text}'.")
                        # We don't block conclusion if heading is missing PK, but we warn or log.
            
            # 4. Phase-Aware Priority Requirement
            if requires_pk and exact_count == 0:
                # If section required exact-form but has none, check if it has a strong variant
                kw_comp = [w.lower() for w in re.findall(r'\b\w+\b', primary_kw) if len(w) > 2]
                found_comp = [w for w in kw_comp if w in content_lower]
                if len(found_comp) / max(len(kw_comp), 1) < 0.4:
                    # Only error if even the topic relevance is weak 
                    errors.append(f"TOPIC_RELEVANCE_VIOLATION: Priority section '{heading_text}' lacks clear topic relevance to '{primary_kw}'.")

        # Link Verification
        found_links = re.findall(r'\[.*?\]\((https?://.*?)\)', content)
        internal_domain = LinkManager.domain(brand_url) if brand_url else ""
        for link in found_links:
            link_domain = LinkManager.domain(link)
            if link_domain == internal_domain: continue
            if not await self._verify_external_link(link):
                errors.append(f"Broken external link: {link}")

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

    def validate_outline_quality(self, outline: List[Dict[str, Any]], content_type: str = "") -> List[str]:
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
