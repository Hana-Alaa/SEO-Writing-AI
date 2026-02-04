import re
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class SEOValidator:
    """
    Validates the generated article against Hard SEO Rules.
    Modular design for easy extensibility.
    """

    def validate(self, content: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Runs all checks and returns a PASS/FAIL report.
        """
        report = {
            "passed": True,
            "score": 100,
            "errors": [],
            "warnings": []
        }

        # 1. Content Basics (Length, Keyword Usage, Readability)
        self._check_content_basics(content, metadata, report)

        # 2. Heading Structure (H2, H3, Keyword Placement)
        self._check_headings(content, metadata, report)

        # 3. Call to Action (CTA)
        self._check_cta(content, metadata, report)

        # 4. Links (Internal vs External, Validity)
        self._check_links(content, metadata, report)

        # 5. Media (Images, Featured, Alt Text)
        self._check_media(content, metadata, report)

        # 6. Advanced Sections (FAQ, Schema placeholders)
        self._check_advanced_sections(content, metadata, report)

        # Final Score Calculation
        report["score"] -= (len(report["errors"]) * 10)
        report["score"] -= (len(report["warnings"]) * 5)
        report["score"] = max(0, report["score"])

        return report

    def _check_content_basics(self, content: str, metadata: Dict, report: Dict):
        primary_keyword = metadata.get("main_keyword", "").lower()
        word_count = len(content.split())

        # Length
        if word_count < 1000:
            report["errors"].append(f"Word count is {word_count}. Minimum required is 1000.")
            report["passed"] = False

        # First Paragraph Keyword
        paragraphs = [p for p in content.split("\n\n") if p.strip() and not p.strip().startswith("#")]
        if paragraphs:
            if primary_keyword not in paragraphs[0].lower():
                 report["errors"].append(f"Primary keyword '{primary_keyword}' not found in the first paragraph.")
                 report["passed"] = False

        # Density
        if word_count > 0:
            density = (content.lower().count(primary_keyword) / word_count) * 100
            if density < 1.2:
                 report["warnings"].append(f"Keyword density ({density:.2f}%) is too low. Target: 1.2% - 1.6%.")
            elif density > 2.0:
                 report["warnings"].append(f"Keyword density ({density:.2f}%) is high. Avoid keyword stuffing.")

        # LSI Keywords
        secondary_keywords = metadata.get("secondary_keywords", [])
        missing_lsi = [kw for kw in secondary_keywords if kw.lower() not in content.lower()]
        if missing_lsi:
            report["warnings"].append(f"Missing LSI keywords: {', '.join(missing_lsi)}")

        # Readability
        sentences = re.split(r'[.!?]+', content)
        long_sentences = [s for s in sentences if len(s.split()) > 25]
        if len(long_sentences) > 8:
             report["warnings"].append(f"Readability: Found {len(long_sentences)} sentences with >25 words.")

    def _check_headings(self, content: str, metadata: Dict, report: Dict):
        primary_keyword = metadata.get("main_keyword", "").lower()
        
        # H2 checks
        h2s = re.findall(r'^##\s+(.*)', content, re.MULTILINE)
        if len(h2s) < 3:
            report["errors"].append(f"Found only {len(h2s)} H2 headings. Minimum 3 required.")
            report["passed"] = False
        
        if not any(primary_keyword in h.lower() for h in h2s):
            report["warnings"].append(f"Primary keyword '{primary_keyword}' not found in any H2 heading.")

        # H3 checks
        h3s = re.findall(r'^###\s+(.*)', content, re.MULTILINE)
        if not h3s:
            report["warnings"].append("No H3 headings found. Consider deeper structure.")
        
        # Nesting check (H3 should usually be preceded by an H2 somewhere)
        # We can do a sequence check
        headings = re.findall(r'^(##+)\s+', content, re.MULTILINE)
        for i, h in enumerate(headings):
            if h == "###" and (i == 0 or headings[i-1] not in ["##", "###"]):
                # This is a very basic check, just to ensure they don't start with H3
                if i == 0:
                    report["errors"].append("Article starts with H3 instead of H1 or H2.")
                    report["passed"] = False

    def _check_cta(self, content: str, metadata: Dict, report: Dict):
        # Flexible patterns from metadata or defaults
        cta_patterns = metadata.get("cta_patterns", [
            r"buy now", r"get started", r"contact us", r"click here", 
            r"sign up", r"learn more", r"download", r"try for free"
        ])
        
        found = False
        for pattern in cta_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                found = True
                break
        
        if not found:
            report["errors"].append("No Call to Action (CTA) detected.")
            report["passed"] = False

    def _check_links(self, content: str, metadata: Dict, report: Dict):
        all_links = re.findall(r'\[.*?\]\((.*?)\)', content)
        internal_links = []
        external_links = []
        domain = metadata.get("domain", "").lower()

        for link in all_links:
            link_low = link.lower()
            # Validity check (basic regex for malformed URLs)
            if not link_low.startswith(("/", "#", "http", "mailto", "tel")):
                report["warnings"].append(f"Malformed link detected: {link}")

            # Heuristic
            is_external = link_low.startswith(("http", "mailto", "tel"))
            if domain and domain in link_low:
                internal_links.append(link)
            elif link_low.startswith(("/", "#")) or not is_external:
                internal_links.append(link)
            else:
                external_links.append(link)

        if len(internal_links) < 2:
            report["errors"].append(f"Found {len(internal_links)} internal links. Minimum 2 required.")
            report["passed"] = False
        
        if not external_links:
            report["warnings"].append("No external authority links found.")

    def _check_media(self, content: str, metadata: Dict, report: Dict):
        primary_keyword = metadata.get("main_keyword", "").lower()
        
        # Image count
        image_count = len(re.findall(r'!\[.*?\]\(.*?\)', content))
        if image_count < 7:
            report["errors"].append(f"Found {image_count} images. Minimum 7 required.")
            report["passed"] = False

        # Featured Image check
        images_info = metadata.get("images", [])
        has_featured = any(img.get("image_type") == "Featured Image" for img in images_info)
        if not has_featured:
            report["errors"].append("Missing 'Featured Image' in metadata assets.")
            report["passed"] = False

        # Alt text validation
        alts = re.findall(r'!\[(.*?)\]\(.*?\)', content)
        if alts:
            bad_alts = sum(1 for alt in alts if primary_keyword not in alt.lower())
            if bad_alts > (len(alts) / 2):
                report["warnings"].append("Over 50% of images are missing the keyword in Alt text.")

    def _check_advanced_sections(self, content: str, metadata: Dict, report: Dict):
        # FAQ Improvements
        faq_section = re.search(r'#+.*FAQ.*?\n(.*?)(?=#+|$)', content, re.IGNORECASE | re.DOTALL)
        if faq_section:
            faq_text = faq_section.group(1)
            # Find questions by ? OR by specific H3/Bullet patterns
            items = re.findall(r'(\d+\.|\*|-|###)\s+.*', faq_text)
            q_marks = faq_text.count('?')
            q_count = max(len(items), q_marks)
            if q_count < 3:
                report["errors"].append(f"FAQ section too thin ({q_count} items). Minimum 3 required.")
                report["passed"] = False
        else:
            report["errors"].append("FAQ section missing.")
            report["passed"] = False
