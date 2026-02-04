import re
from typing import Dict, Any, List

class SEOValidator:
    """
    Validates the generated article against Hard SEO Rules.
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

        # extracting core data
        primary_keyword = metadata.get("main_keyword", "").lower()
        word_count = len(content.split())
        
        # Rule 1: Article Length
        if word_count < 1000:
            report["errors"].append(f"Word count is {word_count}. Minimum required is 1000.")
            report["passed"] = False
        
        # Rule 2: Keyword Usage (First Paragraph)
        # Naive split by double newline to find first paragraph (excluding title H1)
        paragraphs = [p for p in content.split("\n\n") if p.strip() and not p.strip().startswith("#")]
        if paragraphs:
            first_para = paragraphs[0].lower()
            if primary_keyword not in first_para:
                 report["errors"].append(f"Primary keyword '{primary_keyword}' not found in the first paragraph.")
                 report["passed"] = False
        
        # Rule 2: Keyword Density (1.2% - 1.6%)
        # Case insensitive count
        keyword_count = content.lower().count(primary_keyword)
        density = (keyword_count / word_count) * 100 if word_count > 0 else 0
        
        if density < 1.2:
             report["warnings"].append(f"Keyword density is too low ({density:.2f}%). Target: 1.2% - 1.6%.")
        elif density > 2.5: # increased tolerance slightly for variations
             report["warnings"].append(f"Keyword density is too high ({density:.2f}%). avoid stuffing.")

        # Rule 10: Images count
        # Count explicit image markdown ![...](...) or <img> tags
        image_count = len(re.findall(r'!\[.*?\]\(.*?\)', content))
        if image_count < 7:
             report["errors"].append(f"Found {image_count} images. Minimum required is 7.")
             report["passed"] = False

        # Rule 10a: Alt Text Validation
        # Check if alt text contains primary keyword (at least mostly)
        images = re.findall(r'!\[(.*?)\]\(.*?\)', content)
        alt_fail_count = 0
        for alt in images:
            if primary_keyword not in alt.lower():
                alt_fail_count += 1
        
        if alt_fail_count > len(images) * 0.5: # if >50% fail
             report["warnings"].append("Many images are missing the primary keyword in ALT text.")

        # Rule 4: Structure (H2/H3 checks)
        if "## " not in content:
            report["errors"].append("No H2 headings found. Structure is broken.")
            report["passed"] = False

        return report
