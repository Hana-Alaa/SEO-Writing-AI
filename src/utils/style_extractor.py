import re
import json
import logging
import requests
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class StyleExtractor:
    """
    Deconstructs a reference article (HTML or Markdown) into a 'Style Blueprint'.
    This blueprint guides the OutlineGenerator and SectionWriter to mimic the 
    structure, formatting, and tactical 'feel' of the reference.
    """

    def __init__(self, ai_client):
        self.ai_client = ai_client
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

    async def extract_blueprint(self, reference_input: str) -> Dict[str, Any]:
        """
        Main entry point for style analysis. 
        Supports both raw content (HTML/MD) and URLs.
        """
        logger.info("Extracting style blueprint...")

        # 1. Fetch content if it's a URL
        reference_content = reference_input
        if reference_input.strip().startswith("http"):
            logger.info(f"Fetching style reference from URL: {reference_input}")
            try:
                r = requests.get(reference_input, timeout=15, headers=self.headers)
                if r.status_code == 200:
                    reference_content = r.text
                else:
                    logger.warning(f"Failed to fetch style reference: Status {r.status_code}")
            except Exception as e:
                logger.error(f"Error fetching style reference URL: {e}")

        # 2. Structural Analysis (Element Sequence)
        structure = self._analyze_html_structure(reference_content)
        
        # 2. AI-Driven Tactical Analysis (Tone, CTA, Nuance)
        tactical_prompt = f"""You are a Master Content Architect. 
Analyze the following reference article and extract its "DNA" for structural replication.

Reference Content:
\"\"\"
{reference_content[:15000]}
\"\"\"

Output STRICT JSON only:
{{
    "writing_tone": "The specific persona and vibe (e.g., Authoritative Consultant, Enthusiastic Fan)",
    "tonal_dna": {{
        "persona": "Expert/Casual/... ",
        "sentence_rhythm": "[staccato/flowing/varied]",
        "audience_level": "[General/Enthusiast/Professional]",
        "forbidden_jargon": ["<list of overly technical terms to avoid if audience is general>"]
    }},
    "cta_strategy": {{
        "density": "[low/medium/high]",
        "preferred_placement": "[after_h2/middle/at_end]",
        "total_ideal_count": 2
    }},
    "formatting_blueprint": {{
        "bolding_frequency": "[rare/moderate/frequent]",
        "list_usage": "[bulleted/numbered/minimal]",
        "special_elements": ["Quotes", "Comparison Tables", "FAQ Schema"]
    }},
    "structural_skeleton": [
        {{"type": "H1", "detail": "Header content style"}},
        {{"type": "P", "detail": "Initial hook style"}},
        {{"type": "IMG", "detail": "Placement rule"}},
        {{"type": "H2", "detail": "Next section"}},
        {{"type": "TABLE", "detail": "Cols/Rows pattern"}},
        {{"type": "FAQ", "detail": "Number of Qs"}}
    ]
}}"""

        try:
            res = await self.ai_client.send(tactical_prompt, step="style_extraction")
            blueprint = json.loads(re.search(r'\{.*\}', res["content"], re.DOTALL).group(0))
            
            # Merge automated structural detection with AI tactical analysis
            blueprint["detected_elements"] = structure
            return blueprint
        except Exception as e:
            logger.error(f"Failed to extract style blueprint: {e}")
            return {}

    def _analyze_html_structure(self, html: str) -> List[str]:
        """
        Heuristic-based extraction of element sequence.
        """
        soup = BeautifulSoup(html, "html.parser")
        elements = []
        for tag in soup.find_all(['h1', 'h2', 'h3', 'p', 'ul', 'ol', 'table', 'blockquote', 'img', 'iframe']):
            elements.append(tag.name.upper())
        return elements
