import hashlib
import os
import json
import re
import requests
import logging
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
        self.cache_dir = os.path.join("assets", "data", "style_cache")
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_cache_path(self, input_str: str) -> str:
        """Generates a file path for the cached blueprint."""
        hasher = hashlib.mdsafe_hash(input_str.encode()) if hasattr(hashlib, 'mdsafe_hash') else hashlib.md5(input_str.encode())
        filename = f"{hasher.hexdigest()}.json"
        return os.path.join(self.cache_dir, filename)

    async def extract_blueprint(self, reference_input: str) -> Dict[str, Any]:
        """
        Main entry point for style analysis. 
        Supports both raw content (HTML/MD) and URLs.
        """
        # 0. Check Cache
        cache_path = self._get_cache_path(reference_input)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    cached_data = json.load(f)
                    logger.info(f"Using cached style blueprint for: {reference_input[:50]}...")
                    return cached_data
            except Exception as e:
                logger.warning(f"Failed to read cache: {e}")

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
        
        # 3. AI-Driven Editorial DNA Analysis
        tactical_prompt = f"""You are a Master Content Architect & Linguistic Analyst. 
Analyze the following reference article and extract its "Editorial DNA" for replication. 
Note: Focus on "How" they write (structure, flow), NOT "What" they write (facts).

Reference Content (CLEANED):
\"\"\"
{reference_content[:15000]}
\"\"\"

Output STRICT JSON only:
{{
    "writing_tone": "The specific persona and vibe",
    "tonal_dna": {{
        "persona": "Expert/Casual Persona",
        "sentence_rhythm": "[Staccato/Flowing/Varied]",
        "audience_level": "[General/Professional/Stakeholder]",
        "signature_phrasing": ["list of 3-5 high-impact transition phrases or vocabulary choices used"]
    }},
    "editorial_dna": {{
        "paragraph_cohesion": "Description of how the author links ideas (e.g., uses thematic questions, direct address)",
        "keyword_weaving_style": "How keywords are integrated (e.g., subtly in the middle, bolded in conclusions)",
        "cta_transition_logic": "The style of the sentence immediately preceding a link",
        "human_touch_patterns": "Specific non-robotic patterns (e.g., using 'Imagine...', starting with 'Look...')"
    }},
    "few_shot_mirrors": [
        "Include 3 actual sentences from the reference that represent the peak style/quality to be mirrored"
    ],
    "cta_strategy": {{
        "density": "[low/medium/high]",
        "preferred_placement": "[after_h2/middle/at_end]",
        "total_ideal_count": 2
    }},
    "formatting_blueprint": {{
        "bolding_frequency": "[rare/moderate/frequent]",
        "list_usage": "[bulleted/numbered/minimal]",
        "special_elements": ["Quotes", "Comparison Tables", "FAQ Schema"]
    }}
}}"""

        try:
            res = await self.ai_client.send(tactical_prompt, step="style_extraction")
            blueprint = json.loads(re.search(r'\{.*\}', res["content"], re.DOTALL).group(0))
            
            # Merge automated structural detection
            blueprint["detected_elements"] = structure
            
            # 4. Save to Cache
            try:
                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(blueprint, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.warning(f"Failed to write cache: {e}")
                
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
