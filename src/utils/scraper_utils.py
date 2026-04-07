import logging
import httpx
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import re

logger = logging.getLogger(__name__)

class ScraperUtils:
    """Utility for extracting structural headings and key topics from external URLs."""
    
    @staticmethod
    async def fetch_headings_from_url(url: str, timeout: int = 15) -> List[Dict[str, str]]:
        """
        Fetches an external URL and extracts H1, H2, and H3 tags.
        Returns a list of dictionaries with 'tag' and 'text'.
        """
        if not url or not url.startswith("http"):
            return []
            
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        try:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, verify=False) as client:
                r = await client.get(url, headers=headers)
                if r.status_code != 200:
                    logger.warning(f"Failed to fetch {url}: Status {r.status_code}")
                    return []
                
                soup = BeautifulSoup(r.text, "html.parser")
                
                # Decompose noise
                for tag in soup(["script", "style", "nav", "footer", "aside", "header"]):
                    tag.decompose()
                
                headings = []
                for tag in soup.find_all(["h1", "h2", "h3"]):
                    text = tag.get_text(strip=True)
                    if len(text) > 10 and len(text) < 200: # Filter noise/too long strings
                        headings.append({
                            "tag": tag.name.upper(),
                            "text": text
                        })
                
                return headings
        except Exception as e:
            logger.error(f"Scraping error for {url}: {e}")
            return []

    @staticmethod
    def extract_common_themes(competitor_structures: List[List[Dict[str, str]]]) -> List[str]:
        """
        Heuristic: Identifies themes appearing in multiple competitors.
        For now, this is a placeholder. The AI actually performs the semantic grouping.
        """
        return []
