import re
import logging
from urllib.parse import urlparse
from typing import Dict, Any, List, Optional, Set

logger = logging.getLogger(__name__)

class LinkManager:
    """Utility class for URL and link processing and deduplication."""

    @staticmethod
    def canon_url(url: str) -> str:
        """Standardize URL by removing fragments, queries, and trailing slashes."""
        if not url:
            return ""
        u = url.strip()
        u = re.sub(r"#.*$", "", u)
        u = re.sub(r"\?.*$", "", u)
        return u.rstrip("/").lower()

    @staticmethod
    def domain(url: str) -> str:
        """Extract the base domain from a URL (e.g., example.com)."""
        try:
            return urlparse(url).netloc.lower().replace("www.", "")
        except Exception:
            return ""

    @staticmethod
    def sluggify(text: str) -> str:
        """Generates a clean slug from English or Arabic text."""
        clean = re.sub(r'[^\w\s-]', '', text).strip().lower()
        return re.sub(r'[-\s_]+', '-', clean)

    @classmethod
    def extract_brand_name(cls, url: str) -> str:
        """Extracts a clean brand name from a URL."""
        dom = cls.domain(url)
        if not dom:
            return ""
        name = dom.split('.')[0]
        return name.capitalize()

    @classmethod
    def is_same_site(cls, url: str, brand_url: str) -> bool:
        """Check if two URLs belong to the same site/domain."""
        if not url or not brand_url:
            return False
        d1 = cls.domain(url)
        d2 = cls.domain(brand_url)
        return d1 == d2 or d1.endswith("." + d2) or d2.endswith("." + d1)

    @staticmethod
    def is_authority_domain(domain: str, allowed_domains: Optional[Set[str]] = None) -> bool:
        """Check if a domain is a credible authority (gov, edu, org)."""
        if not domain:
            return False
        if allowed_domains and domain in allowed_domains:
            return True
        return domain.endswith(".gov") or domain.endswith(".gov.sa") or domain.endswith(".edu") or domain.endswith(".org")

    @classmethod
    def normalize_url_for_dedup(cls, url: Any) -> str:
        """Normalize URL for deduplication by removing trailing slashes, fragments, and queries."""
        if not url:
            return ""
        
        if isinstance(url, dict):
            url = url.get("url") or url.get("link", "")
            if not url:
                return ""
        
        try:
            url = str(url).strip()
            parsed = urlparse(url)
            netloc = parsed.netloc.lower()
            if netloc.startswith("www."):
                netloc = netloc[4:]
            
            path = parsed.path
            if len(path) > 1 and path.endswith("/"):
                path = path[:-1]
                
            return f"{netloc}{path}"
        except Exception:
            return url.strip().lower()

    @classmethod
    def sanitize_section_links(cls, content: str, state: Dict[str, Any], brand_url: str, max_external: int = None) -> str:
        """Removes duplicate links and enforces external link limits within a text section."""
        if not content:
            return content

        if brand_url in {"None", "", None}:
            brand_url = ""

        if "used_all_urls" not in state:
            state["used_all_urls"] = set()
            for u in state.get("used_internal_links", []):
                state["used_all_urls"].add(cls.canon_url(u))
            for u in state.get("used_external_links", []):
                state["used_all_urls"].add(cls.canon_url(u))

        internal_set = state.get("internal_url_set", set()) or set()
        blocked_domains = state.get("blocked_external_domains", set()) or set()
        section_external_count = 0 
        
        global_used_external_links = state.get("used_external_links", [])
        global_used_external_count = len(global_used_external_links)
        
        if max_external is None:
            max_external = state.get("max_external_links", 6)

        pattern = r'\[([^\]]+)\]\(([^)]+)\)'

        def repl(m):
            nonlocal section_external_count, global_used_external_count
            text, raw_url = m.group(1), m.group(2).strip()

            if raw_url.lower() in {"none", "null", ""}:
                return text

            if not raw_url.startswith("http"):
                return text

            cu = cls.canon_url(raw_url)
            dom = cls.domain(cu)

            is_internal = cu in internal_set or (brand_url and cls.is_same_site(cu, brand_url))

            # Relaxed Deduplication:
            # We ONLY strip if the EXACT URL (Canonical) was already used globally.
            # We NO LONGER strip based on anchor text alone here.
            if cu in state["used_all_urls"]:
                return text

            if is_internal:
                state["used_all_urls"].add(cu)
                counts = state.setdefault("internal_link_counts", {})
                counts[cu] = counts.get(cu, 0) + 1
                return f"[{text}]({raw_url})"
            else:
                if dom in blocked_domains:
                    return text
                if section_external_count >= 2:
                    return text
                if global_used_external_count >= max_external:
                    return text
                
                state["used_all_urls"].add(cu)
                section_external_count += 1
                global_used_external_count += 1
                return f"[{text}]({raw_url})"

        return re.sub(pattern, repl, content)

    @classmethod
    def sanitize_links(
        cls,
        markdown: str,
        max_external: int = 3,
        max_brand: int = 1,
        brand_url: str = None,
        internal_url_set: set = None,
        blocked_domains: set = None,
        allowed_domains: set = None
    ) -> str:
        """Sanitizes links in markdown based on authority and limits."""
        if not markdown:
            return markdown

        internal_url_set = internal_url_set or set()
        blocked_domains = blocked_domains or set()
        allowed_domains = allowed_domains or set()

        used_external = set()
        brand_count = 0
        
        pattern = r'\[([^\]]+)\]\(([^)]+)\)'

        def repl(m):
            nonlocal brand_count
            text, raw_url = m.group(1), m.group(2).strip()

            if raw_url.lower() in {"none", "null", ""}:
                return text
            if not raw_url.startswith("http"):
                return text

            cu = cls.canon_url(raw_url)
            dom = cls.domain(cu)

            # Internal / Same Site
            if cu in internal_url_set or (brand_url and cls.is_same_site(cu, brand_url)):
                # If it's the specific brand_url, check brand count
                if brand_url and cls.canon_url(raw_url) == cls.canon_url(brand_url):
                    if brand_count >= max_brand:
                        return text
                    brand_count += 1
                return f"[{text}]({raw_url})"

            # External
            if dom in blocked_domains:
                return text
            if not cls.is_authority_domain(dom, allowed_domains):
                return text
            if cu in used_external:
                return text
            if len(used_external) >= max_external:
                return text

            used_external.add(cu)
            return f"[{text}]({raw_url})"

        return re.sub(pattern, repl, markdown)

    @classmethod
    def deduplicate_links_in_markdown(cls, markdown_text: str, brand_domain: str = "", max_internal: int = 6) -> str:
        """Final safety gate for link quality across the whole markdown."""
        if not markdown_text:
            return markdown_text

        date_pattern = re.compile(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b|\b(19|20)\d{2}\b')
        link_pattern = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')
        seen_urls = set()
        seen_anchors = set()
        internal_count = 0
        links_in_current_h2 = 0

        def is_internal(url):
            return brand_domain and brand_domain.lower() in url.lower()

        def is_seo_valuable(url):
            # purely administrative, legal or noise pages that offer zero marketing value to a reader.
            # We ALLOW 'login', 'signup', 'register', 'account' as they can be valid conversion points.
            junk_slugs = {'privacy', 'terms', 'cookies', 'legal', 'disclaimer'}
            path = urlparse(url).path.lower().rstrip('/')
            last_segment = path.split('/')[-1]
            return last_segment not in junk_slugs

        def replace_func(match):
            nonlocal internal_count, links_in_current_h2
            anchor_raw = match.group(1)
            url = match.group(2).strip()

            anchor = anchor_raw.strip()
            if not anchor:
                return ""

            core_url = cls.normalize_url_for_dedup(url)
            if core_url in seen_urls:
                return anchor

            if is_internal(url):
                if links_in_current_h2 >= 2:
                    return anchor
            
            if is_internal(url) and not is_seo_valuable(url):
                # SPECIAL EXCEPTION: If the link is to a contact/help/faq page AND it is at the end of the article, keep it.
                # Since we don't have block-level context here easily, we rely on the junk_slug reduction above.
                pass

            if is_internal(url) and internal_count >= max_internal:
                return anchor

            # Relaxed Anchor Deduplication:
            # We allow the same anchor (e.g., "احجز الآن") if the URL is different.
            # The `seen_anchors` set is no longer used for deduplication.
            
            seen_urls.add(core_url)
            # seen_anchors.add(anchor_key) # DISABLED: Allow duplicate anchors for different URLs
            if is_internal(url):
                internal_count += 1
                links_in_current_h2 += 1
            
            return f"[{anchor_raw}]({url})"

        parts = re.split(r'(^##\s+.*)', markdown_text, flags=re.MULTILINE)
        processed_parts = []
        for part in parts:
            if part.startswith('##'):
                links_in_current_h2 = 0
                processed_parts.append(part)
            else:
                processed_parts.append(link_pattern.sub(replace_func, part))

        return "".join(processed_parts)

    @classmethod
    def extract_competitor_domains(cls, serp_data: Dict[str, Any], brand_url: str = "") -> Set[str]:
        """Extracts competitor domains from SERP results."""
        blocked = set()
        brand_domain = cls.domain(brand_url)
        for r in serp_data.get("top_results", []):
            if isinstance(r, dict):
                d = cls.domain(r.get("url", ""))
                if d and d != brand_domain:
                    blocked.add(d)
        return blocked
