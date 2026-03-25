import os
import logging
import json
import asyncio
import shutil
import uuid
import re
import hashlib
import requests
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from PIL import Image
from io import BytesIO
from jinja2 import Template

from src.utils.link_manager import LinkManager
from src.utils.json_utils import recover_json

logger = logging.getLogger(__name__)

class ResearchService:
    """Service dedicated to brand discovery, web research, and SERP analysis."""

    def __init__(self, ai_client, work_dir: str):
        self.ai_client = ai_client
        self.work_dir = work_dir
        self.upload_dir = os.path.join(work_dir, "uploads")
        os.makedirs(self.upload_dir, exist_ok=True)

    async def run_brand_discovery(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep brand discovery:
        1. Crawls the homepage to discover all internal links.
        2. Scores each link by relevance to the primary keyword.
        3. Fetches the top relevant subpages.
        4. Uses AI to extract a factual brand context.
        """
        brand_url = state.get("brand_url")
        if not brand_url:
            urls = state.get("input_data", {}).get("urls", [])
            if urls:
                brand_url = urls[0].get("link")
        
        if not brand_url or not brand_url.startswith("http"):
            logger.info("Skipping brand discovery: No valid brand_url found. Implementing Pseudo-Brand fallback.")
            # Fallback: Derive a generic but professional brand persona from keywords
            primary_kw = state.get("primary_keyword", "المزود")
            article_lang = state.get("article_language", "ar")
            
            if article_lang == "ar":
                state["brand_name"] = f"منصة {primary_kw}"
                state["brand_context"] = f"منصة رائدة متخصصة في {primary_kw} وتقديم أفضل الخدمات الاحترافية في هذا المجال."
            else:
                state["brand_name"] = f"{primary_kw} Platform"
                state["brand_context"] = f"A leading platform specializing in {primary_kw} and providing professional services in the industry."
            return state

        primary_keyword = state.get("primary_keyword", "").lower()
        kw_tokens = [t for t in primary_keyword.split() if len(t) > 2]

        logger.info(f"Starting deep brand discovery for: {brand_url}")
        domain = LinkManager.domain(brand_url)
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

        try:
            # Discover Logo, Colors, and Brand Name (Only if images are enabled)
            # We always try to get the brand name as it helps the AI context
            brand_assets = await self._discover_logo_and_colors(brand_url, state)
            if brand_assets:
                # Only store image assets if requested
                if state.get("generate_images", True):
                    state["logo_path"] = brand_assets.get("logo_path")
                    state["brand_colors"] = brand_assets.get("brand_colors", [])
                    logger.info(f"Brand image assets added to state: Logo={state.get('logo_path')} | Colors={state.get('brand_colors')}")
                
                if brand_assets.get("brand_name"):
                    state["brand_name"] = brand_assets.get("brand_name")
                logger.info(f"Brand identity identified: {state.get('brand_name')}")
            else:
                # Fallback brand name if discovery fails
                state["brand_name"] = LinkManager.domain(brand_url).split('.')[0].capitalize()

            # Internal helper for fetching clean text
            def fetch_text(url: str) -> str:
                try:
                    r = requests.get(url, timeout=10, headers=headers)
                    if r.status_code != 200: return ""
                    s = BeautifulSoup(r.text, "html.parser")
                    for tag in s(["nav", "footer", "script", "style", "header", "aside", "form", "button", "iframe", "svg", "noscript"]):
                        tag.decompose()
                    main = s.find("main") or s.find(id="main") or s.find(class_="content") or s
                    blocks = []
                    current_heading = ""
                    current_paras = []
                    for tag in main.find_all(["h1", "h2", "h3", "p", "li"]):
                        text = tag.get_text(separator=" ", strip=True)
                        if len(text) < 40: continue
                        if tag.name in ("h1", "h2", "h3"):
                            if current_paras:
                                blocks.append((f"## {current_heading}\n" if current_heading else "") + "\n".join(current_paras))
                            current_heading = text
                            current_paras = []
                        else:
                            current_paras.append(text)
                    if current_paras:
                        blocks.append((f"## {current_heading}\n" if current_heading else "") + "\n".join(current_paras))
                    return "\n\n".join(blocks)[:6000]
                except Exception as ex:
                    logger.warning(f"Failed to fetch {url}: {ex}")
                    return ""

            def relevance_score(url: str, anchor: str) -> int:
                text = (url + " " + anchor).lower()
                score = 0
                
                # 1. Primary Keyword Boost
                if primary_keyword.lower() in text: score += 20
                score += sum(3 for t in kw_tokens if t in text and len(t) > 2)
                
                # 2. Content Type Boost (Prioritize Services/Products/Pillars)
                service_patterns = ["service", "solution", "product", "pillar", "offer", "خدمات", "حلول", "منتج"]
                if any(p in text for p in service_patterns): score += 15
                
                blog_patterns = ["blog", "article", "news", "guide", "مدونة", "مقال"]
                if any(p in text for p in blog_patterns): score += 5
                
                # 3. Area & Neighborhood Boost (MANDATORY LOCAL SEO)
                area = state.get("area", "").lower()
                neighborhoods = state.get("area_neighborhoods", [])
                
                if area and area in text:
                    score += 30 # Highest priority for exact local match
                elif neighborhoods:
                    if any(nb.lower() in text for nb in neighborhoods):
                        score += 15 # High priority for local neighborhood match
                
                # 4. Cultural/Regional Proximity Boost (AI-Driven)
                # Use culturally similar areas suggested by the strategy AI
                strategy = state.get("content_strategy", {})
                peer_areas = strategy.get("cultural_peer_areas", [])
                
                if peer_areas and any(p.lower() in text for p in peer_areas):
                    score += 10
                
                # 5. Core Utility Boost (Contact/Booking for conclusion)
                utility_patterns = ["contact", "book", "appointment", "tours", "تواصل", "حجز"]
                if any(p in text for p in utility_patterns): score += 5
                
                return score

            # Crawl homepage
            homepage_html = requests.get(brand_url, timeout=15, headers=headers)
            if homepage_html.status_code != 200: return state
            homepage_soup = BeautifulSoup(homepage_html.text, "html.parser")
            discovered_links = {}
            
            def extract_links(soup, base_url):
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    anchor = re.sub(r' \b\d{2}/\d{2}/\d{4}\b|\b(19|20)\d{2}\b', '', a.get_text(strip=True))
                    anchor = re.sub(r'\s+', ' ', anchor).strip()
                    full_url = urljoin(base_url, href)
                    if LinkManager.domain(full_url) != domain: continue
                    canon = LinkManager.canon_url(full_url)
                    if canon == LinkManager.canon_url(brand_url): continue
                    if not anchor or len(anchor) < 3 or len(anchor) > 80: continue
                    if anchor.lower() in {"click here", "read more", "learn more", "lets talk", "let's talk", "contact us", "see all", "اقرأ أكثر", "انقر هنا"}: continue
                    score = relevance_score(canon, anchor)
                    if canon not in discovered_links or score > discovered_links[canon][1]:
                        discovered_links[canon] = (anchor, score)

            extract_links(homepage_soup, brand_url)
            
            # Hub Crawling
            hub_links = [c for c, (a, s) in discovered_links.items() if any(k in c.lower() or k in a.lower() for k in ["service", "solution", "product", "offer", "خدمات", "حلول"])]
            for hub_url in hub_links[:2]:
                try:
                    hub_html = requests.get(hub_url, timeout=10, headers=headers)
                    if hub_html.status_code == 200: extract_links(BeautifulSoup(hub_html.text, "html.parser"), hub_url)
                except: pass

            sorted_links = sorted(discovered_links.items(), key=lambda x: x[1][1], reverse=True)
            top_links = sorted_links[:10]

            # Update state with resources
            if "internal_resources" not in state: state["internal_resources"] = []
            seen_canons = {LinkManager.canon_url(r['link']) for r in state["internal_resources"] if r.get("link")}
            added_count = 0
            for canon, (anchor, score) in sorted_links:
                if canon not in seen_canons:
                    state["internal_resources"].append({
                        "link": canon, 
                        "text": anchor, 
                        "score": score
                    })
                    seen_canons.add(canon)
                    added_count += 1
                if added_count >= 30: break

            # Index content
            brand_pages_index = {brand_url: fetch_text(brand_url)[:2500]}
            for canon, (anchor, score) in top_links:
                txt = fetch_text(canon)
                if txt: brand_pages_index[canon] = txt[:2500]
            state["brand_pages_index"] = brand_pages_index

            # AI Brand Context Extraction
            combined_text = "\n\n".join(f"[Page: {url}]\n{text}" for url, text in brand_pages_index.items())[:12000]
            if combined_text:
                context_prompt = f"""You are a Brand Intelligence Analyst.
Below image represents text scraped from multiple pages of a company's website.
Article Topic: "{primary_keyword}"

Website Content:
\"\"\"
{combined_text}
\"\"\"

Extract a detailed FACT SHEET related to "{primary_keyword}". Include services, processes, technologies, USPs, and target audience. 
Write the detailed fact sheet now:"""
                res = await self.ai_client.send(context_prompt, step="brand_discovery")
                brand_content = res["content"]
                metadata = res["metadata"]
                if state.get("workflow_logger"):
                    state["workflow_logger"].log_ai_call(step_name="brand_discovery", prompt=context_prompt, response=brand_content, tokens=metadata.get("tokens", {}), duration=metadata.get("duration", 0))
                
                state["brand_context"] = brand_content.strip()
            
            # Neighborhood Discovery
            area = state.get("area")
            if area:
                neighborhood_prompt = f"List the top 8-10 neighborhoods in '{area}' relevant to '{primary_keyword}'. Output ONLY a valid JSON array of strings."
                try:
                    nb_res = await self.ai_client.send(neighborhood_prompt, step="local_seo")
                    match = re.search(r'\[.*?\]', nb_res["content"], re.DOTALL)
                    if match:
                        state["area_neighborhoods"] = json.loads(match.group(0))
                except: pass

        except Exception as e:
            logger.error(f"Error during brand discovery: {e}", exc_info=True)
            
        return state

    async def run_brand_discovery_light(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Lightweight brand discovery:
        1. Discovers Logo and Colors.
        2. Performs a RAPID scrape of the homepage for internal links to populate the pool.
        """
        brand_url = state.get("brand_url")
        if not brand_url:
            urls = state.get("input_data", {}).get("urls", [])
            if urls: brand_url = urls[0].get("link")
        
        if brand_url and brand_url.startswith("http"):
            logger.info(f"Starting light brand discovery for: {brand_url}")
            brand_assets = await self._discover_logo_and_colors(brand_url, state)
            
            if brand_assets:
                if state.get("generate_images", True):
                    state["logo_path"] = brand_assets.get("logo_path")
                    state["brand_colors"] = brand_assets.get("brand_colors", [])
                
                if brand_assets.get("brand_name"):
                    state["brand_name"] = brand_assets.get("brand_name")
                
                # POPULATE INTERNAL RESOURCES (Even in Light Mode)
                # This ensures we have a pool of 3-6 internal links to distribute
                discovered_links = brand_assets.get("discovered_links", [])
                if discovered_links:
                    seen_canons = {LinkManager.canon_url(brand_url)}
                    # Add existing manual links
                    for r in state.get("internal_resources", []):
                        seen_canons.add(LinkManager.canon_url(r["link"]))
                    
                    # Add up to 8 most relevant discovered links
                    added_count = 0
                    for lnk in discovered_links:
                        canon = LinkManager.canon_url(lnk["link"])
                        if canon not in seen_canons:
                            state["internal_resources"].append({
                                "link": lnk["link"],
                                "text": lnk["text"],
                                "is_manual": False
                            })
                            seen_canons.add(canon)
                            added_count += 1
                        if added_count >= 8: break
                    logger.info(f"Discovered {added_count} relevant internal sub-pages from homepage.")

        # Ensure brand_context exists even if empty, or use manual voice
        if not state.get("brand_context"):
            state["brand_context"] = state.get("brand_voice_description", "Standard Brand Context (Light Discovery)")
            
        return state

    async def run_web_research(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Step 0: Perform deep web research for topic grounding."""
        primary_keyword = state["primary_keyword"]
        area = state.get("area")
        search_query = f"{primary_keyword} in {area}" if area else primary_keyword

        with open("assets/prompts/templates/seo_web_research.txt") as f:
            template = Template(f.read())

        async def _do_serp_call(query: str):
            research_prompt = template.render(primary_keyword=query)
            # Default to 3 results unless explicitly requested otherwise
            max_results = state.get("competitor_count", 3)
            res = await self.ai_client.send_with_web(prompt=research_prompt, max_results=max_results)
            raw = res["content"]
            metadata = res["metadata"]
            if state.get("workflow_logger"):
                state["workflow_logger"].log_ai_call(step_name="web_research", prompt=research_prompt, response=raw, tokens=metadata.get("tokens", {}), duration=metadata.get("duration", 0))
            return recover_json(re.sub(r"```json|```", "", raw).strip()) or {}

        serp_data = await _do_serp_call(search_query)
        if not serp_data.get("top_results") and area:
            serp_data = await _do_serp_call(primary_keyword)
        
        if not serp_data.get("top_results"):
            raise RuntimeError("SERP returned no top results")

        state["serp_data"] = serp_data
        state["seo_intelligence"] = serp_data
        return state

    async def run_hybrid_research(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Hybrid optimization for Core Mode:
        Combines SERP grounding AND strategic analysis into ONE AI call.
        """
        primary_keyword = state["primary_keyword"]
        area = state.get("area")
        search_query = f"{primary_keyword} in {area}" if area else primary_keyword
        
        logger.info(f"Running Hybrid SERP+Strategy Research for: {search_query}")
        
        # Load the combined prompt (we'll use a specialized template)
        try:
            with open("assets/prompts/templates/seo_hybrid_research.txt") as f:
                template = Template(f.read())
        except FileNotFoundError:
            # Fallback to web research template if hybrid doesn't exist yet
            with open("assets/prompts/templates/seo_web_research.txt") as f:
                template = Template(f.read())

        research_prompt = template.render(primary_keyword=search_query)
        
        # Default to 3 results unless explicitly requested otherwise
        max_results = state.get("competitor_count", 3)
        res = await self.ai_client.send_with_web(prompt=research_prompt, max_results=max_results)
        raw = res["content"]
        metadata = res["metadata"]
        
        if state.get("workflow_logger"):
            state["workflow_logger"].log_ai_call(
                step_name="hybrid_research", 
                prompt=research_prompt, 
                response=raw, 
                tokens=metadata.get("tokens", {}), 
                duration=metadata.get("duration", 0)
            )
            
        serp_data = recover_json(re.sub(r"```json|```", "", raw).strip()) or {}
        
        # Fallback if AI fails to return structured JSON
        if not serp_data.get("top_results"):
             serp_data = {"top_results": [{"title": primary_keyword, "url": "", "snippet": "Manual Fallback"}], "intent": "informational"}

        state["serp_data"] = serp_data
        state["seo_intelligence"] = {"serp_raw": serp_data, "strategic_analysis": serp_data}
        return state

    async def run_serp_analysis(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Step 0: Analyze SERP for competitors and intent."""
        serp_data = state.get("serp_data", {})
        primary_keyword = state.get("primary_keyword")
        with open("assets/prompts/templates/seo_serp_analysis.txt") as f:
            template = Template(f.read())
        
        paa_clean = [q.get("question", "") if isinstance(q, dict) else q for q in serp_data.get("paa_questions", [])[:10]]
        light_serp = {
            "paa": paa_clean,
            "lsi": serp_data.get("lsi_keywords", [])[:20],
            "related": serp_data.get("related_searches", [])[:15],
            "titles_pattern": [r.get("title", "")[:120] for r in serp_data.get("top_results", []) if isinstance(r, dict)][:state.get("competitor_count", 5)]
        }

        analysis_prompt = template.render(primary_keyword=primary_keyword, serp_data=json.dumps(light_serp))
        res = await self.ai_client.send(analysis_prompt, step="serp_analysis")
        metadata = res["metadata"]
        if state.get("workflow_logger"):
            state["workflow_logger"].log_ai_call(step_name="serp_analysis", prompt=analysis_prompt, response=res["content"], tokens=metadata.get("tokens", {}), duration=metadata.get("duration", 0))

        serp_insights = recover_json(res["content"]) or {}
        serp_insights["semantic_assets"] = {k: (serp_data.get(k) or []) for k in ["paa_questions", "lsi_keywords", "related_searches", "autocomplete_suggestions"]}

        # Keyword Clusters Fallback
        if not serp_insights.get("strategic_intelligence", {}).get("keyword_clusters"):
            lsi = serp_data.get("lsi_keywords", [])
            related = serp_data.get("related_searches", [])
            raw_fallback = [primary_keyword] + [str(k.get("keyword") if isinstance(k, dict) else k) for k in (lsi[:5] + related[:5])]
            serp_insights.setdefault("strategic_intelligence", {})["keyword_clusters"] = [{"cluster_name": "Semantic Cluster (Fallback)", "keywords": list(dict.fromkeys(raw_fallback))}]

        state["seo_intelligence"] = {"serp_raw": state.get("serp_data", {}), "strategic_analysis": serp_insights}
        return state

    async def _discover_logo_and_colors(self, url: str, state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extracts company logo URL and dominant colors from a website."""
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            r = requests.get(url, timeout=10, headers=headers)
            if r.status_code != 200: return None
            soup = BeautifulSoup(r.text, "html.parser")
            logo_url = None
            discovered_brand_name = None

            # Brand Name Extraction
            og_site = soup.find("meta", property="og:site_name")
            if og_site: discovered_brand_name = og_site.get("content")
            if not discovered_brand_name:
                title_tag = soup.find("title")
                if title_tag: 
                    discovered_brand_name = title_tag.get_text().split('|')[0].split('-')[0].split('–')[0].strip()
            if not discovered_brand_name:
                discovered_brand_name = LinkManager.extract_brand_name(url)

            # Logo Extraction (Only if images are enabled)
            logo_local_path = None
            colors = []
            is_svg = False
            
            num_images = state.get("num_images", 7)
            should_gen = state.get("generate_images", True)
            
            if should_gen and num_images > 0:
                logo_candidates = soup.find_all("img", alt=lambda x: x and 'logo' in x.lower())
                if not logo_candidates:
                     logo_candidates = soup.find_all("img", class_=lambda x: x and 'logo' in x.lower())
                
                if logo_candidates:
                    logo_url = urljoin(url, logo_candidates[0].get("src"))
                else:
                    og_image = soup.find("meta", property="og:image")
                    if og_image: logo_url = og_image.get("content")

                if not logo_url:
                    domain = urlparse(url).netloc
                    logo_url = f"https://www.google.com/s2/favicons?sz=128&domain={domain}"

                # Download and Save
                try:
                    lr = requests.get(logo_url, timeout=5, headers=headers)
                    if lr.status_code == 200:
                        img_data = lr.content
                        is_svg = logo_url.lower().endswith(".svg") or b"<svg" in img_data[:100].lower()
                        output_dir = state.get("output_dir", self.work_dir)
                        ext = ".svg" if is_svg else ".png"
                        logo_local_path = os.path.join(output_dir, "assets/images", f"brand_logo_{uuid.uuid4().hex[:8]}{ext}")
                        os.makedirs(os.path.dirname(logo_local_path), exist_ok=True)
                        with open(logo_local_path, "wb") as f: f.write(img_data)
                        colors = self._extract_colors_from_image(logo_local_path)
                except Exception as e:
                    logger.warning(f"Logo download failed: {e}")

            return {
                "logo_path": logo_local_path, 
                "brand_colors": colors, 
                "brand_name": discovered_brand_name, 
                "is_svg": is_svg
            }

        except Exception as e:
            logger.warning(f"Logo discovery failed: {e}")
        return None

    def _extract_colors_from_image(self, image_path: str) -> List[str]:
        """Helper to extract dominant colors from a local image file."""
        if not image_path or not os.path.exists(image_path): return []
        try:
            if image_path.lower().endswith(".svg"):
                with open(image_path, "r", encoding="utf-8", errors="ignore") as f:
                    hex_colors = re.findall(r'#(?:[0-9a-fA-F]{3}){1,2}', f.read())
                    meaningful = [c.lower() for c in hex_colors if c.lower() not in ['#ffffff', '#000000', '#fff', '#000']]
                    rgb = []
                    for hc in meaningful[:3]:
                        hc = hc.lstrip('#')
                        if len(hc) == 3: hc = ''.join([c*2 for c in hc])
                        rgb.append(f"rgb({int(hc[0:2], 16)},{int(hc[2:4], 16)},{int(hc[4:6], 16)})")
                    return rgb
            with Image.open(image_path) as img:
                img_small = img.convert("RGBA").resize((50, 50))
                colors = img_small.getcolors(2500)
                filtered = []
                if colors:
                    for count, color in sorted(colors, reverse=True):
                        if color[3] < 50 or sum(color[:3]) > 720 or sum(color[:3]) < 40: continue
                        filtered.append(f"rgb({color[0]},{color[1]},{color[2]})")
                        if len(filtered) >= 3: break
                return filtered
        except: return []
