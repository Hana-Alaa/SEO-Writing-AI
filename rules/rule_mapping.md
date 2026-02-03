# SEO Rule Mapping & Deconstruction
Simulate competitor content patterns based on general SEO best practices and common SERP structures.
Do NOT claim access to real Google results.
Do NOT mention competitor names or domains.
 
**Stages Legend:**
- **[SYS]**: System Prompt (Global Identity & Constraints)
- **[OUT]**: Outline Generation Step (Structure & Hierarchy)
- **[SEC]**: Section Writing Step (Content & Keywords)
- **[IMG]**: Image Generation Step
- **[ASM]**: Final Assembly/Validator (Formatting & Metadata)

| Rule ID | Rule Name | Category | Primary Stage | Constraint Type | Prompt Implementation Strategy |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **01** | Article Length | Structural | [SYS] / [SEC] | **Hard** | System: "Ensure ≥1000 words, expand naturally up to 3000–5000 words without repeating ideas or filler." Section: allocate content naturally per section. |
| **02** | Main Keyword Usage | Content | [SEC] | **Hard** | Include main keyword in first paragraph; Primary keyword density: ~1.2% – 1.6% per 1000 words Must scale proportionally with article length; use synonyms/semantic variations; include in some H2/H3 headings. |
| **03** | Secondary Keywords | Content | [SEC] | **Hard** | Use secondary keywords from Google autocomplete, People Also Ask, Related searches. Auto-generate if missing. Distribute naturally across sections. |
| **04** | Article Structure | Structural | [OUT] | **Hard** | Generate nested JSON outline of H2/H3. H4 optional for deep explanations. Ensure smooth transitions between sections. |
| **05** | CTA (Call to Action) | Content | [SEC] | **Hard** | Include clear CTA in first paragraph. Add additional CTAs where relevant. Examples: service promotion, contact numbers, offers, product links. Adjust tone per keyword intent (commercial vs informational),Introduction Constraints: Maximum 2 short paragraphs, CTA sentence must appear in paragraph 1, CTA tone must match keyword intent |
| **06** | Content Style by Intent | Content | [SYS] | **Soft** | Persona based on intent: Commercial = persuasive/sales, Informational = educational/neutral. |
| **07** | Internal Linking | Formatting | [SEC] | **Hard** | Place links naturally. Prioritize service/product pages & commercial pillar articles. Avoid clustering links. |
| **08** | Article Title (H1) | Structural | [OUT] | **Hard** | Title 60–70 chars, include main keyword. Commercial = sales-oriented. Optionally include numbers/years. |
| **09** | Meta Tags | Structural | [ASM] | **Hard** | Generate Meta Title (60–70 chars), Meta Description (action-oriented), optional Meta Keywords (primary + secondary). |
| **10** | Images | Formatting | [IMG] | **Hard** | Include ≥7 images, including a featured image. ALT text must contain primary keyword but vary phrasing/semantic variations. Optimize size & dimensions. |
| **11** | Additional Content Enhancements (Lists/Tables/Schema) | Formatting | [ASM] | **Soft** | Use bullet/numbered lists and comparison tables where relevant. Apply schema markup (Article, FAQ) in final assembly. |
| **12** | External Links | Formatting | [SEC] | **Soft** | Link to authoritative sources. Never link to competitors targeting same keyword. Ensure links add value. |
| **13** | Writing Quality & Logic | Content | [SYS] | **Soft** | Active voice, short paragraphs (2–4 lines), logical flow, no fluff/repetition. Each paragraph must contain a complete idea. Add concise concluding paragraph with final CTA. |
| **14** | FAQ Section | Structural | [OUT] / [SEC] | **Hard** | Outline must include FAQ H2 section. Generate 4–6 unique FAQs per article with keyword variations. Apply FAQ Schema in assembly. |
| **15** | User Intent Recognition | Content | [SYS] | **Hard** | Classify keyword intent (Transactional/Commercial/Informational/Comparative). Adjust tone, structure, CTA placement. |
| **16** | EEAT Requirements | Content | [SYS] | **Soft** | Include expert opinions, real examples, statistics when relevant. Maintain authoritative & reader-focused tone. |
| **17** | Plagiarism Avoidance | Content | [SYS] | **Hard** | Content 100% original. Never copy/rephrase. Each section must add new value. |
| **18** | Permalink Structure | Structural | [ASM] | **Hard** | Short, readable URL containing main keyword. Suggest manual fix if CMS alters it. Avoid unnecessary parameters. Fully aligned with Google SEO guidelines. |
| **19** | Competitor Analysis | Content | [Pre-Processing] | **Hard** | Before writing, analyze top-ranking pages for primary keyword: content depth, headings, meta tags, media, keyword usage, unique value, gaps. Ensure content exceeds competitors in coverage, structure, and actionability. |


## Logic Flow Strategy

1. **Pre-Processing**:
    - Classify Intent (Rule 15, 6).
    - Perform Competitor Analysis (Rule 19).
    - Select System Prompt Variant (Commercial vs Informational).

2. **Stage 1 (Outline Generation)**:
    - Enforce Rules: 4 (Hierarchy), 8 (H1), 14 (FAQ outline), 19 (Gap Analysis via LLM reasoning).

3. **Stage 2 (Section Loop)**:
    - Iterate through H2s & H3s.
    - Enforce Rules: 2 (Main KW), 3 (Secondary KW), 5 (CTA), 7 (Internal Links), 11 (Lists/Tables).

4. **Stage 3 (Images)**:
    - Enforce Rule 10 (7+ images, Alt Text variations).

5. **Stage 4 (Assembly & Metadata)**:
    - Enforce Rule 9 (Meta Tags), 18 (Permalink).
    - Apply schema markup for FAQ/Article where needed (Rule 11 & 14).
