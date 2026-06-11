# SEO Writing AI — Pipeline Step Audit Report

> **Purpose:** Step-by-step technical audit (code + run evidence). No fixes proposed here.  
> **Primary evidence runs:**
> - `output/افضل-شركة-تصميم-مواقع-في-السعودية_20260609_234736` — through **outline** (heading-only inspect)
> - `output/افضل-شركة-تصميم-مواقع-في-السعودية_20260609_143818` — through **section writing** (full article path)
> - **Keyword / area / brand:** افضل شركة تصميم مواقع في السعودية · السعودية · https://cems-it.com/ (Creative Minds)

---

## Pipeline Map (execution order)

| # | Step name (`workflow_controller`) | Entry function | LLM? |
|---|-----------------------------------|----------------|------|
| 0 | `analysis_init` | `_step_0_init` | No |
| 1 | `brand_discovery` | `_step_brand_discovery_router` | No (HTTP crawl only) |
| 2 | `web_research` | `_step_web_research_router` → `ResearchService.run_web_research` | Yes + live web |
| 3 | `serp_analysis` | `_step_serp_analysis_router` → `ResearchService.run_serp_analysis` | Yes |
| 4 | `intent_title` | `StrategyService.run_intent_title` | Yes |
| 5 | `style_analysis` | `StrategyService.run_style_analysis` | Light / optional |
| 6 | `content_strategy` | `StrategyService.run_content_strategy` | Yes |
| 7 | `outline_generation` | `_step_1_outline` | Yes |
| 8 | `content_writing` | `_step_2_write_sections` | Yes (per section) |
| 9+ | `global_coherence`, `assembly`, `render_html`, … | (out of scope for this doc) | Yes |

---

# Step 1 — Brand Discovery

## Purpose

Discover and extract all brand-owned evidence from the target website.

## Responsibilities

- Brand identification
- Website crawling
- Page classification
- Evidence extraction
- Ground Truth generation
- Evidence inventory generation
- Evidence boundaries generation

## Current Architecture

```text
Brand URL
→ Crawl Pages
→ Extract Raw Evidence
→ Build Evidence Cards
→ Build Ground Truth
→ Build Derived Catalogs
→ Build Inventory
→ Build Claim Boundaries
→ Output Discovery Package
```

**Code entry:** `AsyncWorkflowController._step_brand_discovery_router()` — `src/services/workflow_controller.py` (~L543–765)

**Note:** This step is **deterministic** (no LLM). Duration run `234736`: **40.04s**, **0 tokens**.

## Problems

### BD-01 — Limited Crawl Coverage

**Description:** The crawler operates under a page budget and may skip valid high-value pages.

**Observed Impact / Example:** Billion project present in some runs, missing in others.

**Result:** Ground Truth changes between runs.

**Evidence (234736):** 8 HTTP fetches / `max_pages` default 10. Billion absent from entire GT (12 logical pages).

**Code:** `BrandEvidenceService.enrich_brand_internal_resources()` — `brand_evidence_service.py` ~L2285+

---

### BD-02 — Archive / Listing Page Evidence Weighting

**Description:** Archive pages containing multiple projects are treated similarly to portfolio detail pages. Archive and listing pages often contain valuable discovery signals and additional project references. However, these pages currently participate in project relevance scoring similarly to dedicated project-detail pages. Archive pages should remain part of discovery, but should not contribute equal proof weight to project relevance scoring.

**Observed Impact / Example:** `ar/portfolio-type/المواقع-الإلكترونية` contains many projects in one page.

**Result:** Project relevance ranking can be distorted when multiple projects appear within a single archive page.

**Evidence (234736):** Archive URL in `crawled_urls`; 2376 chars, 34 semantic sections.

---

### BD-03 — Duplicate Arabic / English Portfolio Pages

**Description:** The same project may be crawled multiple times.

**Observed Impact / Example:** Baddel/بدل, AUC Business Forward / auc-busines-forward.

**Result:** Duplicate evidence and noisy catalogs.

**Evidence (234736):** Both Baddel EN+AR and both AUC slugs crawled.

---

### BD-04 — Crawl Non-Determinism

**Description:** Different runs can crawl different page sets.

**Observed Impact / Example:** 16 pages vs 12 pages vs 8 pages.

**Result:** Ground Truth varies across executions.

---

### BD-05 — Multiple Sources Of Truth (Critical)

**Description:** Discovery outputs can drift from each other.

**Observed Impact / Example:** Inventory, Boundaries, Ground Truth, Ground Truth Data.

**Result:** Different downstream layers may see different facts.

**Clarification:** Multiple artifacts are **by design**; the bug is **contradictory flags** (e.g. `inventory.projects_available=true` vs `gt_data.claim_boundaries.projects_available=false`).

**Code:** `build_brand_evidence_inventory`, `build_brand_evidence_boundaries`, `build_brand_ground_truth_data` — `brand_evidence_service.py`

---

### BD-06 — Derived Catalog Noise

**Description:** Catalogs may contain phrases that are not actual projects.

**Observed Impact / Example:** `Technology Used`, `positioned iPluto as...`

**Result:** Incorrect project candidates enter planning.

**Evidence (234736):** Present in `brand_ground_truth.md` Observed Projects catalog.

---

### BD-07 — Shallow Evidence From Listing-Derived Cards

**Description:** Some project cards are created from listing pages only.

**Observed Impact / Example:** No dedicated page fetch.

**Result:** Lower confidence proof points.

**Evidence (234736):** 12 GT pages from 8 crawls — Ipluto / ارتقاء appear without dedicated `crawled_urls` entry.

---

### BD-08 — Catalog Generation Drift

**Description:** Catalog generation can diverge from the underlying page evidence. Important entities may exist in crawled pages but fail to appear in derived catalogs.

**Observed Impact / Example:** Billion present in portfolio page evidence (other runs) but missing from `catalogs.projects`.

**Result:** Planning layers may select weaker or incorrect proof points.

**Distinct from BD-06:** omission of valid entities, not insertion of junk.

---

### BD-09 — Discovery Outputs Are Not Versioned

**Description:** Discovery artifacts are generated independently without explicit traceability between crawled pages, catalogs, inventories, and ground truth outputs.

**Observed Impact / Example:** Difficult to determine which crawl snapshot produced a given catalog or planning slice.

**Result:** Root-cause analysis harder when outputs differ between runs.

**Type:** Observability / ops gap (not a functional crawl bug).

---

### BD-10 — No Evidence Confidence Scoring

**Description:** Evidence extracted from different page types is treated similarly without a confidence or reliability weighting model.

**Observed Impact / Example:** Portfolio detail, service, homepage, and archive pages contribute with similar influence.

**Result:** Lower-quality evidence competes with stronger evidence during ranking and catalog derivation.

**Type:** Architecture gap; overlaps BD-02 and BD-07.

---

## Step 1 — Run Evidence Summary (`234736`)

| Metric | Value |
|--------|--------|
| Duration | 40.04s |
| Tokens | 0 |
| Brand | Creative Minds (`explicit_input`) |
| HTTP crawls | 8 |
| GT pages analyzed | 12 |
| Files written | `brand_ground_truth.md`, `brand_page_knowledge_pack.md` |

---

# Step 2 — Web Research

## Purpose

Perform live Google SERP observation for the primary keyword to ground market structure, competitor patterns, and semantic assets.

## Responsibilities

- Compose search query from keyword + area + language
- Call LLM with **live web search** enabled
- Parse strict JSON SERP payload (top results, headings, structural stats)
- Store raw SERP in state for downstream analysis

## Current Architecture

```text
primary_keyword + area
→ compose_search_query()
→ render seo_web_research.txt
→ ai_client.send_with_web()
→ recover_json → serp_data
→ state["serp_data"], state["seo_intelligence"] (initial)
```

**Code entry:** `_step_web_research_router` → `ResearchService.run_web_research()` — `research_service.py` ~L620+

**Prompt template:** `assets/prompts/templates/seo_web_research.txt`

## Inputs (state keys read)

`primary_keyword`, `area`, `article_language`, `competitor_count`, `workflow_logger`

## Outputs (state keys written)

`serp_data`, `seo_intelligence` (partial), `last_step_*`, logged AI call `web_research`

## Run Evidence (`234736`)

| Metric | Value |
|--------|--------|
| Duration | 44.81s (step) / 45.19s (total) |
| Tokens | 28,239 |
| Query | افضل شركة تصميم مواقع في السعودية |
| Top results | 3 service/agency pages (bs.net.sa, is.net.sa, promksa.com) |
| PAA / related / autocomplete | Empty (`not_observed`) |
| LSI | From page content (3 clusters) |
| `observed_notes` | Page 1 dominated by company service sites; no informational guides |

## Problems

| ID | Problem | Evidence | Impact | Confidence |
|----|---------|----------|--------|------------|
| **WR-01** | SERP page-1 is service-dominated for commercial KW | 3/3 results are agency homepages | Outline/strategy bias toward service-page structure | High |
| **WR-02** | Empty PAA/related when modules not visible | All enrichment `not_observed` | FAQ/outline gap-filling relies on LLM inference later | Medium |
Design Note not problem | **WR-03** | No brand-site separation at this step | Brand URL already in state but not excluded from SERP logic | OK by design — but commercial articles mix SERP + brand later | Low |
| **WR-04** | Hardcoded relative prompt path | `open("assets/prompts/templates/seo_web_research.txt")` | Breaks if CWD ≠ project root | Medium |
| **WR-05** | Single primary query; fallback only if zero results | 1 attempt in 234736 | Thin SERP if first query fails partially | Medium |

## Not blamed on Web Research

- Brand GT completeness (Step 1)
- Intent classification (Step 4)
- Strategy proof points (Step 6)

---

# Step 3 — SERP Analysis

## Purpose

Transform raw SERP JSON into structured market intelligence **without brand identity leakage**.

## Responsibilities

- Build neutral SERP payload (no brand fields)
- Fetch competitor heading structures (HTTP scrape top 3 URLs)
- LLM analysis via brand-unaware prompt
- Apply intent firewall + deterministic structural stat overrides
- Build `serp_outline_brief` for outline generator

## Current Architecture

```text
serp_data
→ fetch competitor headers (ScraperUtils)
→ render seo_serp_analysis_observed_v2.txt
→ LLM serp_analysis
→ _apply_serp_intent_firewall
→ merge → state["seo_intelligence"]
→ build_serp_outline_brief()
```

**Code entry:** `_step_serp_analysis_router` — `workflow_controller.py` ~L1398; `ResearchService.run_serp_analysis()` ~L757

**Prompt template:** `assets/prompts/templates/seo_serp_analysis_observed_v2.txt`

**Also in router:** `_extract_observed_pricing_signals(state)`

## Run Evidence (`234736`)

| Metric | Value |
|--------|--------|
| Duration | 8.92s |
| Tokens | 0 in summary (LLM call nested) |
| Confirmed intent | commercial (confidence 1.0) |
| Dominant page type | service |
| Avg H2 / H3 | 7.3 / 2.7 |
| FAQ presence ratio | 0.33 |

## Problems

| ID | Problem | Evidence | Impact | Confidence |
|----|---------|----------|--------|------------|
| **SA-01** | Competitor scrape may fail silently | `gather` returns None per URL | Weaker structural intelligence | Medium |
| **SA-02** | LLM market insights still generic when SERP thin | `content_gaps: []`, empty `brand_advantages` | Strategy lacks SERP differentiation signals | Medium |
| **SA-03** | Pricing signals depend on separate extraction | `_extract_observed_pricing_signals` in router | Pricing sections may lack observed data (ratio 0.0 in 234736) | Medium |
| **SA-04** | Hardcoded prompt path | Same CWD risk as WR-04 | Deployment fragility | Medium |

## Not blamed on SERP Analysis

- Brand project ranking (Step 1 + planning slice)
- Outline H3 mismatches (Step 7)

---

# Step 4 — Intent, Title & Style

## Purpose

Resolve user intent, content type (`brand_commercial` vs informational), and optionally refine title. Style step captures reference tone if provided.

## Responsibilities

- `run_intent_title`: title generator + SERP intent lock + `resolve_content_type`
- `run_style_analysis`: reference image / style text (often no-op)

## Current Architecture

```text
serp_data + serp_intent_evidence
→ TitleGenerator.generate() [LLM]
→ detect_intent_ai() [LLM]
→ reconcile → state["intent"], state["content_type"]
→ run_style_analysis (optional)
```

**Code:** `StrategyService.run_intent_title` / `run_style_analysis` — `strategy_service.py` ~L456+

## Run Evidence (`234736`)

| Metric | Value |
|--------|--------|
| intent_title duration | 2.35s / 726 tokens |
| style_analysis | 0.01s (passthrough) |
| content_type | `brand_commercial` (inferred) |
| Title | Uses raw user title (heading-only run) |

## Problems

| ID | Problem | Evidence | Impact | Confidence |
|----|---------|----------|--------|------------|
| **IT-01** | Language detection split across API vs Strategy | API regex on title vs `resolve_article_language` in init | Rare `ar`/`en` mismatch | Medium |
Architecture Complexity not problem| **IT-02** | Multiple intent reconcilers | title intent + AI classifier + SERP lock | Hard to trace why `brand_commercial` won | Low |
| **IT-03** | Style step often empty | 0.01s in 234736 | No issue when no style reference; misleading step in metrics | Low |

## Not blamed here

- SERP quality (Step 2–3)
- Strategy content (Step 6)

---

# Step 5 — Content Strategy

## Purpose

Produce `content_strategy` JSON: angles, differentiators, proof points, `section_role_map`, conversion philosophy — grounded in SERP + brand boundaries + **GT planning slice (3C-A)**.

## Responsibilities

- Build GT planning slice from `brand_ground_truth_data`
- Render `00_content_strategy_brand_commercial_observed_v2.txt`
- LLM → JSON → normalize → `_apply_brand_evidence_boundaries`
- `apply_ground_truth_strategy_postfill` when fields empty

## Current Architecture

```text
seo_intelligence + brand_evidence_boundaries + ground_truth_planning_slice
→ LLM content_strategy (up to 3 retries)
→ normalize + boundary sanitization + postfill
→ state["content_strategy"], state["brand_strategy_provenance"]
```

**Code:** `StrategyService.run_content_strategy` — `strategy_service.py` ~L600+

**Prompt:** `assets/prompts/templates/00_content_strategy_brand_commercial_observed_v2.txt`

## Run Evidence (`234736`)

| Metric | Value |
|--------|--------|
| Duration | 7.30s / 5,622 tokens |
| `strategy_prompt_used_gt` | true (from planning slice in prompt) |
| Differentiators | React JS, WordPress, PHP, AI… (sentences) |
| Proof | بدل, Baddel, AUC, إبلوتو (slice-driven noise) |
| Process in role_map | 2 steps (matches incomplete slice) |

## Problems

| ID | Problem | Evidence | Impact | Confidence |
|----|---------|----------|--------|------------|
| **CS-01** | Strategy inherits planning slice bugs | Archive-derived targets in slice | Wrong proof in `supported_proof_points` | High |
| **CS-02** | LLM outputs prose lists not structured tech | Long Arabic sentences vs `["WordPress","PHP"]` | Writer/outline less deterministic | Medium |
| **CS-03** | Boundary sanitizer can misclassify Arabic phrasing | "اعتماد على تقنيات" vs certifications regex | Differentiators stripped in some runs | Medium |
| **CS-04** | Postfill only when empty | LLM wrong proof not corrected | My Progress-style errors if slice wrong | Medium |
| **CS-05** | `local_strategy` may imply market presence | Text mentions Saudi market projects | Reader context OK; watch for writer over-claim | Low |
| **CS-06** | Strategy Output Contract Is Weak Strategy outputs rely on natural language fields instead of strongly structured contracts | supported_differentiators, supported_proof_points, process guidance may be returned as free text rather than reusable entities | Downstream layers must reinterpret strategy decisions.
## Not blamed here

- Crawl missing Billion (Step 1)
- Outline not receiving slice (Step 7 — separate wiring gap)

---

# Step 6 — Outline Generation

## Purpose

Generate heading-only outline JSON (H2/H3 structure) aligned to buyer journey + content strategy.

## Responsibilities

- Select template (`01_outline_generator_heading_only_commercial_v2.txt` for brand commercial)
- Inject brand evidence inventory, buyer journey, content strategy
- LLM outline + validation + heading quality audit
- Optional outline repair (disabled in diagnostic runs)

## Current Architecture

```text
content_strategy + brand_evidence_inventory + buyer_journey
→ OutlineGenerator [LLM]
→ validate heading contract
→ audit_heading_outline_quality
→ state["outline"], state["outline_structure"]
```

**Code:** `_step_1_outline` — `workflow_controller.py` ~L1407+; `OutlineGenerator` — `content_generator.py`

**Prompt:** `assets/prompts/templates/01_outline_generator_heading_only_commercial_v2.txt`

**Note:** Stops workflow when `heading_only_mode=true` (run 234736).

## Run Evidence (`234736`)

| Metric | Value |
|--------|--------|
| Duration | 44.59s / 15,147 tokens |
| Sections | 9 (sec_01–sec_09) |
| Heading audit | 8 warnings (H3 mismatch, ENTITY_DRIFT sec_05) |
| GT planning slice in prompt | **No** — only inventory + strategy |
| sec_07 process subheadings | `[]` empty |
| sec_08 FAQ subheadings | 4 questions |

## Problems

| ID | Problem | Evidence | Impact | Confidence |
|----|---------|----------|--------|------------|
| **OL-01** | **No GT planning slice in outline prompt (3C-B not done)** | Diagnostic prompt blocks | Process/proof sections generic | **High** |
| **OL-02** | Strategy `section_role_map.process` not mapped to H3 subheadings | sec_07 empty despite good strategy text | Writer gets no step headings | High |
| **OL-03** | Heading audit warns but does not block | 8 warnings, `passed: true` | Weak structure proceeds | Medium |
| **OL-04** | Duplicate class definitions in `content_generator.py` | `OutlineGenerator` defined twice (L103 & L219) | Risk of editing dead code path | High (code hygiene) |
| **OL-05** | Template path relative to CWD | `FileSystemLoader("assets/prompts/templates")` | Same deployment risk | Medium |
| **OL-06** | No variation policy yet | Similar H2 patterns across runs | "Template feel" across articles | Medium |

## Not blamed here

- Section body quality (Step 8)
- GT crawl gaps (Step 1) — though outline can't fix missing Billion

---

# Step 7 — Content Writing (Section Writing)

## Purpose

Write each outline section sequentially (commercial) or parallel (informational) with contracts, GT writer context, link pools, and quality gates.

## Responsibilities

- Refresh brand evidence state (`_ensure_brand_evidence_state_current`)
- Build per-section contracts and commercial role enforcement
- `SectionWriter` LLM per section
- Safe-repair / quality gates → `quality_warnings.txt`

## Current Architecture

```text
outline + content_strategy + brand_page_knowledge_pack / GT writer blocks
→ for each section: _write_single_section [LLM]
→ validation / fulfillment checks
→ CONTENT_STAGE_QUALITY_GATE
→ state["sections"], article_content_draft.md
```

**Code:** `_step_2_write_sections` — `workflow_controller.py` ~L8623+

**Note:** Run `234736` did **not** execute this step (heading-only). Evidence below from **`143818`**.

## Run Evidence (`143818`)

| Metric | Value |
|--------|--------|
| Duration | 114.51s |
| Sections written | 9 (+ duplicate intro event in log) |
| Quality status | `needs_revision` (11 warnings) |

### Key quality warnings (`143818`)

| Warning | Section |
|---------|---------|
| `process_section_insufficient_steps` | sec_07 |
| `project_proof_missed_target_relevant_evidence` | sec_05 |
| Unsupported trust/certification claims | sec_01, sec_04, sec_07, sec_09 |
| `conclusion_missing_brand_url_cta` | sec_09 |
| `faq_preamble_removed` | sec_08 |
| `intro_missing_soft_cta` | sec_01 |

## Problems

| ID | Problem | Evidence | Impact | Confidence |
|----|---------|----------|--------|------------|
| **CW-01** | Writer depends on outline subheadings for process depth | sec_07 insufficient steps | Thin process sections | High |
| **CW-02** | Proof section misses target projects | sec_05 warning | No Baddel/Billion in body | High |
| **CW-03** | Fulfillment flags certification-like Arabic | Multiple sec_* trust warnings | Over-claim or gate noise | Medium |
| **CW-04** | Conclusion CTA URL not injected (by design post Safe-Repair) | sec_09 warning only | Manual CTA needed | Medium |
| **CW-05** | Sequential-only for commercial | `use_parallel = False` for brand_commercial | Slower but needed for link pool | Low (by design) |
| **CW-06** | Writer GT wiring separate from planning slice | Uses knowledge pack + contracts | Same facts gap as outline if discovery weak | Medium |

## Not blamed on Section Writing alone

- Empty process H3s (Step 6)
- Wrong strategy proof (Steps 1 + 5)
- Crawl missing pages (Step 1)

---

# Cross-Step Issue Map (quick reference)

| Symptom | Primary step | Secondary |
|---------|--------------|-----------|
| Billion missing | BD-01 | — |
| AUC/إبلوتو as Saudi proof | BD-02 | CS-01 |
| `projects_available` contradiction | BD-05 | CS planning slice |
| Outline sec_07 empty | OL-01, OL-02 | CS process |
| Article process thin | CW-01 | OL-02 |
| Proof section generic | OL-01 | CW-02, BD-08 |
| SERP all service pages | WR-01 | SA-01 |
| needs_revision article | CW-* | OL-* + BD-* |

---

# Audit Status

| Step | Report status | Evidence run |
|------|---------------|--------------|
| 1 Brand Discovery | **Complete** (user + validated) | 234736 |
| 2 Web Research | **Complete** | 234736 |
| 3 SERP Analysis | **Complete** | 234736 |
| 4 Intent / Title / Style | **Complete** | 234736 |
| 5 Content Strategy | **Complete** | 234736 |
| 6 Outline Generation | **Complete** | 234736 |
| 7 Content Writing | **Complete** | 143818 (234736 skipped) |
| 8+ Assembly / HTML | **Not started** | — |

---

# Can this audit be done well?

**Yes**, with these rules (used in this document):

1. **One primary run** for steps 1–7 outline (`234736`) plus **one full article run** for section writing (`143818`).
2. **Code entry points** verified per step — not guessed from prompt names alone.
3. **Problems scoped per step** — cross-step symptoms mapped explicitly in the table above.
4. **Distinguish** functional bugs (BD-01–08, OL-01) from observability gaps (BD-09, BD-10).

**Limitation:** Post-writing steps (`global_coherence`, `assembly`, `render_html`, images) are not yet audited in this file.

---

*Generated for internal pipeline review. Update per step as fixes land.*
