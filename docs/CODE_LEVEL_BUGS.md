# Code-Level Technical Bugs (Not in PDF Report)

| # | Issue | File / Location | Fix |
|---|-------|-----------------|-----|
Done | 1 | **Duplicate class definitions** - `_enforce_paragraph_word_limit`, `OutlineGenerator`, `SectionWriter` defined twice (lines 1-216 duplicated) | `src/services/content_generator.py` | Removed shadowed block (lines 103–218): duplicate imports, duplicate `_enforce_paragraph_word_limit`, and incomplete `OutlineGenerator`; one active definition per symbol remains |
| 2 | **Circular import** - `api.py` imports from itself: `from src.app.api import app` | `src/app/api.py:11` | |
Done | 3 | **Hardcoded relative template paths** - `FileSystemLoader("assets/prompts/templates")` breaks when CWD ≠ project root | `src/services/workflow_controller.py:181`, `content_generator.py:107,223`, `research_service.py:640,729,783,970` | Added `src/config/paths.py` (`PROMPT_TEMPLATES_DIR` from project root); replaced all `FileSystemLoader` and `open()` calls in the three files with absolute paths |
| 4 | **State mutation on failed steps** - `state = result.get("data", state)` updates state even when step fails (non-critical) | `src/services/workflow_controller.py:330` | |
| 5 | **Language detection race condition** - API auto-detects from title regex vs Strategy service uses `langdetect` with 0.70 threshold | `src/app/api.py:162-168` vs `src/services/strategy_service.py:313-348` | |
| 6 | **`content_stage_only_mode` logic bug** - Skips assembly entirely instead of stopping after content writing | `src/services/workflow_controller.py:295-318` | |
| 7 | **Heading-only mode early stop** - Breaks loop without running `_assemble_final_output` properly | `src/services/workflow_controller.py:346-349` | |
| 8 | **Pricing extraction regex fails on Arabic-Indic digits** - Misses `١٠٠٬٠٠٠`, `110K`, `1.1M` formats | `src/services/workflow_controller.py:1290` | Currency-agnostic `_extract_observed_pricing_signals`: multi-currency tokens/symbols, Arabic-Indic digits, K/M and مليون/ألف scales; reject listing-count and discount-only %; require currency/phrase near number |
| 9 | **Link sanitization triple-processing** - 3 passes (`sanitize_section_links`, `sanitize_links`, `deduplicate_links_in_markdown`) cause drift | `src/utils/link_manager.py:89,164,224` | |
| 10 | **ValidationService state leakage** - Shared instance mutates `self.is_property_domain` across requests | `src/services/validation_service.py:150,155` | |
| 11 | **No request timeout config / circuit breaker** - Hardcoded 40s timeout, no per-step differentiation | `src/services/openrouter_client.py:41` | |
| 12 | **Security: Full traceback in API response** - Returns internal stack traces to client | `src/app/api.py:369-372` | |
| 13 | **Security: CORS allows all origins** - `allow_origins=["*"]` in production | `src/app/api.py:49` | |
| 14 | **Security: File upload without type validation** - Arbitrary file write to `output/uploads/` | `src/app/api.py:193-230` | |
| 15 | **Dead code / unused imports** - `detect_langs`, `Counter`, `PIL.Image`, duplicate blocks | `workflow_controller.py:26,32`, `research_service.py:15`, `content_generator.py:117-216` | |
| 16 | **Hardcoded model names in config** - `gpt-4.1`, `o4-mini:online`, `gemini-3.1-flash-image-preview` not env-configurable | `src/config/ai_config.py:23,28,32` | |
| 17 | **No typed state schema (Pydantic)** - 100+ state keys with no validation, typos cause silent failures | Across all services | |
| 18 | **pytest not in requirements** - 50+ test files but no test runner dependency | `requirements.txt` / `pyproject.toml` (missing) | |

| 19 | **Brand Discovery Problems** - Portfolio listing pages can contaminate project relevance scoring | Archive and listing pages may contain multiple project references in a single blob, causing unrelated projects to be ranked as target-market proof points.
Arabic and English project variants are not consistently merged | The same project can appear multiple times under different names, creating duplicate evidence and noisy planning outputs.
Ground Truth catalogs and page-level evidence can diverge | Important projects may exist in page evidence but be missing from derived catalogs, causing planning layers to select weaker proof points.
| 20 | **7ontent Strategy Problems** - Ground Truth planning slices can inherit catalog inaccuracies | If project or process catalogs contain gaps, strategy planning may promote incorrect proof points or incomplete execution flows.
Supported differentiators are not consistently structured | Technologies may appear as long narrative statements rather than reusable planning entities, making downstream consumption less reliable.
Target project selection remains sensitive to crawl coverage | Missing portfolio pages can significantly alter which projects strategy considers most relevant.
| 21 | **Outline Generation Problems** - Outline does not consume the same Ground Truth planning slice used by strategy | Important planning decisions can be lost between strategy and outline generation.
Process planning can disappear during outline generation | Strategy may identify valid execution steps while outline produces empty or incomplete process structures.
Proof-section planning is weakly connected to selected target projects | Relevant projects identified during planning are not consistently reflected in proof-oriented sections.
Heading quality warnings do not always influence outline revision decisions | Outline generation may continue despite structural warnings that indicate weak section planning.
| 22 | **Content Writing Problems** - Writers remain dependent on strategy and outline quality | Even when Ground Truth is available, weak planning can still produce shallow or generic content.
Market-relevant projects are not always prioritized in generated proof sections | The writer may select available projects that are less relevant to the target reader market.
Content coverage contracts are not consistently enforced | Required process depth, FAQ depth, and proof coverage can vary significantly between runs. 
| 23 | **Validation and Quality Gate Problems** - Validation still relies on multiple truth sources | Different validation paths can evaluate content using different evidence representations.
Trust-related false positives still occur | Commercial marketing language can sometimes be interpreted as unsupported trust or certification claims.
Project-name validation can misclassify descriptive text as project evidence | Long descriptive phrases may be incorrectly flagged as project references.
Quality warnings are not always prioritized by severity | Minor structural issues and critical content issues can appear with similar weight during review. 
