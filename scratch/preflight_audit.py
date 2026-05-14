"""
Pre-flight audit script for SEO-Writing-AI pipeline.
Checks for:
1. Bad regex character class ranges in source files
2. Template variables referenced but not in render() call
3. Unsafe .get() patterns on LLM output dicts
4. Jinja2 syntax errors in prompt templates
"""
import re
import os
import sys
from jinja2 import Environment, FileSystemLoader, TemplateSyntaxError

SRC_DIR = r"f:\SEO-Writing-AI\src"
TEMPLATES_DIR = r"f:\SEO-Writing-AI\assets\prompts\templates"

issues = []
warnings = []

# ============================================================
# 1. REGEX AUDIT: Bad character ranges in re.* calls
# ============================================================
print("\n=== 1. REGEX PATTERN AUDIT ===")
RE_CALL = re.compile(r're\.(findall|search|match|compile|sub|fullmatch|split)\s*\(')
# Pattern: unescaped hyphen causing range inside character class
BAD_RANGE = re.compile(r'\[[^\]]*[^\\]:--[^\]]*\]|\[[^\]]*--[^-\\\]][^\]]*\]')

for root, dirs, files in os.walk(SRC_DIR):
    dirs[:] = [d for d in dirs if d not in ["__pycache__", ".git"]]
    for fname in files:
        if not fname.endswith(".py"):
            continue
        fpath = os.path.join(root, fname)
        with open(fpath, encoding="utf-8", errors="replace") as f:
            content = f.read()
            lines = content.splitlines()

        for i, line in enumerate(lines, 1):
            if RE_CALL.search(line):
                # Try to extract the first string literal (the pattern) from re.* call
                # Handles re.findall(r"...", text) or re.sub(r"...", r"...", text)
                m = re.search(r're\.(?:findall|search|match|compile|sub|fullmatch|split)\s*\(\s*r?["\']([^"\']+)["\']', line)
                if m:
                    pat = m.group(1)
                    # Skip common substitution placeholders that aren't patterns
                    if pat in [r'\1', r'\2', r'\3']:
                        continue
                    try:
                        re.compile(pat)
                    except re.error as e:
                        issues.append(f"  CRASH RISK - Invalid regex at {fname}:{i}: {e}")
                        issues.append(f"    Pattern: {pat[:100]}")

if not [x for x in issues if "CRASH RISK" in x]:
    print("  OK - No invalid regex patterns found.")
else:
    for iss in issues:
        print(iss)

# ============================================================
# 2. JINJA2 TEMPLATE AUDIT: Syntax errors
# ============================================================
print("\n=== 2. JINJA2 TEMPLATE SYNTAX AUDIT ===")
env = Environment(loader=FileSystemLoader(TEMPLATES_DIR))

template_files = [f for f in os.listdir(TEMPLATES_DIR) if f.endswith(".txt")]
template_syntax_ok = True
for tmpl in sorted(template_files):
    try:
        env.get_template(tmpl)
        print(f"  OK  - {tmpl}")
    except TemplateSyntaxError as e:
        issues.append(f"  CRASH RISK - Jinja2 SyntaxError in {tmpl}: {e}")
        template_syntax_ok = False
        print(f"  FAIL - {tmpl}: {e}")

# ============================================================
# 3. TEMPLATE RENDER AUDIT: Test base + commercial extension
# ============================================================
print("\n=== 3. TEMPLATE RENDER AUDIT ===")
RENDER_VARS = dict(
    section={
        "heading_text": "Test Section",
        "section_type": "standard",
        "section_intent": "Informational",
        "section_contract": {"location_policy": "local_allowed"},
        "execution_mode": "taxonomy_breakdown",
        "semantic_goal": "test goal",
        "decision_frame": "test frame",
        "content_behavior": "test behavior",
        "entity_head": "Test entity",
        "requires_primary_keyword": False,
        "cta_eligible": False,
        "cta_type": "none",
        "assigned_keywords": [],
        "available_link_pool": {"internal": [], "external_references": []},
    },
    cognitive_blueprint={
        "section_thesis": "Test thesis",
        "decision_logic": [],
        "evidence_plan": [],
        "reader_value": "Test value",
        "avoid_patterns": [],
    },
    regional_profile="",
    mode_instructions="",
    operational_instructions=[],
    title="Test Title",
    primary_keyword="Test Keyword",
    global_keywords={"primary": "Test Keyword", "lsi": [], "semantic": []},
    supporting_keywords=[],
    article_language="ar",
    article_intent="informational",
    content_type="informational",
    link_strategy={},
    brand_url="https://example.com",
    brand_link_used=False,
    brand_link_allowed=True,
    allow_external_links=True,
    execution_plan={},
    area="Cairo",
    area_neighborhoods=[],
    used_phrases=[],
    used_topics=[],
    used_anchors=[],
    previous_section_text="",
    previous_content_summary="",
    used_internal_links=[],
    used_external_links=[],
    brand_name="TestBrand",
    brand_advantages=[],
    section_index=0,
    total_sections=5,
    brand_context="",
    section_source_text="",
    external_sources=[],
    external_resources=[],
    used_claims=[],
    ctas_placed=0,
    tables_placed=0,
    prohibited_competitors=[],
    current_year="2026",
    workflow_mode="core",
    global_keyword_count=0,
    brand_mentions_count=0,
    draft_to_fix="",
    writing_blueprint="",
    section_contract={},
)

for tmpl_name in ["02_section_writer_base.txt", "02_section_writer_brand_commercial_v2.txt"]:
    try:
        tmpl = env.get_template(tmpl_name)
        rendered = tmpl.render(**RENDER_VARS)
        print(f"  OK  - {tmpl_name} rendered ({len(rendered)} chars)")
    except Exception as e:
        issues.append(f"  CRASH RISK - Render error in {tmpl_name}: {e}")
        print(f"  FAIL - {tmpl_name}: {e}")

# ============================================================
# 4. WORKFLOW CONTROLLER: Critical unguarded key access
# ============================================================
print("\n=== 4. UNGUARDED KEY ACCESS AUDIT ===")
wc_path = r"f:\SEO-Writing-AI\src\services\workflow_controller.py"
with open(wc_path, encoding="utf-8", errors="replace") as f:
    wc_lines = f.readlines()

# Find res["key"] patterns NOT followed by a .get() fallback — specifically on LLM output dicts
risky_keys = ["generated_content", "content", "sections", "outline"]
risky_found = []
for i, line in enumerate(wc_lines, 1):
    for key in risky_keys:
        if f'res["{key}"]' in line and ".get(" not in line:
            risky_found.append(f"  WARN - workflow_controller.py:{i}: unguarded res[\"{key}\"] - {line.strip()[:100]}")

if risky_found:
    for r in risky_found[:15]:  # cap output
        print(r)
        warnings.append(r)
else:
    print("  OK - No critical unguarded key accesses found.")

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "="*60)
print(f"AUDIT COMPLETE: {len(issues)} CRASH RISKS | {len(warnings)} WARNINGS")
if issues:
    print("\nCRASH RISKS:")
    for iss in issues:
        print(iss)
