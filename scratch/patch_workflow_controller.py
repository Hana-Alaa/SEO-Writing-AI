"""
Surgical patch for workflow_controller.py using LINE NUMBERS.
Removes orphaned lines 2556-2567 (1-indexed) and inserts correct code.
"""
import re as re_module
import shutil
import py_compile
import os

filepath = r"f:\SEO-Writing-AI\src\services\workflow_controller.py"

with open(filepath, "rb") as f:
    raw = f.read()

# Normalize to LF for processing
content = raw.decode("utf-8", errors="replace").replace("\r\n", "\n")
lines = content.split("\n")

print(f"Total lines before patch: {len(lines)}")

# Verify line 2556 is our orphan (0-indexed: 2555)
orphan_start = 2555  # 0-indexed (line 2556 1-indexed)
orphan_end = 2566    # 0-indexed inclusive (line 2567 1-indexed)

print(f"Lines to be removed ({orphan_start+1}-{orphan_end+1}):")
for i in range(orphan_start, orphan_end + 1):
    print(f"  {i+1}: {lines[i]!r}")

# Confirm this is actually the orphaned block
assert "section=outline[0]" in lines[orphan_start], f"Wrong start line! Got: {lines[orphan_start]!r}"
assert lines[orphan_end].strip() == ")", f"Wrong end line! Got: {lines[orphan_end]!r}"

# Replacement lines to insert after line 2555
repl = [
    "",
    "                    # Update global brand mention count",
    "                    state[\"brand_mentions_count\"] = state.get(\"brand_mentions_count\", 0) + res.get(\"brand_mentions_count\", 0)",
    "",
    "                results.append(res)",
    "",
    "        sections_content = {}",
    "        for res in results:",
    "            if isinstance(res, Exception):",
    "                logger.error(f\"Section failed: {res}\")",
    "                continue",
    "            if not res:",
    "                continue",
    "",
    "            if res.get(\"brand_link_used\"):",
    "                state[\"brand_link_used\"] = True",
    "",
    "            sections_content[res[\"section_id\"]] = res",
    "            if res.get(\"section_index\") == 0:",
    "                state[\"introduction_text\"] = res.get(\"generated_content\", \"\")",
    "",
    "            # Update global keyword count",
    "            primary_keyword = global_keywords.get(\"primary\", \"\")",
    "            if primary_keyword:",
    "                full_text_for_search = (res.get(\"heading_text\") or \"\") + \"\\n\" + res.get(\"generated_content\", \"\")",
    "                if any(ord(c) > 127 for c in primary_keyword):",
    "                    pattern = r'(?:[وبلفك]|ال)*{}'.format(re_module.escape(primary_keyword.lower()))",
    "                else:",
    "                    pattern = r'\\b{}\\b'.format(re_module.escape(primary_keyword.lower()))",
    "                matches = re_module.findall(pattern, full_text_for_search.lower())",
    "                state[\"global_keyword_count\"] = state.get(\"global_keyword_count\", 0) + len(matches)",
    "",
    "            state[\"full_content_so_far\"] = state.get(\"full_content_so_far\", \"\") + \"\\n\\n\" + res.get(\"generated_content\", \"\")",
    "",
    "            if PARALLEL_SECTIONS:",
    "                state[\"brand_mentions_count\"] = state.get(\"brand_mentions_count\", 0) + res.get(\"brand_mentions_count\", 0)",
    "",
    "        state[\"sections\"] = sections_content",
    "",
    "        # Local SEO Enforcement (Retry first section if area is missing)",
    "        area = state.get(\"area\")",
    "        if area and sections_content:",
    "            first_id = outline[0][\"section_id\"]",
    "            first_res = sections_content.get(first_id)",
    "",
    "            if first_res and area.lower() not in (first_res.get(\"generated_content\") or \"\").lower():",
    "                logger.info(f\"Local area '{area}' missing in first section. Retrying with enforcement...\")",
    "",
    "                retry_res = await self._write_single_section(",
    "                    title=title,",
    "                    global_keywords=global_keywords,",
    "                    section=outline[0],",
    "                    article_intent=intent,",
    "                    seo_intelligence=seo_intelligence,",
    "                    content_type=content_type,",
    "                    link_strategy=link_strategy,",
    "                    state=state,",
    "                    force_local=True,",
    "                    section_index=0,",
    "                    total_sections=len(outline),",
    "                    brand_advantages=seo_intelligence.get(\"market_analysis\", {}).get(\"market_insights\", {}).get(\"brand_advantages\", []),",
    "                    writing_blueprint=seo_intelligence.get(\"market_analysis\", {}).get(\"market_insights\", {}).get(\"writing_blueprint\", \"\")",
    "                )",
]

new_lines = lines[:orphan_start] + repl + lines[orphan_end + 1:]
print(f"Total lines after patch: {len(new_lines)}")

new_content = "\n".join(new_lines)
with open(filepath, "w", encoding="utf-8", newline="\n") as f:
    f.write(new_content)

print("Patch written. Running syntax check...")

tmp = filepath + ".syntax_check.py"
shutil.copy(filepath, tmp)
try:
    py_compile.compile(tmp, doraise=True)
    print("SYNTAX CHECK: PASSED - OK")
except py_compile.PyCompileError as e:
    print(f"SYNTAX CHECK: FAILED ✗ - {e}")
finally:
    os.remove(tmp)
