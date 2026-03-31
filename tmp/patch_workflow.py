import sys
import os
import re

target_file = r'e:\SEO-Writing-AI\src\services\workflow_controller.py'
with open(target_file, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# All SectionWriter.write calls need the memory parameters
# We'll look for used_claims=state.get("used_claims", []) and append the new ones

found = 0
new_lines = []
for line in lines:
    new_lines.append(line)
    if 'used_claims=state.get("used_claims", [])' in line:
        # Check if the next line or two already have previous_section_text to avoid double entries
        # for lookahead in lines[lines.index(line):lines.index(line)+3]:
        # Actually line index is risky in a loop, lets just check if it was already updated
        pass
        
        # Determine indentation
        indent = line[:line.find('used_claims')]
        
        # We'll check the next line in the ORIGINAL lines to see if it's already there
        # but let's just use a simple flag or check against the current state
        # Better yet, only add if 'previous_section_text' is not in the next 2 lines
        
# A safer way with regex sub
content = "".join(lines)

def patch_call(match):
    indent = match.group(1)
    suffix = match.group(2)
    # Check if 'previous_section_text' is already nearby (within 300 chars)
    # This is tricky because content is global. 
    # Let's just do a manual search/replace for the ones that don't have it.
    return match.group(0)

# Pattern to find 'used_claims' line and capture indentation
pattern = re.compile(r'(\s+)used_claims=state\.get\("used_claims", \[\]\),')

def replacer(match):
    indent = match.group(1)
    # If the text immediately following doesn't have previous_section_text, add it
    # We use a global counter to skip the 1st one if we already updated it manually
    # But let's just use a safe multiline match
    
    # Actually, the 1st one was already updated. 
    # Let's see if we can find the ones that are NOT followed by previous_section_text
    return f'{indent}used_claims=state.get("used_claims", []),\n{indent}previous_section_text=state.get("last_section_content", ""),\n{indent}previous_content_summary=state.get("full_content_so_far", ""),'

# We'll use a negative lookahead to only replace if previous_section_text doesn't exist within the next 100 chars
pattern_safe = re.compile(r'(\s+)used_claims=state\.get\("used_claims", \[\]\),(?!(?:.|\n){1,200}previous_section_text)')

new_content = pattern_safe.sub(replacer, content)

with open(target_file, 'w', encoding='utf-8') as f:
    f.write(new_content)

print("Patching complete.")
