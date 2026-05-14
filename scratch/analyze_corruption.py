import os

PATH = r"f:\SEO-Writing-AI\src\services\content_generator.py"

with open(PATH, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find where the duplication starts and where it ends
# The tool inserted a block of imports and class def inside the class

# I'll just look for the first occurrence of "import json" and the second one.
first_json = -1
second_json = -1
for i, line in enumerate(lines):
    if "import json" in line:
        if first_json == -1:
            first_json = i
        else:
            second_json = i
            break

if first_json != -1 and second_json != -1:
    print(f"Found duplication at lines {first_json} and {second_json}")
    # The duplicated block starts at second_json
    # It probably goes until "class OutlineGenerator:" again.
    
    # I'll just reconstruct the file from my known state if possible, 
    # but I don't have the whole file.
    
    # Wait, I'll just remove the lines between the end of the previous valid code 
    # and the start of the next valid code.
    
    # Actually, I'll use a simpler heuristic: 
    # Delete everything between line 117 (where the first duplication started) 
    # and line 282 (where the actual generate method should be).
    
    # Let's find "async def generate"
    generate_line = -1
    for i, line in enumerate(lines):
        if "async def generate" in line:
            generate_line = i
            break
    
    if generate_line != -1:
        # Keep lines before 115 (approx) and lines after generate_line
        # But wait, I need the methods I just added!
        pass

# This is too risky without seeing the full content.
# I'll use run_command to output the first 500 lines to a file and read it.
