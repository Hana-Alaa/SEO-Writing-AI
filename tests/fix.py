import os

file_path = r'e:\SEO-Writing-AI\src\services\content_generator.py'

with open(file_path, 'r', encoding='utf-8') as f:
    code = f.read()

target1 = '''                if clean_first_line == clean_heading or clean_first_line.startswith(clean_heading):
                    logger.info(f"[Assembler] Removing duplicate heading from content: '{first_line}'")
                    content = "\\n".join(content_lines[1:]).strip()'''

replacement1 = '''                if clean_heading and (clean_first_line == clean_heading or clean_first_line.startswith(clean_heading)):
                    logger.info(f"[Assembler] Removing duplicate heading from content: '{first_line}'")
                    content = "\\n".join(content_lines[1:]).strip()'''

target2 = '''            is_first_sec = (sections.index(sec) == 0)
            skip_heading = is_first_sec or heading.strip().lower() in ["introduction", "مقدمة", "مقدمه"]
            
            skip_heading = skip_heading and sec.get("section_type") == "introduction"

            if not skip_heading:'''

replacement2 = '''            is_first_sec = (sections.index(sec) == 0)
            
            is_intro_name = heading.strip().lower() in ["introduction", "مقدمة", "مقدمه"]
            is_intro_type = sec.get("section_type") == "introduction"
            
            skip_heading = is_first_sec or (is_intro_name and is_intro_type)
            
            if not heading.strip():
                skip_heading = True

            if not skip_heading:'''

code = code.replace(target1, replacement1)
code = code.replace(target2, replacement2)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Fixtures applied successfully.")
