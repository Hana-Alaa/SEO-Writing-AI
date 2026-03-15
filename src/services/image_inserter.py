import re
from typing import List, Dict

class ImageInserter:

    async def insert(self, final_markdown: str, image_plan: List[Dict]) -> str:
        if not final_markdown or not image_plan:
            return final_markdown

        # === Featured after H1 ===
        featured = next((img for img in image_plan if img["image_type"] == "Featured"), None)

        lines = final_markdown.split("\n")
        new_lines = []
        h1_done = False

        for line in lines:
            new_lines.append(line)
            if not h1_done and line.startswith("# "):
                h1_done = True
                if featured:
                    new_lines.append("") # Spacer
                    new_lines.append(f'![{featured["alt_text"]}]({featured["local_path"]})')
                    new_lines.append("") # Spacer

        final_markdown = "\n".join(new_lines)

        # === Section Images ===
        sections_in_order = re.findall(r"<!-- section_id: (.*?) -->", final_markdown)
        first_section_id = sections_in_order[0] if sections_in_order else None

        for img in image_plan:
            if img["image_type"] == "Featured":
                continue

            section_id = img.get("section_id")
            marker = f"<!-- section_id: {section_id} -->"

            if marker in final_markdown:
                image_md = f'\n\n![{img["alt_text"]}]({img["local_path"]})\n'
                
                # SPECIAL HANDLING: If this is the first section and we have a featured image,
                # move this image AFTER the first paragraph of the section to avoid stacking.
                if featured and section_id == first_section_id:
                    parts = final_markdown.split(marker, 1)
                    post_marker = parts[1]
                    # Find the end of the first paragraph (two newlines or just one if it's short)
                    # We look for the first real block of text then the next double newline.
                    match = re.search(r"(\n\s*\n|\r\n\s*\r\n)", post_marker)
                    if match:
                        insert_pos = match.end()
                        final_markdown = parts[0] + marker + post_marker[:insert_pos] + image_md + post_marker[insert_pos:]
                    else:
                        # Fallback if no paragraph break found
                        final_markdown = final_markdown.replace(marker, marker + image_md, 1)
                else:
                    final_markdown = final_markdown.replace(marker, marker + image_md, 1)

        return final_markdown
