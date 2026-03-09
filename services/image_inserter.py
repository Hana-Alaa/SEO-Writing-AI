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

        # Prepend featured image if it exists
        if featured:
            new_lines.append(f'![{featured["alt_text"]}]({featured["local_path"]})')
            new_lines.append("") # Spacer

        for line in lines:
            new_lines.append(line)
            if line.startswith("# "):
                h1_done = True

        final_markdown = "\n".join(new_lines)

        # === Section Images ===
        for img in image_plan:
            if img["image_type"] == "Featured":
                continue

            marker = f"<!-- section_id: {img['section_id']} -->"

            if marker in final_markdown:
                # Add a bit more spacing for stacked images
                image_md = f'\n\n![{img["alt_text"]}]({img["local_path"]})\n'
                final_markdown = final_markdown.replace(
                    marker,
                    marker + image_md,
                    1
                )

        return final_markdown
