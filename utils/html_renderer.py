# import os
# import re
# import markdown
# from jinja2 import Environment, FileSystemLoader

# import logging

# logger = logging.getLogger(__name__)

# def render_html_page(final_result: dict):
#     output_dir = final_result["output_dir"]
#     os.makedirs(output_dir, exist_ok=True)
    
#     md_content = final_result.get("final_markdown", "")
#     logger.info(f"HTML Renderer received markdown length: {len(md_content)}")
    
#     if not md_content:
#         logger.warning("HTML Renderer received EMPTY markdown content!")
        
#     # Remove the first H1 (# Title) line if present, as it is already rendered in the HTML template header
#     lines = md_content.lstrip().splitlines()
#     if lines and lines[0].startswith("# "):
#         logger.info(f"Stripping H1 title from markdown: {lines[0]}")
#         md_content = "\n".join(lines[1:])


#     # 1. Convert Markdown → HTML
#     md_content = _normalize_markdown_tables(md_content)
#     try:
#         html_content = markdown.markdown(
#             md_content,
#             extensions=[
#                 "fenced_code",     
#                 "tables",          
#                 "toc",              
#                 "attr_list",       
#                 "smarty",          
#                 "md_in_html",       
#                 "footnotes",       
#             ],
#             output_format="html5"
#         )
#         logger.info(f"Converted HTML content length: {len(html_content)}")
#         html_content = _wrap_tables(html_content)
#     except Exception as e:
#         logger.error(f"Markdown conversion failed: {e}")
#         html_content = f"<p>Error converting markdown: {e}</p>"

#     # 2. Load HTML template
#     try:
#         env = Environment(
#             loader=FileSystemLoader("templates"),
#             autoescape=True
#         )
#         template = env.get_template("article.html")
#     except Exception as e:
#          logger.error(f"Template loading failed: {e}")
#          return None

#     # Use meta_title preferentially, fallback to title
#     page_title = final_result.get("meta_title") or final_result.get("title", "Untitled")

#     # 3. Render final HTML
#     # Robust Detection: Check if title contains Arabic characters
#     import re
#     has_arabic = bool(re.search(r'[\u0600-\u06FF]', page_title))
    
#     direction = "rtl" if has_arabic else "ltr"
    
#     # Dynamic Copyright based on direction/language
#     if direction == "rtl":
#         copyright_text = "© 2026 جميع الحقوق محفوظة"
#     else:
#         copyright_text = "© 2026 All Rights Reserved"

#     try:
#         html = template.render(
#             meta_title=page_title,
#             meta_description=final_result.get("meta_description", ""),
#             content=html_content,
#             lang=final_result.get("article_language", "ar"),
#             dir=direction,
#             copyright_text=copyright_text
#         )
#         logger.info(f"Final rendered HTML length: {len(html)}")
#     except Exception as e:
#         logger.error(f"Template rendering failed: {e}")
#         return None

#     # 4. Save file
#     html_path = os.path.join(output_dir, "page.html")
#     try:
#         with open(html_path, "w", encoding="utf-8") as f:
#             f.write(html)
#         logger.info(f"HTML saved to: {html_path}")
#     except Exception as e:
#         logger.error(f"File saving failed: {e}")

#     return html_path

# def _normalize_markdown_tables(md: str) -> str:
#     if not md:
#         return md

#     md = md.replace("\r\n", "\n")

#     # remove blank lines between consecutive table rows
#     pattern = re.compile(r'(\n[ \t]*\|[^\n]*\|[ \t]*\n)[ \t]*\n(?=[ \t]*\|)', re.MULTILINE)
#     prev = None
#     while prev != md:
#         prev = md
#         md = pattern.sub(r'\1', md)

#     # trim table lines
#     lines = []
#     in_fence = False
#     for line in md.split("\n"):
#         s = line.strip()
#         if s.startswith("```"):
#             in_fence = not in_fence
#             lines.append(line)
#             continue

#         if (not in_fence) and s.startswith("|") and "|" in s[1:]:
#             lines.append(s)
#         else:
#             lines.append(line)

#     return "\n".join(lines)

# def _wrap_tables(html: str) -> str:
#     if not html:
#         return html
#     return re.sub(
#         r'(<table>.*?</table>)',
#         r'<div class="table-wrapper">\1</div>',
#         html,
#         flags=re.IGNORECASE | re.DOTALL
#     )

import os
import re
import markdown
from jinja2 import Environment, FileSystemLoader
import logging

logger = logging.getLogger(__name__)


def _is_table_line(s: str) -> bool:
    s = s.strip()
    return s.startswith("|") and s.count("|") >= 2


def _normalize_delimiter_row(line: str) -> str:
    """
    Convert delimiter rows like:
    | :— | —: | :—: |   ->   | :--- | ---: | :---: |
    """
    s = line.strip()
    if not (s.startswith("|") and s.endswith("|")):
        return line

    cells = [c.strip() for c in s.strip("|").split("|")]
    if not cells:
        return line

    # delimiter-like only if all cells are made of colon + dash variants
    if not all(re.fullmatch(r":?[—–-]{1,}:?", c) for c in cells):
        return line

    fixed = []
    for c in cells:
        left = c.startswith(":")
        right = c.endswith(":")
        core = "---"
        fixed.append(f"{':' if left else ''}{core}{':' if right else ''}")

    return "| " + " | ".join(fixed) + " |"


def _normalize_markdown_tables(md: str) -> str:
    if not md:
        return md

    md = md.replace("\r\n", "\n").replace("\r", "\n")
    raw_lines = md.split("\n")

    # Pass 1: fix malformed table lines (emdash separator, trailing text after last pipe)
    lines = []
    in_fence = False

    for line in raw_lines:
        s = line.strip()

        if s.startswith("```"):
            in_fence = not in_fence
            lines.append(line)
            continue

        if not in_fence:
            # Check for table-like content anywhere in the line
            # e.g. "Prose text | cell 1 | cell 2 |"
            if "|" in s and s.count("|") >= 2:
                # Find the FIRST pipe that likely starts the table header
                # We assume a pipe followed by some text and another pipe is a table
                first_pipe = s.find("|")
                leading_text = s[:first_pipe].strip()
                table_row = s[first_pipe:].strip()

                if leading_text and table_row.startswith("|") and table_row.endswith("|"):
                    # Split them
                    lines.append(leading_text)
                    lines.append("") # Blank line before table
                    line = _normalize_delimiter_row(table_row)
                    lines.append(line)
                    continue
                elif leading_text and table_row.startswith("|"):
                    # Check for trailing text as well
                    last_pipe = table_row.rfind("|")
                    if last_pipe != -1:
                         actual_row = table_row[:last_pipe+1]
                         tail = table_row[last_pipe+1:].strip()
                         
                         lines.append(leading_text)
                         lines.append("")
                         actual_row = _normalize_delimiter_row(actual_row)
                         lines.append(actual_row)
                         if tail:
                             lines.append("")
                             lines.append(tail)
                         continue

            if _is_table_line(s):
                # Case 1: line is clear table row ending with |  → keep as-is after delimiter normalization
                if s.endswith("|"):
                    line = _normalize_delimiter_row(line)
                    lines.append(line)
                    continue

                # Case 2: line has trailing text after the last | that closes a table cell
                last_pipe = s.rfind("|")
                tail_candidate = s[last_pipe + 1:].strip()
                if tail_candidate:
                    table_part = s[:last_pipe + 1].rstrip()
                    table_part = _normalize_delimiter_row(table_part)
                    lines.append(table_part)
                    lines.append("")       
                    lines.append(tail_candidate)
                else:
                    line = _normalize_delimiter_row(line)
                    lines.append(line)
                continue

        lines.append(line)

    # Pass 2: enforce blank line before/after contiguous table blocks
    out = []
    in_fence = False
    in_table_block = False

    for line in lines:
        s = line.strip()

        if s.startswith("```"):
            if in_table_block and out and out[-1].strip() != "":
                out.append("")
            in_table_block = False

            in_fence = not in_fence
            out.append(line)
            continue

        is_tbl = (not in_fence) and _is_table_line(s)

        if is_tbl and not in_table_block:
            if out and out[-1].strip() != "":
                out.append("")  # blank before table
            in_table_block = True

        if (not is_tbl) and in_table_block:
            if out and out[-1].strip() != "":
                out.append("")  # blank after table
            in_table_block = False

        out.append(line)

    if in_table_block and out and out[-1].strip() != "":
        out.append("")

    # Pass 3: remove accidental blank lines between table rows
    normalized = "\n".join(out)
    normalized = re.sub(
        r"(\n[ \t]*\|[^\n]*\|[ \t]*\n)[ \t]*\n(?=[ \t]*\|)",
        r"\1",
        normalized,
        flags=re.MULTILINE
    )

    return normalized


def _wrap_tables(html: str) -> str:
    if not html:
        return html
    return re.sub(
        r"(<table>.*?</table>)",
        r'<div class="table-wrapper">\1</div>',
        html,
        flags=re.IGNORECASE | re.DOTALL
    )


def render_html_page(final_result: dict):
    output_dir = final_result["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    md_content = final_result.get("final_markdown", "")
    logger.info(f"HTML Renderer received markdown length: {len(md_content)}")

    if not md_content:
        logger.warning("HTML Renderer received EMPTY markdown content!")

    lines = md_content.lstrip().splitlines()
    if lines and lines[0].startswith("# "):
        logger.info(f"Stripping H1 title from markdown: {lines[0]}")
        md_content = "\n".join(lines[1:])

    md_content = _normalize_markdown_tables(md_content)

    try:
        html_content = markdown.markdown(
            md_content,
            extensions=["fenced_code", "tables", "toc", "attr_list", "smarty", "md_in_html", "footnotes"],
            output_format="html5"
        )
        html_content = _wrap_tables(html_content)
    except Exception as e:
        logger.error(f"Markdown conversion failed: {e}")
        html_content = f"<p>Error converting markdown: {e}</p>"

    try:
        env = Environment(loader=FileSystemLoader("templates"), autoescape=True)
        template = env.get_template("article.html")
    except Exception as e:
        logger.error(f"Template loading failed: {e}")
        return None

    page_title = final_result.get("meta_title") or final_result.get("title", "Untitled")
    has_arabic = bool(re.search(r'[\u0600-\u06FF]', page_title))
    direction = "rtl" if has_arabic else "ltr"
    copyright_text = "© 2026 جميع الحقوق محفوظة" if direction == "rtl" else "© 2026 All Rights Reserved"

    try:
        html = template.render(
            meta_title=page_title,
            meta_description=final_result.get("meta_description", ""),
            content=html_content,
            lang=final_result.get("article_language", "ar"),
            dir=direction,
            copyright_text=copyright_text
        )
    except Exception as e:
        logger.error(f"Template rendering failed: {e}")
        return None

    html_path = os.path.join(output_dir, "page.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"HTML saved to: {html_path}")
    return html_path