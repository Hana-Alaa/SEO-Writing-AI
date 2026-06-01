import re
import os
import sys

# Ensure UTF-8 output for terminal
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

md_path = r"f:\SEO-Writing-AI\output\افضل-شركة-تصميم-مواقع-في-السعودية_20260601_105514\article_final.md"
html_template_path = r"f:\SEO-Writing-AI\output\شقق-للايجار-في-الرياض_20260514_105342\article_final.html"
output_path = r"f:\SEO-Writing-AI\output\افضل-شركة-تصميم-مواقع-في-السعودية_20260601_105514\article_final.html"



with open(md_path, 'r', encoding='utf-8') as f:
    md_content = f.read()

with open(html_template_path, 'r', encoding='utf-8') as f:
    html_template = f.read()

def md_to_html(text):
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2" target="_blank">\1</a>', text)
    text = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', text)
    return text

# Advanced parsing
lines = md_content.split('\n')
parsed_sections = []
current_section = {"title": "", "content": []}

for line in lines:
    line = line.strip()
    if not line or line.startswith('<!--'): continue
    
    if line.startswith('# '): continue
    
    if line.startswith('## '):
        if current_section["title"] or current_section["content"]:
            parsed_sections.append(current_section)
        current_section = {"title": line[3:], "content": []}
    elif line.startswith('### '):
        current_section["content"].append({"type": "h3", "text": line[4:]})
    elif line.startswith('- '):
        if current_section["content"] and current_section["content"][-1]["type"] == "ul":
            current_section["content"][-1]["items"].append(line[2:])
        else:
            current_section["content"].append({"type": "ul", "items": [line[2:]]})
    elif line.startswith('|'):
        if current_section["content"] and current_section["content"][-1]["type"] == "table":
            current_section["content"][-1]["rows"].append(line)
        else:
            current_section["content"].append({"type": "table", "rows": [line]})
    else:
        current_section["content"].append({"type": "p", "text": line})

if current_section["title"] or current_section["content"]:
    parsed_sections.append(current_section)

# Build HTML
body_html = ""
faq_html = ""
cta_html = ""

for i, sec in enumerate(parsed_sections):
    sec_html = ""
    is_faq = "أسئلة شائعة" in sec["title"]
    is_cta = (i == len(parsed_sections) - 1 or "كيف تبدأ" in sec["title"])

    if is_faq:
        faq_html += f'<h2>{sec["title"]}</h2>\n'
        curr_q = ""
        curr_a = []
        for item in sec["content"]:
            if item["type"] == "h3":
                if curr_q:
                    ans = "\n".join(curr_a)
                    faq_html += f'<div class="faq-item">\n    <h3>{curr_q}</h3>\n    <p>{ans}</p>\n</div>\n'
                curr_q = item["text"]
                curr_a = []
            elif item["type"] == "p":
                curr_a.append(md_to_html(item["text"]))
            elif item["type"] == "ul":
                ul = "<ul>" + "".join([f"<li>{md_to_html(li)}</li>" for li in item["items"]]) + "</ul>"
                curr_a.append(ul)
        if curr_q:
            ans = "\n".join(curr_a)
            faq_html += f'<div class="faq-item">\n    <h3>{curr_q}</h3>\n    <p>{ans}</p>\n</div>\n'
        continue

    if is_cta:
        cta_html += '<div class="cta-box">\n'
        cta_html += f'    <h2>{sec["title"]}</h2>\n'
        for item in sec["content"]:
            if item["type"] == "p":
                cta_html += f'    <p>{md_to_html(item["text"])}</p>\n'
            elif item["type"] == "ul":
                cta_html += "    <ul>" + "".join([f"<li>{md_to_html(li)}</li>" for li in item["items"]]) + "</ul>\n"
        cta_html += '</div>\n'
        continue

    if sec["title"]:
        sec_html += f'<h2>{sec["title"]}</h2>\n'
    for item in sec["content"]:
        if item["type"] == "h3":
            sec_html += f'<h3>{item["text"]}</h3>\n'
        elif item["type"] == "p":
            sec_html += f'<p>{md_to_html(item["text"])}</p>\n'
        elif item["type"] == "ul":
            sec_html += "<ul>\n" + "".join([f"    <li>{md_to_html(li)}</li>\n" for li in item["items"]]) + "</ul>\n"
        elif item["type"] == "table":
            sec_html += "<div class=\"table-container\" style=\"overflow-x: auto; margin-bottom: 2rem; border-radius: 12px; border: 1px solid var(--border); box-shadow: 0 4px 6px rgba(0,0,0,0.05);\">\n<table style=\"width: 100%; border-collapse: collapse; text-align: right; background: var(--surface);\">\n"
            for r_idx, row in enumerate(item["rows"]):
                if '---' in row: continue
                cells = [c.strip() for c in row.strip().strip('|').split('|')]
                sec_html += "    <tr style=\"border-bottom: 1px solid var(--border); transition: background 0.3s;\">\n"
                for cell in cells:
                    tag = "th" if r_idx == 0 else "td"
                    if tag == "th":
                        style = "padding: 16px; background-color: var(--secondary); color: white; font-weight: 700; border-bottom: 3px solid var(--primary);"
                    else:
                        style = "padding: 16px; color: var(--text-main);"
                    sec_html += f"        <{tag} style=\"{style}\">{md_to_html(cell)}</{tag}>\n"
                sec_html += "    </tr>\n"
            sec_html += "</table>\n</div>\n"
    body_html += sec_html

# Inject Title
title_match = re.search(r'^# (.*)', md_content)
title = title_match.group(1) if title_match else "افضل شركة تصميم مواقع في السعودية"
brand_to_wrap = "Creative Minds" if "Creative Minds" in title else ("قولدن هوست" if "قولدن هوست" in title else "")
# Replace page title in head
final_html = re.sub(r'<title>.*?</title>', f'<title>{title}</title>', html_template)

if brand_to_wrap:
    final_html = re.sub(r'<h1>.*?</h1>', f'<h1>{title.replace(brand_to_wrap, f"<span>{brand_to_wrap}</span>")}</h1>', final_html)
else:
    final_html = re.sub(r'<h1>.*?</h1>', f'<h1>{title}</h1>', final_html)

# Replace Article Content
replacement_content = body_html + faq_html + cta_html
final_html = re.sub(r'<article class="article-card">.*?</article>', 
                    f'<article class="article-card">\n{replacement_content}\n        </article>', 
                    final_html, flags=re.DOTALL)

with open(output_path, 'w', encoding='utf-8') as f:
    f.write(final_html)

print("Successfully generated HTML file.")
