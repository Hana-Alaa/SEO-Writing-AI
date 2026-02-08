import os
import markdown
from jinja2 import Environment, FileSystemLoader

def render_html_page(final_result: dict):
    """
    Converts final_markdown into a full HTML page and saves it beside the article output.
    """

    output_dir = final_result["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    # 1. Convert Markdown → HTML
    html_content = markdown.markdown(
        final_result.get("final_markdown", ""),
        extensions=["fenced_code", "tables"]
    )

    # 2. Load HTML template
    env = Environment(
        loader=FileSystemLoader("templates"),
        autoescape=True
    )
    template = env.get_template("article.html")

    # 3. Render final HTML
    html = template.render(
        title=final_result.get("title", ""),
        meta_description=final_result.get("meta_description", ""),
        content=html_content
    )

    # 4. Save file
    html_path = os.path.join(output_dir, "page.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    return html_path
