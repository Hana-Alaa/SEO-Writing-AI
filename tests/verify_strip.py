
import logging

# Mock logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def strip_title_logic(md_content):
    lines = md_content.lstrip().splitlines()
    if lines and lines[0].startswith("# "):
        logger.info(f"Stripping H1 title from markdown: {lines[0]}")
        md_content = "\n".join(lines[1:])
    return md_content

def test_strip():
    # Case 1: Standard
    md1 = """# My Title
This is the content.
## Section 1
"""
    res1 = strip_title_logic(md1)
    print(f"CASE 1:\n{res1}\n---\n")
    
    # Case 2: Leading newlines
    md2 = """

# My Title With Space
Content starts here.
"""
    res2 = strip_title_logic(md2)
    print(f"CASE 2:\n{res2}\n---\n")
    
    # Case 3: No Title
    md3 = """Just content.
## Section 1
"""
    res3 = strip_title_logic(md3)
    print(f"CASE 3:\n{res3}\n---\n")

if __name__ == "__main__":
    test_strip()
