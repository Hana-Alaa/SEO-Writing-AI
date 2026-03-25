import json
import re
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

JSON_BLOCK_RE = re.compile(
    r"```json\s*(.*?)```|(\{.*?\}|\[.*?\])",
    re.DOTALL
)

def recover_json(text: str) -> Optional[Any]:
    if not text or not isinstance(text, str):
        return None

    # Clean smart quotes which break json.loads
    text = text.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")

    # Direct attempt
    try:
        return json.loads(text)
    except Exception:
        pass

    # Extract markdown block specifically
    md_match = re.search(r'```(?:json)?\s*(.*?)\s*```', text, re.DOTALL | re.IGNORECASE)
    if md_match:
        try:
            return json.loads(md_match.group(1))
        except Exception as e:
            logger.debug(f"Failed to parse markdown JSON: {e}")

    # Aggressive Cleanup: Remove control characters and non-printable stuff
    text = "".join(char for char in text if char.isprintable() or char in "\n\r\t")

    # Fallback: Find the first and last brackets/braces to ignore conversational text
    first_bracket = text.find('[')
    last_bracket = text.rfind(']')
    
    first_brace = text.find('{')
    last_brace = text.rfind('}')
    
    # Try object if it contains "content" (The most likely target for SectionWriter)
    if first_brace != -1 and last_brace != -1:
        candidate = text[first_brace:last_brace+1]
        try:
            return json.loads(candidate)
        except Exception:
            # Last ditch: try to fix common JSON errors (like trailing commas)
            try:
                fixed = re.sub(r',\s*([\]}])', r'\1', candidate)
                return json.loads(fixed)
            except Exception:
                 pass

    logger.debug("Regex JSON recovery and outermost bracket extraction failed.")
    return None
