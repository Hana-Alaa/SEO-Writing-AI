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

    # Fallback: Find the first and last brackets/braces to ignore conversational text
    first_bracket = text.find('[')
    last_bracket = text.rfind(']')
    
    first_brace = text.find('{')
    last_brace = text.rfind('}')
    
    # Try array if it starts first
    if first_bracket != -1 and (first_brace == -1 or first_bracket < first_brace) and last_bracket != -1:
        try:
            return json.loads(text[first_bracket:last_bracket+1])
        except Exception:
            pass
            
    # Try object if it starts first
    if first_brace != -1 and (first_bracket == -1 or first_brace < first_bracket) and last_brace != -1:
        try:
            return json.loads(text[first_brace:last_brace+1])
        except Exception:
            pass

    logger.debug("Regex JSON recovery and outermost bracket extraction failed.")
    return None
