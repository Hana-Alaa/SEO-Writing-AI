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

    # Direct attempt
    try:
        return json.loads(text)
    except Exception:
        pass

    # Strip markdown fences
    cleaned = (
        text.replace("```json", "")
            .replace("```", "")
            .strip()
    )

    try:
        return json.loads(cleaned)
    except Exception:
        pass

    # Regex extraction (last resort)
    match = JSON_BLOCK_RE.search(text)
    if match:
        candidate = match.group(1) or match.group(2)
        if candidate:
            try:
                return json.loads(candidate.strip())
            except Exception as e:
                logger.debug(f"Regex JSON recovery failed: {e}")

    return None
