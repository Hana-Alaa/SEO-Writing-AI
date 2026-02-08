import json
import re
from typing import Any, Optional

JSON_BLOCK_RE = re.compile(
    r"```json\s*(.*?)```|(\{.*\}|\[.*\])",
    re.DOTALL
)

def recover_json(text: str) -> Optional[Any]:
    """
    Attempts to recover valid JSON from a noisy LLM response.
    Returns parsed JSON or None if recovery fails.
    """
    if not text or not isinstance(text, str):
        return None

    # 1. Direct attempt
    try:
        return json.loads(text)
    except Exception:
        pass

    # 2. Strip markdown fences
    cleaned = (
        text.replace("```json", "")
            .replace("```", "")
            .strip()
    )

    try:
        return json.loads(cleaned)
    except Exception:
        pass

    # 3. Regex extraction (last resort)
    match = JSON_BLOCK_RE.search(text)
    if match:
        candidate = match.group(1) or match.group(2)
        if candidate:
            try:
                return json.loads(candidate.strip())
            except Exception:
                return None

    return None
