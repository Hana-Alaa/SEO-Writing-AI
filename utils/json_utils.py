import json
import re
from typing import Any, Optional

def recover_json(text: str) -> Optional[Any]:
    """
    Attempts to safely extract JSON from LLM responses.
    Handles:
    - Wrapped markdown ```json blocks
    - Extra commentary before/after JSON
    - Malformed trailing commas
    """

    if not text:
        return None

    # Remove markdown fences
    text = re.sub(r"```json|```", "", text).strip()

    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try extracting first JSON object
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            return None

    return None
