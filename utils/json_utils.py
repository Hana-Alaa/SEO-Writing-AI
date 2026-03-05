import json
import re
from typing import Any, Optional

def recover_json(text: str) -> Optional[Any]:
    """
    Attempts to safely extract and parse JSON from LLM responses.
    Handles markdown blocks, conversational filler, and common malformations.
    """
    if not text or not isinstance(text, str):
        return None

    # Step 1: Clean markdown and basic whitespace
    cleaned = re.sub(r"```json|```", "", text).strip()
    
    # Step 2: Try direct parse
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Step 3: Extract the first occurring JSON object or array
    # This handles cases where the LLM adds text before/after the JSON
    match = re.search(r"(\{.*\}|\[.*\])", cleaned, re.DOTALL)
    if match:
        json_str = match.group(1)
        
        # Step 4: Handle common malformations like trailing commas before closing braces
        # Note: This is a simple regex fix for the most common case
        json_str = re.sub(r",\s*([\}\]])", r"\1", json_str)
        
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            pass

    return None
