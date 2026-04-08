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

    # Step 1: Clean markdown blocks and basic whitespace
    # Handles ```json { ... } ``` or just { ... }
    cleaned = re.sub(r"```(?:json)?|```", "", text).strip()
    
    # Convert smart quotes to straight quotes (common with some models)
    cleaned = cleaned.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")

    # Step 2: Try direct parse
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Step 3: Better Extraction Strategy
    # Find the outermost potential JSON structure
    # We look for the first { or [ and the last } or ]
    start_idx_brace = cleaned.find('{')
    start_idx_bracket = cleaned.find('[')
    
    start_idx = -1
    if start_idx_brace != -1 and (start_idx_bracket == -1 or start_idx_brace < start_idx_bracket):
        start_idx = start_idx_brace
    elif start_idx_bracket != -1:
        start_idx = start_idx_bracket

    if start_idx != -1:
        # We have a start. Now find the last matching end OR effectively close it if truncated.
        extracted = cleaned[start_idx:]
        
        # Try to find the last occurring closing char
        last_brace = extracted.rfind('}')
        last_bracket = extracted.rfind(']')
        end_idx = max(last_brace, last_bracket)
        
        if end_idx != -1:
            candidate = extracted[:end_idx+1]
            # Clean common malformations like trailing commas
            candidate = re.sub(r",\s*([\}\]])", r"\1", candidate)
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass

        # Step 4: HEAL TRUNCATED JSON
        # If we couldn't parse it, maybe it was cut off.
        # Simple heuristic: balance brackets/braces and close strings.
        stack = []
        healed = []
        in_string = False
        escape = False
        
        for char in extracted:
            if char == '"' and not escape:
                in_string = not in_string
            
            if char == '\\' and in_string:
                escape = not escape
            else:
                escape = False

            if not in_string:
                if char == '{':
                    stack.append('}')
                elif char == '[':
                    stack.append(']')
                elif char == '}':
                    if stack and stack[-1] == '}':
                        stack.pop()
                    else: # Mismatch or extra closing - stop here
                        break
                elif char == ']':
                    if stack and stack[-1] == ']':
                        stack.pop()
                    else: # Mismatch or extra closing - stop here
                        break
            
            healed.append(char)
        
        # Build back the string
        healed_str = "".join(healed)
        
        # Close the string if needed
        if in_string:
            healed_str += '"'
            
        # Close remaining brackets/braces in reverse order
        while stack:
            healed_str += stack.pop()

            
        try:
            # Final attempt at cleaning trailing commas even in healed string
            healed_str = re.sub(r",\s*([\}\]])", r"\1", healed_str)
            return json.loads(healed_str)
        except json.JSONDecodeError:
            pass

    return None

