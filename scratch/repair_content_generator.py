import os

PATH = r"f:\SEO-Writing-AI\src\services\content_generator.py"

with open(PATH, 'r', encoding='utf-8') as f:
    content = f.read()

# Target broken transition
target = """        section.setdefault("content_behavior", "")
            urls: List[Dict[str, str]],"""

replacement = """        section.setdefault("content_behavior", "")

        section.setdefault("content_type", content_type)
        section.setdefault("content_strategy", content_strategy)
        section.setdefault("area", area)
        section.setdefault("requires_table", False)
        section.setdefault("table_type", "none")
        section.setdefault("requires_list", False)
        section.setdefault("list_type", "none")
        section.setdefault("cta_position", "none")
        # --- Primary keyword enforcement for strategic sections ---
        sec_type = (section.get("section_type") or "").lower()
        if sec_type == "introduction" or idx == 0:
            section.setdefault("requires_primary_keyword", True)
        else:
            section.setdefault("requires_primary_keyword", section.get("requires_primary_keyword", False))

        # --- Apply Semantic Execution State ---
        self._apply_semantic_execution_state(section, idx)

    def _apply_semantic_execution_state(self, section: Dict[str, Any], idx: int):
        \"\"\"
        Maps a section to a cognitive execution mode based on its type and taxonomy axis.
        \"\"\"
        from src.services.strategy_service import SEMANTIC_EXECUTION_LAYER
        
        sec_type = (section.get("section_type") or "").lower()
        taxonomy = (section.get("taxonomy_axis") or "").lower()
        heading = (section.get("heading_text") or "").lower()
        
        mode_key = "taxonomy_breakdown"
        
        if sec_type == "introduction" or idx == 0:
            mode_key = "onboarding_context"
        elif sec_type == "conclusion":
            mode_key = "buyer_guidance"
        elif any(kw in taxonomy or kw in heading for kw in ["price", "cost", "pricing", "أسعار", "تكلفة", "سعر"]):
            mode_key = "market_practical"
        elif any(kw in taxonomy or kw in heading for kw in ["location", "area", "neighborhood", "أحياء", "موقع"]):
            mode_key = "locality_analysis"
        elif "process" in sec_type or "how" in sec_type or "خطوات" in heading:
            mode_key = "buyer_guidance"
        elif any(kw in taxonomy or kw in heading for kw in ["comparison", "vs", "مقارنة"]):
            mode_key = "comparison_decision"
        elif sec_type == "proof" or "trust" in taxonomy or "دليل" in heading:
            mode_key = "trust_proof"
        elif sec_type == "faq":
            mode_key = "buyer_guidance"
            
        state = SEMANTIC_EXECUTION_LAYER.get(mode_key, SEMANTIC_EXECUTION_LAYER["taxonomy_breakdown"])
        
        if not section.get("execution_mode"):
            section["execution_mode"] = state["execution_mode"]
        if not section.get("semantic_goal"):
            section["semantic_goal"] = state["semantic_goal"]
        if not section.get("decision_frame"):
            section["decision_frame"] = state["decision_frame"]
        if not section.get("content_behavior"):
            section["content_behavior"] = state["content_behavior"]

    def _validate_outline_schema(self, outline: List[Dict[str, Any]], heading_only_mode: bool = False) -> bool:
        if heading_only_mode:
            required_keys = {
                "section_id", "heading_level", "heading_text", "section_type", "section_intent"
            }
        else:
            required_keys = {
                "section_id", "heading_level", "heading_text", "section_intent",
                "section_promise", "reader_takeaway", "must_include_details",
                "must_not_repeat", "practical_decision_value", "evidence_expectation",
                "value_density_target", "allowed_generality_level", "subheading_policy"
            }

        for section in outline:
            if not required_keys.issubset(section.keys()):
                missing = required_keys - set(section.keys())
                logger.error(f"Section {section.get('section_id')} missing keys: {missing}")
                return False
        return True

    async def generate(
            self,
            title: str,
            keywords: List[str],
            urls: List[Dict[str, str]],"""

if target in content:
    new_content = content.replace(target, replacement)
    with open(PATH, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Success: Fixed the corrupted transition.")
else:
    # Try with CRLF if LF failed
    target_crlf = target.replace('\\n', '\\r\\n')
    if target_crlf in content:
        new_content = content.replace(target_crlf, replacement.replace('\\n', '\\r\\n'))
        with open(PATH, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print("Success: Fixed the corrupted transition (CRLF).")
    else:
        print("Error: Could not find the target broken transition.")
        # Print a snippet of where we think it is
        idx = content.find('section.setdefault("content_behavior", "")')
        if idx != -1:
            print("Found part of target. Snippet:")
            print(repr(content[idx:idx+100]))
        else:
            print("Could not even find the first part of target.")
