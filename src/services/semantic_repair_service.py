from typing import Dict, Any, List

class SemanticRepairService:
    """
    Generates targeted repair instructions for semantic quality failures.
    Converts heuristic warnings into executable writing actions.
    """

    def __init__(self):
        self.repair_mappings = {
            "pricing_without_market_logic": {
                "focus": "pricing_market_logic",
                "instruction": (
                    "Your previous draft lacked practical pricing logic. In this repair, you MUST: "
                    "1. Explain why prices vary (cost drivers). 2. Add budget tradeoff logic. "
                    "3. Include relative tiers, and use numeric hints only when provided in observed_data_mentions. "
                    "4. Avoid generic pricing filler."
                ),
                "must_avoid": ["generic pricing prose", "unsupported exact prices"]
            },
            "locality_without_lifestyle": {
                "focus": "locality_lifestyle_fit",
                "instruction": (
                    "Your previous draft listed locations without context. In this repair, you MUST: "
                    "1. Connect each area to specific resident lifestyle needs. 2. Explain accessibility, "
                    "services, commute, or family/work fit. 3. Include local anchors. 4. Avoid pure geographic listing."
                ),
                "must_avoid": ["pure geographic listing", "distance-only descriptions"]
            },
            "taxonomy_without_user_fit": {
                "focus": "category_user_fit",
                "instruction": (
                    "Your previous draft listed categories without utility. In this repair, you MUST: "
                    "1. Explain who each type/category suits. 2. Explain practical differences between options. "
                    "3. Add situational fit logic. 4. Avoid pricing-first logic."
                ),
                "must_avoid": ["pricing-first logic", "overlapping category definitions"]
            },
            "comparison_without_tradeoffs": {
                "focus": "tradeoff_evaluation",
                "instruction": (
                    "Your previous draft compared options superficially. In this repair, you MUST: "
                    "1. Add clear tradeoffs between options. 2. Explain when each option wins. "
                    "3. Highlight suitability differences. 4. Avoid serial descriptions."
                ),
                "must_avoid": ["serial descriptive summaries", "vague 'both are good' conclusions"]
            },
            "generic_buyer_guidance": {
                "focus": "actionable_guidance",
                "instruction": (
                    "Your previous draft was too broad. In this repair, you MUST: "
                    "1. Add concrete next steps or selection criteria. 2. Add specific checks. "
                    "3. Convert vague advice into a clear decision sequence."
                ),
                "must_avoid": ["encyclopedic advice", "theoretical market commentary"]
            },
            "missing_observed_data": {
                "focus": "data_grounding",
                "instruction": "You ignored the provided 'observed_data_mentions'. Integrate them as cautious grounding hints to support your logic.",
                "must_add": ["observed_data_mentions"]
            },
            "blueprint_pattern_violated": {
                "focus": "blueprint_alignment",
                "instruction": "You used forbidden patterns or filler text that violates the section's cognitive blueprint. Remove the identified patterns and re-align with the section thesis.",
                "must_avoid": ["forbidden patterns"]
            }
        }

    def create_repair_plan(self, section_meta: Dict[str, Any], content: str, 
                          warnings: List[Dict[str, Any]], blueprint: Dict[str, Any] = None,
                          operational_instructions: List[str] = None) -> Dict[str, Any]:
        """
        Creates a structured repair plan based on semantic warnings.
        """
        if not warnings:
            return {
                "needs_repair": False,
                "repair_scope": "none"
            }

        repair_focus = []
        combined_instructions = []
        must_add = []
        must_avoid = []

        for warning in warnings:
            v_name = warning["validator_name"]
            mapping = self.repair_mappings.get(v_name)
            if mapping:
                repair_focus.append(mapping["focus"])
                combined_instructions.append(mapping["instruction"])
                must_add.extend(mapping.get("must_add", []))
                must_avoid.extend(mapping.get("must_avoid", []))

        # Add blueprint context if available
        if blueprint:
            combined_instructions.append(f"Re-align with Thesis: {blueprint.get('section_thesis')}")
            must_avoid.extend(blueprint.get("avoid_patterns", []))

        return {
            "needs_repair": True,
            "repair_scope": "section_only",
            "repair_focus": list(set(repair_focus)),
            "repair_instruction": "\n".join(combined_instructions),
            "must_add": list(set(must_add)),
            "must_avoid": list(set(must_avoid)),
            "preserve_headings": True,
            "section_id": section_meta.get("section_id", "unknown"),
            "original_content": content
        }
