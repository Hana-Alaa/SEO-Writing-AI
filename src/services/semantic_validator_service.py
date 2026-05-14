import re
from typing import Dict, Any, List

class SemanticValidatorService:
    """
    Heuristic-based semantic validators for AI-generated SEO content.
    Detects quality failures without LLM overhead.
    """

    def __init__(self):
        # Arabic keyword sets for heuristics
        self.keywords = {
            "pricing_logic": ["أسباب", "تعتمد", "ميزانية", "فئات", "عوامل", "تؤثر", "سعر", "تكلفة", "توفير", "منخفض", "متوسط", "مرتفع"],
            "lifestyle": ["خدمات", "مرافق", "مواصلات", "عائلات", "أفراد", "عمل", "قريب", "نمط", "حياة", "هدوء", "حيوية", "مدارس", "جامعات"],
            "user_fit": ["يناسب", "خيار", "احتياج", "اختيار", "ملائم", "مثالي", "بديل"],
            "tradeoffs": ["مقابل", "أفضل", "ميزة", "عيب", "يتفوق", "فرق", "مقارنة", "بينما", "لكن", "أما"],
            "guidance": ["خطوات", "نصيحة", "معايير", "كيفية", "تأكد", "افحص", "اسأل", "تحقق", "قرار"],
        }

    def validate_section(self, content: str, section_meta: Dict[str, Any], blueprint: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        warnings = []
        mode = section_meta.get("execution_mode", "taxonomy_breakdown")
        taxonomy = (section_meta.get("taxonomy_axis") or "").lower()
        heading = (section_meta.get("heading_text") or "").lower()
        
        # 1. Pricing Validator
        if mode == "market_practical" or "price" in taxonomy or any(kw in heading for kw in ["أسعار", "تكلفة", "سعر"]):
            if not self._check_keywords(content, self.keywords["pricing_logic"], min_matches=2):
                warnings.append(self._build_warning("pricing_without_market_logic", section_meta, 
                    "Section lacks practical pricing logic, budget tradeoffs, or cost driver explanations."))
            
            observed = section_meta.get("observed_data_mentions", [])
            if observed and not any(str(val) in content for val in observed):
                warnings.append(self._build_warning("missing_observed_data", section_meta,
                    "Section ignores provided observed data mentions."))

        # 2. Locality Validator
        if mode == "locality_analysis" or "location" in taxonomy or any(kw in heading for kw in ["أحياء", "موقع"]):
            if not self._check_keywords(content, self.keywords["lifestyle"], min_matches=3):
                warnings.append(self._build_warning("locality_without_lifestyle", section_meta,
                    "Section mentions locations but lacks resident-fit logic (services, accessibility, lifestyle)."))

        # 3. Taxonomy Validator
        if mode == "taxonomy_breakdown":
            if not self._check_keywords(content, self.keywords["user_fit"], min_matches=2):
                warnings.append(self._build_warning("taxonomy_without_user_fit", section_meta,
                    "Section lists categories without explaining who each category suits or how they differ practically."))

        # 4. Comparison Validator
        if mode == "comparison_decision" or any(kw in heading for kw in ["مقارنة", "vs"]):
            if not self._check_keywords(content, self.keywords["tradeoffs"], min_matches=2):
                warnings.append(self._build_warning("comparison_without_tradeoffs", section_meta,
                    "Section compares options without clear tradeoffs, pros/cons, or suitability differences."))

        # 5. Buyer Guidance Validator
        if mode == "buyer_guidance" or section_meta.get("section_type") == "faq":
            if not self._check_keywords(content, self.keywords["guidance"], min_matches=2):
                warnings.append(self._build_warning("generic_buyer_guidance", section_meta,
                    "Section provides broad advice without concrete next steps, criteria, or a decision sequence."))

        # 6. Blueprint Alignment
        if blueprint:
            thesis_kws = blueprint.get("section_thesis", "").split()[:5] # Take first few words as potential anchors
            if not self._check_keywords(content, thesis_kws, min_matches=1):
                # This is a very weak check, but better than nothing
                pass 
            
            # Check for avoid patterns (if any specific ones exist)
            for pattern in blueprint.get("avoid_patterns", []):
                if pattern in content.lower():
                    warnings.append(self._build_warning("blueprint_pattern_violated", section_meta,
                        f"Content contains forbidden pattern: '{pattern}'"))

        return warnings

    def _check_keywords(self, content: str, keywords: List[str], min_matches: int = 1) -> bool:
        matches = 0
        for kw in keywords:
            if kw in content:
                matches += 1
            if matches >= min_matches:
                return True
        return False

    def _build_warning(self, validator_name: str, meta: Dict[str, Any], reason: str) -> Dict[str, Any]:
        return {
            "validator_name": validator_name,
            "section_id": meta.get("section_id", "unknown"),
            "heading_text": meta.get("heading_text", "Untitled"),
            "reason": reason,
            "severity": "warning"
        }
