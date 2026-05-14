import unittest
from src.services.semantic_validator_service import SemanticValidatorService

class TestSemanticValidators(unittest.TestCase):
    def setUp(self):
        self.validator = SemanticValidatorService()

    def test_pricing_without_market_logic(self):
        meta = {"execution_mode": "market_practical", "heading_text": "أسعار الشقق"}
        content = "تختلف أسعار الشقق في الرياض من منطقة لأخرى."
        warnings = self.validator.validate_section(content, meta)
        
        self.assertTrue(any(w["validator_name"] == "pricing_without_market_logic" for w in warnings))
        
        # Passing case
        content_good = "تعتمد أسعار الشقق على فئات الميزانية وتؤثر العوامل المختلفة على التكلفة."
        warnings_good = self.validator.validate_section(content_good, meta)
        self.assertFalse(any(w["validator_name"] == "pricing_without_market_logic" for w in warnings_good))

    def test_missing_observed_data(self):
        meta = {"execution_mode": "market_practical", "observed_data_mentions": ["35000"]}
        content = "الأسعار مرتفعة في الشمال."
        warnings = self.validator.validate_section(content, meta)
        self.assertTrue(any(w["validator_name"] == "missing_observed_data" for w in warnings))

    def test_locality_without_lifestyle(self):
        meta = {"execution_mode": "locality_analysis", "heading_text": "أحياء الرياض"}
        content = "حي الملقا وحي النرجس وحي الياسمين."
        warnings = self.validator.validate_section(content, meta)
        self.assertTrue(any(w["validator_name"] == "locality_without_lifestyle" for w in warnings))
        
        # Passing case
        content_good = "حي الملقا يوفر خدمات ممتازة ومرافق قريبة من العمل والمدارس."
        warnings_good = self.validator.validate_section(content_good, meta)
        self.assertFalse(any(w["validator_name"] == "locality_without_lifestyle" for w in warnings_good))

    def test_taxonomy_without_user_fit(self):
        meta = {"execution_mode": "taxonomy_breakdown"}
        content = "هناك شقق مفروشة وشقق غير مفروشة."
        warnings = self.validator.validate_section(content, meta)
        self.assertTrue(any(w["validator_name"] == "taxonomy_without_user_fit" for w in warnings))
        
        # Passing case
        content_good = "الشقق المفروشة خيار مثالي لمن يبحث عن احتياج مؤقت وسريع."
        warnings_good = self.validator.validate_section(content_good, meta)
        self.assertFalse(any(w["validator_name"] == "taxonomy_without_user_fit" for w in warnings_good))

    def test_comparison_without_tradeoffs(self):
        meta = {"execution_mode": "comparison_decision"}
        content = "هذا هو الخيار الأول وهذا هو الخيار الثاني."
        warnings = self.validator.validate_section(content, meta)
        self.assertTrue(any(w["validator_name"] == "comparison_without_tradeoffs" for w in warnings))
        
        # Passing case
        content_good = "الخيار الأول يتفوق في الميزة بينما الخيار الثاني يمثل فرقاً في السعر."
        warnings_good = self.validator.validate_section(content_good, meta)
        self.assertFalse(any(w["validator_name"] == "comparison_without_tradeoffs" for w in warnings_good))

    def test_generic_buyer_guidance(self):
        meta = {"execution_mode": "buyer_guidance"}
        content = "يجب أن تختار بعناية فائقة."
        warnings = self.validator.validate_section(content, meta)
        self.assertTrue(any(w["validator_name"] == "generic_buyer_guidance" for w in warnings))
        
        # Passing case
        content_good = "اتبع هذه الخطوات وتأكد من معايير فحص الشقة قبل القرار."
        warnings_good = self.validator.validate_section(content_good, meta)
        self.assertFalse(any(w["validator_name"] == "generic_buyer_guidance" for w in warnings_good))

    def test_blueprint_pattern_violated(self):
        meta = {"execution_mode": "taxonomy_breakdown"}
        blueprint = {"avoid_patterns": ["filler text"]}
        content = "This content has some filler text in it."
        warnings = self.validator.validate_section(content, meta, blueprint)
        self.assertTrue(any(w["validator_name"] == "blueprint_pattern_violated" for w in warnings))

if __name__ == "__main__":
    unittest.main()
