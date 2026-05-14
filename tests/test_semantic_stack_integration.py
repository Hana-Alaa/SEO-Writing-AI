import unittest
from src.services.content_generator import SectionWriter
from src.services.semantic_validator_service import SemanticValidatorService
from src.services.semantic_repair_service import SemanticRepairService

class MockAIClient:
    async def send(self, *args, **kwargs):
        return {"content": "{}", "metadata": {}}

class TestSemanticStackIntegration(unittest.TestCase):
    def setUp(self):
        self.writer = SectionWriter(MockAIClient())
        self.validator = SemanticValidatorService()
        self.repair = SemanticRepairService()

    def test_pricing_stack_integration(self):
        # 1. Given Section Input
        section = {
            "execution_mode": "market_practical",
            "taxonomy_axis": "pricing_by_area",
            "observed_data_mentions": ["الايجار الشهري : 2.000 ريال"]
        }
        
        # 2. Operational Adapter
        op_instr = self.writer._build_operational_instructions(section)
        self.assertTrue(any("prices vary" in i for i in op_instr))
        self.assertTrue(any("observed_data_mentions" in i for i in op_instr))
        
        # 3. Cognitive Blueprint
        blueprint = self.writer._build_cognitive_blueprint(section)
        self.assertTrue(any("budget tradeoffs" in d for d in blueprint["decision_logic"]))
        self.assertTrue(any("2.000 ريال" in e for e in blueprint["evidence_plan"]))
        
        # 4. Semantic Validation (on generic content)
        content = "تختلف الأسعار في كل منطقة."
        warnings = self.validator.validate_section(content, section, blueprint)
        self.assertTrue(any(w["validator_name"] == "pricing_without_market_logic" for w in warnings))
        self.assertTrue(any(w["validator_name"] == "missing_observed_data" for w in warnings))
        
        # 5. Semantic Repair
        repair_plan = self.repair.create_repair_plan(section, content, warnings, blueprint)
        self.assertTrue(repair_plan["needs_repair"])
        self.assertIn("pricing_market_logic", repair_plan["repair_focus"])
        self.assertIn("data_grounding", repair_plan["repair_focus"])
        self.assertIn("Explain why prices vary", repair_plan["repair_instruction"])

    def test_locality_stack_integration(self):
        section = {
            "execution_mode": "locality_analysis",
            "taxonomy_axis": "location_area"
        }
        
        op_instr = self.writer._build_operational_instructions(section)
        self.assertTrue(any("resident lifestyle needs" in i for i in op_instr))
        
        blueprint = self.writer._build_cognitive_blueprint(section)
        self.assertIn("lifestyle and needs", blueprint["section_thesis"])
        
        content = "حي الملقا وحي النرجس."
        warnings = self.validator.validate_section(content, section, blueprint)
        self.assertTrue(any(w["validator_name"] == "locality_without_lifestyle" for w in warnings))
        
        repair_plan = self.repair.create_repair_plan(section, content, warnings, blueprint)
        self.assertIn("locality_lifestyle_fit", repair_plan["repair_focus"])
        self.assertIn("Connect each area to specific resident lifestyle needs", repair_plan["repair_instruction"])

    def test_taxonomy_stack_integration(self):
        section = {
            "execution_mode": "taxonomy_breakdown"
        }
        
        op_instr = self.writer._build_operational_instructions(section)
        self.assertTrue(any("non-overlapping logic" in i for i in op_instr))
        
        blueprint = self.writer._build_cognitive_blueprint(section)
        self.assertIn("Differentiate available options", blueprint["section_thesis"])
        
        content = "شقق مفروشة وشقق سكنية."
        warnings = self.validator.validate_section(content, section, blueprint)
        self.assertTrue(any(w["validator_name"] == "taxonomy_without_user_fit" for w in warnings))
        
        repair_plan = self.repair.create_repair_plan(section, content, warnings, blueprint)
        self.assertIn("category_user_fit", repair_plan["repair_focus"])

    def test_no_warning_content_passes(self):
        section = {"execution_mode": "buyer_guidance"}
        content = "اتبع هذه الخطوات وتأكد من معايير فحص الشقة قبل القرار."
        warnings = self.validator.validate_section(content, section)
        
        repair_plan = self.repair.create_repair_plan(section, content, warnings)
        self.assertFalse(repair_plan["needs_repair"])

if __name__ == "__main__":
    unittest.main()
