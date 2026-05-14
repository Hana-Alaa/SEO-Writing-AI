import unittest
from src.services.semantic_repair_service import SemanticRepairService

class TestSemanticRepair(unittest.TestCase):
    def setUp(self):
        self.repair_service = SemanticRepairService()

    def test_pricing_repair_plan(self):
        warnings = [{"validator_name": "pricing_without_market_logic"}]
        meta = {"section_id": "sec_01"}
        plan = self.repair_service.create_repair_plan(meta, "old content", warnings)
        
        self.assertTrue(plan["needs_repair"])
        self.assertIn("pricing_market_logic", plan["repair_focus"])
        self.assertIn("Explain why prices vary", plan["repair_instruction"])
        self.assertIn("unsupported exact prices", plan["must_avoid"])
        self.assertTrue(plan["preserve_headings"])

    def test_locality_repair_plan(self):
        warnings = [{"validator_name": "locality_without_lifestyle"}]
        meta = {"section_id": "sec_02"}
        plan = self.repair_service.create_repair_plan(meta, "old content", warnings)
        
        self.assertIn("locality_lifestyle_fit", plan["repair_focus"])
        self.assertIn("Connect each area to specific resident lifestyle needs", plan["repair_instruction"])
        self.assertIn("pure geographic listing", plan["must_avoid"])

    def test_taxonomy_repair_plan(self):
        warnings = [{"validator_name": "taxonomy_without_user_fit"}]
        meta = {"section_id": "sec_03"}
        plan = self.repair_service.create_repair_plan(meta, "old content", warnings)
        
        self.assertIn("category_user_fit", plan["repair_focus"])
        self.assertIn("Explain who each type/category suits", plan["repair_instruction"])

    def test_comparison_repair_plan(self):
        warnings = [{"validator_name": "comparison_without_tradeoffs"}]
        meta = {"section_id": "sec_04"}
        plan = self.repair_service.create_repair_plan(meta, "old content", warnings)
        
        self.assertIn("tradeoff_evaluation", plan["repair_focus"])
        self.assertIn("Explain when each option wins", plan["repair_instruction"])

    def test_guidance_repair_plan(self):
        warnings = [{"validator_name": "generic_buyer_guidance"}]
        meta = {"section_id": "sec_05"}
        plan = self.repair_service.create_repair_plan(meta, "old content", warnings)
        
        self.assertIn("actionable_guidance", plan["repair_focus"])
        self.assertIn("Add concrete next steps", plan["repair_instruction"])

    def test_blueprint_repair_instructions(self):
        warnings = [{"validator_name": "blueprint_pattern_violated"}]
        blueprint = {"section_thesis": "Test Thesis", "avoid_patterns": ["filler"]}
        meta = {"section_id": "sec_06"}
        plan = self.repair_service.create_repair_plan(meta, "old content", warnings, blueprint=blueprint)
        
        self.assertIn("Re-align with Thesis: Test Thesis", plan["repair_instruction"])
        self.assertIn("filler", plan["must_avoid"])

    def test_no_warnings_returns_no_repair(self):
        plan = self.repair_service.create_repair_plan({}, "content", [])
        self.assertFalse(plan["needs_repair"])
        self.assertEqual(plan["repair_scope"], "none")

if __name__ == "__main__":
    unittest.main()
