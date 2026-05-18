from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PromptQualityRulesTests(unittest.TestCase):
    def _get_combined_base_prompt(self) -> str:
        templates_dir = ROOT / "assets/prompts/templates"
        base = (templates_dir / "02_section_writer_base.txt").read_text(encoding="utf-8")
        constitution = (templates_dir / "core_constitution.txt").read_text(encoding="utf-8")
        contract = (templates_dir / "section_contract.txt").read_text(encoding="utf-8")
        state = (templates_dir / "runtime_state.txt").read_text(encoding="utf-8")
        overrides = (templates_dir / "dynamic_overrides.txt").read_text(encoding="utf-8")
        output_format = (templates_dir / "output_contract.txt").read_text(encoding="utf-8")
        return "\n".join([base, constitution, state, contract, overrides, output_format])



    def test_intro_hook_quality_rule_exists_in_writer_prompts(self):
        combined_base = self._get_combined_base_prompt()
        commercial_prompt = (ROOT / "assets/prompts/templates/02_section_writer_brand_commercial_v2.txt").read_text(encoding="utf-8")

        self.assertIn("INTRODUCTION PRIMARY KEYWORD PLACEMENT", combined_base)
        self.assertIn("TONAL DNA & ARABIC QUALITY", combined_base)
        self.assertIn("العامية", combined_base)
        self.assertIn("MANDATORY COMMERCIAL INTRO STRUCTURE (HOOK -> BRAND -> SOFT CTA)", commercial_prompt)
        self.assertIn("Focus purely on the user's problem, tension, or trade-off", commercial_prompt)

    def test_section_promise_and_table_cap_rules_exist(self):
        combined_base = self._get_combined_base_prompt()
        commercial_prompt = (ROOT / "assets/prompts/templates/02_section_writer_brand_commercial_v2.txt").read_text(encoding="utf-8")

        self.assertIn("HEADING PROMISE FULFILLMENT", combined_base)
        self.assertIn("Compound Heading Rule", combined_base)
        self.assertIn("Choice Promise Rule", combined_base)
        self.assertIn("Hard Table Limit", combined_base)
        self.assertIn("Limit the explicit name", commercial_prompt)

    def test_metric_rules_and_firewall_exist(self):
        combined_base = self._get_combined_base_prompt()

        self.assertIn("GLOBAL NUMERIC & DATA MANDATE", combined_base)
        self.assertIn("GEO SCOPE DRIFT CONTROL", combined_base)
        self.assertIn("NO LATER-TOPIC LEAKAGE", combined_base)
        self.assertIn("Approved Subheadings", combined_base)
        self.assertIn("THE TANGIBLE SPECS MANDATE", combined_base)
        self.assertIn("LOCAL CONTEXT LANDING LAW", combined_base)
        self.assertIn("HEADING PROMISE RESOLUTION LAW", combined_base)
        self.assertIn("THE NUMBERED PROCESS MANDATE", combined_base)





if __name__ == "__main__":
    unittest.main()

